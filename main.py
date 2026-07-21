from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
from fastapi import (
    FastAPI,
    Header,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from pydantic import BaseModel, Field
from websockets.exceptions import ConnectionClosed

from admob_ssv import AdMobSsvVerifier, RewardStore
from cloud_profiles import (
    CloudProfileStore,
    ProfileRevisionConflict,
    cloud_profile_response,
)
from platform_identity import PlatformIdentityError, PlatformIdentityVerifier
from analytics import AnalyticsStore

ROOM_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ROOM_TTL_SECONDS = int(os.getenv("ROOM_TTL_SECONDS", "600"))
ALLOW_APP_RELAY = os.getenv("ALLOW_APP_RELAY", "false").lower() == "true"
MAX_ROOM_PEERS = int(os.getenv("MAX_ROOM_PEERS", "16"))
MAX_SIGNAL_BYTES = int(os.getenv("MAX_SIGNAL_BYTES", "131072"))
MAX_APP_BYTES = int(os.getenv("MAX_APP_BYTES", "524288"))
MAX_MESSAGES_PER_SECOND = int(os.getenv("MAX_MESSAGES_PER_SECOND", "120"))
PUBLIC_JOIN_BASE_URL = os.getenv(
    "PUBLIC_JOIN_BASE_URL", "https://example.com/join"
).rstrip("/")
ADMOB_REWARDED_AD_UNIT_IDS = [
    unit.strip()
    for unit in os.getenv(
        "ADMOB_REWARDED_AD_UNIT_IDS",
        os.getenv(
            "ADMOB_REWARDED_AD_UNIT_ID",
            (
                "ca-app-pub-7010865599450469/6336433932,"
                "ca-app-pub-7010865599450469/2585459402"
            ),
        ),
    ).split(",")
    if unit.strip()
]


@dataclass
class Peer:
    peer_id: str
    websocket: WebSocket
    room_code: Optional[str] = None
    profile: dict[str, Any] = field(default_factory=dict)
    last_seen: float = field(default_factory=time.monotonic)
    rate_window_started: float = field(default_factory=time.monotonic)
    rate_window_messages: int = 0
    game_id: str = ""
    public_room: bool = False
    max_players: int = MAX_ROOM_PEERS


rooms: dict[str, dict[str, Peer]] = {}
peers: dict[str, Peer] = {}
admob_ssv = AdMobSsvVerifier(ADMOB_REWARDED_AD_UNIT_IDS)
verified_rewards = RewardStore()
cloud_profiles = CloudProfileStore(
    os.getenv("CLOUD_DB_PATH", "/tmp/stegosaurini-cloud.sqlite3"),
    os.getenv("CLOUD_IDENTITY_PEPPER", ""),
)
platform_identity = PlatformIdentityVerifier.from_environment()
analytics = AnalyticsStore()
analytics_rate_windows: dict[str, tuple[float, int]] = {}


def create_room_code() -> str:
    while True:
        code = "".join(secrets.choice(ROOM_ALPHABET) for _ in range(6))
        if code not in rooms:
            return code


def sanitize_profile(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    entitlement_ids = value.get("owned_entitlements", [])
    if not isinstance(entitlement_ids, list):
        entitlement_ids = []
    return {
        "display_name": str(value.get("display_name", "Player"))[:40],
        "ready": bool(value.get("ready", True)),
        "reconnect_id": str(value.get("reconnect_id", ""))[:64],
        "owned_entitlements": [
            str(item)[:128] for item in entitlement_ids[:32]
        ],
    }


def public_peer(peer: Peer) -> dict[str, Any]:
    return {"peerId": peer.peer_id, "profile": peer.profile}


def requested_room_capacity(message: dict[str, Any]) -> int:
    try:
        requested = int(message.get("maxPlayers", MAX_ROOM_PEERS))
    except (TypeError, ValueError):
        requested = MAX_ROOM_PEERS
    return max(2, min(requested, MAX_ROOM_PEERS))


def room_host(room: dict[str, Peer]) -> Optional[Peer]:
    return next(iter(room.values()), None)


async def send(peer: Peer, message: dict[str, Any]) -> bool:
    try:
        await peer.websocket.send_json(message)
        return True
    except (RuntimeError, WebSocketDisconnect, ConnectionClosed):
        return False


async def leave_room(peer: Peer, recoverable: bool = False) -> None:
    if not peer.room_code:
        return

    room = rooms.get(peer.room_code)
    if room:
        was_host = room_host(room) == peer
        previous_game_id = peer.game_id
        previous_public_room = peer.public_room
        previous_max_players = peer.max_players
        room.pop(peer.peer_id, None)
        new_host = room_host(room) if was_host else None
        if new_host is not None:
            # Matchmaking metadata belongs to the room, not to the departing
            # host's connection. Preserve it when host authority migrates.
            new_host.game_id = previous_game_id
            new_host.public_room = previous_public_room
            new_host.max_players = previous_max_players
        for other in list(room.values()):
            await send(
                other,
                {
                    "type": "peer_left",
                    "peerId": peer.peer_id,
                    "recoverable": recoverable,
                },
            )
        if new_host is not None:
            for other in list(room.values()):
                await send(
                    other,
                    {"type": "host_changed", "peerId": new_host.peer_id},
                )
        if not room:
            rooms.pop(peer.room_code, None)
    peer.room_code = None


async def cleanup_rooms() -> None:
    while True:
        await asyncio.sleep(60)
        expiry = time.monotonic() - ROOM_TTL_SECONDS
        stale = [peer for peer in peers.values() if peer.last_seen < expiry]
        for peer in stale:
            await leave_room(peer, recoverable=True)
            peers.pop(peer.peer_id, None)
            with suppress(RuntimeError):
                await peer.websocket.close(code=1001, reason="inactive")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await analytics.start()
    cleanup_task = asyncio.create_task(cleanup_rooms())
    yield
    cleanup_task.cancel()
    with suppress(asyncio.CancelledError):
        await cleanup_task
    await analytics.stop()


app = FastAPI(
    title="Modular Party Table Server",
    version="0.1.0",
    lifespan=lifespan,
)


class ProfileResponse(BaseModel):
    player_id: str
    friend_code: str
    display_name: str


class RewardResponse(BaseModel):
    token: str
    expires_in_seconds: int


class PlatformSessionRequest(BaseModel):
    provider: str = Field(min_length=1, max_length=64)
    credential: str = Field(min_length=1, max_length=16384)
    initial_profile: dict[str, Any] = Field(default_factory=dict)


class CloudProfileUpdate(BaseModel):
    expected_revision: int = Field(ge=1)
    profile: dict[str, Any] = Field(default_factory=dict)


class AnalyticsEvent(BaseModel):
    event_name: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9_.-]+$")
    event_type: str = Field(default="event", max_length=32)
    session_id: str = Field(default="", max_length=64)
    install_id: str = Field(default="", max_length=64)
    app_version: str = Field(default="", max_length=32)
    platform: str = Field(default="", max_length=32)
    properties: dict[str, Any] = Field(default_factory=dict)


@app.middleware("http")
async def record_unhandled_errors(request: Request, call_next):
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))[:128]
    try:
        response = await call_next(request)
    except Exception as error:
        analytics.record({
            "event_name": "server_error",
            "event_type": "error",
            "request_id": request_id,
            "properties": {
                "path": request.url.path[:256],
                "method": request.method,
                "exception_type": type(error).__name__,
                "message": str(error)[:500],
            },
        })
        raise
    response.headers["x-request-id"] = request_id
    return response


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "rooms": len(rooms),
        "peers": len(peers),
    }


@app.post("/v1/analytics/events", status_code=202)
async def ingest_analytics_event(
    event: AnalyticsEvent,
    request: Request,
) -> dict[str, bool]:
    # Keep this public endpoint safe for a distributable mobile client: no DB
    # credential is shipped, payloads are bounded, and IPs are not persisted.
    if len(await request.body()) > 16_384:
        raise HTTPException(status_code=413, detail="analytics_event_too_large")
    client_key = request.client.host if request.client else "unknown"
    window_started, event_count = analytics_rate_windows.get(
        client_key, (time.monotonic(), 0)
    )
    now = time.monotonic()
    if now - window_started >= 60:
        window_started, event_count = now, 0
    if event_count >= 60:
        raise HTTPException(status_code=429, detail="analytics_rate_limited")
    analytics_rate_windows[client_key] = (window_started, event_count + 1)
    properties_json = json.dumps(event.properties, separators=(",", ":"))
    if len(properties_json.encode("utf-8")) > 12_000:
        raise HTTPException(status_code=413, detail="analytics_properties_too_large")
    accepted = analytics.record(event.model_dump())
    return {"accepted": accepted}


@app.post("/v1/cloud/session/platform")
async def create_platform_cloud_session(
    request: PlatformSessionRequest,
) -> dict[str, Any]:
    try:
        identity = await platform_identity.verify(
            request.provider,
            request.credential,
        )
        session = cloud_profiles.create_session(
            identity.provider,
            identity.subject,
            request.initial_profile,
        )
    except PlatformIdentityError as error:
        raise HTTPException(
            status_code=error.status_code,
            detail=error.code,
        ) from error
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return {
        "session_token": session.token,
        "expires_at": session.expires_at,
        "created": session.created,
        **cloud_profile_response(session.profile),
    }


@app.get("/v1/cloud/profile")
async def get_cloud_profile(
    authorization: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    token = _bearer_token(authorization)
    profile = cloud_profiles.get_profile_for_token(token)
    if profile is None:
        raise HTTPException(status_code=401, detail="cloud_session_invalid")
    return cloud_profile_response(profile)


@app.put("/v1/cloud/profile")
async def update_cloud_profile(
    request: CloudProfileUpdate,
    authorization: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    token = _bearer_token(authorization)
    try:
        profile = cloud_profiles.update_profile_for_token(
            token,
            request.expected_revision,
            request.profile,
        )
    except ProfileRevisionConflict as error:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "profile_revision_conflict",
                "current": cloud_profile_response(error.current),
            },
        ) from error
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    if profile is None:
        raise HTTPException(status_code=401, detail="cloud_session_invalid")
    return cloud_profile_response(profile)


@app.delete("/v1/cloud/session")
async def revoke_cloud_session(
    authorization: Optional[str] = Header(default=None),
) -> dict[str, bool]:
    token = _bearer_token(authorization)
    cloud_profiles.revoke_session(token)
    return {"ok": True}


@app.get("/mock/profile", response_model=ProfileResponse)
async def mock_profile() -> ProfileResponse:
    return ProfileResponse(
        player_id="mock-player",
        friend_code="FOX-4821",
        display_name="Player",
    )


@app.post("/mock/ad-reward", response_model=RewardResponse)
async def mock_ad_reward() -> RewardResponse:
    return RewardResponse(
        token=secrets.token_urlsafe(32),
        expires_in_seconds=600,
    )


@app.get("/admob/ssv")
async def admob_reward_callback(request: Request) -> dict[str, bool]:
    query_params = request.query_params
    if (
        query_params.get("ad_unit") == "1234567890"
        and query_params.get("transaction_id") == "123456789"
    ):
        return {"ok": True}
    try:
        reward = await admob_ssv.verify(request.scope["query_string"])
    except (ValueError, httpx.HTTPError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    verified_rewards.put(reward)
    return {"ok": True}


@app.get("/admob/rewards/{nonce}")
async def admob_reward_status(
    nonce: str,
    player_id: str,
) -> dict[str, Any]:
    reward = verified_rewards.get(nonce, player_id)
    if reward is None:
        return {"verified": False}
    return {
        "verified": True,
        "transaction_id": reward.transaction_id,
        "placement_id": reward.placement_id,
        "game_id": reward.game_id,
        "room_code": reward.room_code,
        "reward_amount": reward.reward_amount,
        "reward_item": reward.reward_item,
    }


@app.get("/join/{room_code}")
async def join_link(room_code: str) -> dict[str, str]:
    normalized = room_code.upper()
    if normalized not in rooms:
        raise HTTPException(status_code=404, detail="room_not_found")
    return {
        "roomCode": normalized,
        "deepLink": f"modularpartytable://join?room={normalized}",
        "webLink": f"{PUBLIC_JOIN_BASE_URL}/{normalized}",
    }


def _bearer_token(authorization: Optional[str]) -> str:
    if authorization is None:
        raise HTTPException(status_code=401, detail="cloud_session_missing")
    scheme, separator, token = authorization.partition(" ")
    if (
        separator != " "
        or scheme.lower() != "bearer"
        or not token.strip()
    ):
        raise HTTPException(status_code=401, detail="cloud_session_invalid")
    return token.strip()


@app.websocket("/ws")
async def signaling_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    peer = Peer(peer_id=secrets.token_hex(8), websocket=websocket)
    peers[peer.peer_id] = peer
    await send(
        peer,
        {
            "type": "connected",
            "peerId": peer.peer_id,
            "relayEnabled": ALLOW_APP_RELAY,
        },
    )

    try:
        while True:
            try:
                message = await websocket.receive_json()
            except RuntimeError:
                # Starlette can raise RuntimeError instead of
                # WebSocketDisconnect when a mobile client drops immediately
                # after leave_room or changes network.
                break
            now = time.monotonic()
            peer.last_seen = now
            if now - peer.rate_window_started >= 1:
                peer.rate_window_started = now
                peer.rate_window_messages = 0
            peer.rate_window_messages += 1
            if peer.rate_window_messages > MAX_MESSAGES_PER_SECOND:
                await websocket.close(code=1008, reason="rate limit")
                break
            if not isinstance(message, dict):
                await send(peer, {"type": "error", "code": "INVALID_MESSAGE"})
                continue
            message_type = message.get("type")

            encoded_message = json.dumps(
                message, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
            if len(encoded_message) > MAX_SIGNAL_BYTES:
                await send(peer, {"type": "error", "code": "MESSAGE_TOO_LARGE"})
                continue

            if message_type == "ping":
                await send(peer, {"type": "pong"})
                continue

            if message_type == "set_room_public":
                if peer.room_code:
                    room = rooms.get(peer.room_code, {})
                    host = room_host(room)
                    if host and host.peer_id == peer.peer_id:
                        peer.public_room = bool(message.get("public", False))
                        await send(peer, {
                            "type": "room_public_updated",
                            "public": peer.public_room,
                        })
                continue

            if message_type == "set_room_game":
                if peer.room_code:
                    room = rooms.get(peer.room_code, {})
                    host = room_host(room)
                    if host and host.peer_id == peer.peer_id:
                        peer.game_id = str(message.get("gameId", ""))[:64]
                        await send(peer, {
                            "type": "room_game_updated",
                            "gameId": peer.game_id,
                        })
                continue

            if message_type == "leave_room":
                await leave_room(peer, recoverable=False)
                await send(peer, {"type": "room_left"})
                continue

            if message_type == "create_room":
                await leave_room(peer)
                peer.profile = sanitize_profile(message.get("profile", {}))
                code = create_room_code()
                peer.game_id = str(message.get("gameId", ""))[:64]
                peer.public_room = bool(message.get("public", False))
                peer.max_players = requested_room_capacity(message)
                peer.room_code = code
                rooms[code] = {peer.peer_id: peer}
                await send(
                    peer,
                    {
                        "type": "room_created",
                        "roomCode": code,
                        "inviteLink": (
                            f"modularpartytable://join?room={code}"
                        ),
                    },
                )
                continue

            if message_type == "quick_join":
                game_id = str(message.get("gameId", ""))[:64]
                available = next(
                    (
                        (code, room)
                        for code, room in rooms.items()
                        if room
                        and room_host(room).public_room
                        and room_host(room).game_id == game_id
                        and len(room) < room_host(room).max_players
                    ),
                    None,
                )
                if available is None:
                    await leave_room(peer)
                    peer.profile = sanitize_profile(message.get("profile", {}))
                    code = create_room_code()
                    peer.game_id = game_id
                    peer.public_room = True
                    peer.max_players = requested_room_capacity(message)
                    peer.room_code = code
                    rooms[code] = {peer.peer_id: peer}
                    await send(peer, {
                        "type": "room_created",
                        "roomCode": code,
                        "inviteLink": f"modularpartytable://join?room={code}",
                    })
                    continue
                code, room = available
                await leave_room(peer)
                peer.profile = sanitize_profile(message.get("profile", {}))
                existing_peers = [public_peer(existing) for existing in room.values()]
                peer.room_code = code
                peer.game_id = game_id
                room[peer.peer_id] = peer
                await send(peer, {
                    "type": "room_joined",
                    "roomCode": code,
                    "peerIds": [item["peerId"] for item in existing_peers],
                    "peers": existing_peers,
                })
                for other in list(room.values()):
                    if other.peer_id != peer.peer_id:
                        await send(other, {
                            "type": "peer_joined",
                            "peerId": peer.peer_id,
                            "profile": peer.profile,
                        })
                continue

            if message_type == "join_room":
                code = str(message.get("roomCode", "")).upper()
                room = rooms.get(code)
                if room is None:
                    await send(
                        peer,
                        {"type": "error", "code": "ROOM_NOT_FOUND"},
                    )
                    continue
                incoming_profile = sanitize_profile(message.get("profile", {}))
                reconnect_id = incoming_profile.get("reconnect_id", "")
                replaced_peer = next(
                    (
                        existing
                        for existing in room.values()
                        if reconnect_id
                        and existing is not room_host(room)
                        and existing.profile.get("reconnect_id", "") == reconnect_id
                        and existing.peer_id != peer.peer_id
                    ),
                    None,
                )
                if replaced_peer is not None:
                    # A network switch can open the replacement socket before the
                    # old WebSocket reports its disconnect. Treat both sockets as
                    # one player before the capacity check and before sending the
                    # joining snapshot.
                    room.pop(replaced_peer.peer_id, None)
                    replaced_peer.room_code = None
                    for other in list(room.values()):
                        await send(other, {
                            "type": "peer_left",
                            "peerId": replaced_peer.peer_id,
                            "recoverable": True,
                        })
                host = room_host(room)
                if host is None or len(room) >= host.max_players:
                    await send(
                        peer,
                        {"type": "error", "code": "ROOM_FULL"},
                    )
                    continue

                await leave_room(peer)
                peer.profile = incoming_profile
                existing_peers = [public_peer(existing) for existing in room.values()]
                existing_peer_ids = [existing["peerId"] for existing in existing_peers]
                peer.room_code = code
                room[peer.peer_id] = peer
                await send(
                    peer,
                    {
                        "type": "room_joined",
                        "roomCode": code,
                        "peerIds": existing_peer_ids,
                        "peers": existing_peers,
                    },
                )
                for other in list(room.values()):
                    if other.peer_id != peer.peer_id:
                        await send(
                            other,
                            {
                                "type": "peer_joined",
                                "peerId": peer.peer_id,
                                "profile": peer.profile,
                            },
                        )
                continue

            if message_type in {"offer", "answer", "ice"}:
                if not peer.room_code:
                    await send(
                        peer,
                        {"type": "error", "code": "NOT_IN_ROOM"},
                    )
                    continue
                target_id = str(message.get("targetPeerId", ""))
                target = rooms.get(peer.room_code, {}).get(target_id)
                if target is None:
                    await send(
                        peer,
                        {"type": "error", "code": "PEER_NOT_FOUND"},
                    )
                    continue
                forwarded = dict(message)
                forwarded["fromPeerId"] = peer.peer_id
                await send(target, forwarded)
                continue

            if message_type == "app":
                if not ALLOW_APP_RELAY:
                    await send(
                        peer,
                        {"type": "error", "code": "RELAY_DISABLED"},
                    )
                    continue
                if not peer.room_code:
                    await send(
                        peer,
                        {"type": "error", "code": "NOT_IN_ROOM"},
                    )
                    continue

                room = rooms.get(peer.room_code, {})
                target_id = str(message.get("targetPeerId", ""))
                if not target_id:
                    await send(
                        peer,
                        {"type": "error", "code": "RELAY_TARGET_REQUIRED"},
                    )
                    continue
                payload = message.get("payload", {})
                encoded_payload = json.dumps(
                    payload, separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8")
                if len(encoded_payload) > MAX_APP_BYTES:
                    await send(
                        peer,
                        {"type": "error", "code": "APP_MESSAGE_TOO_LARGE"},
                    )
                    continue
                forwarded = {
                    "type": "app",
                    "fromPeerId": peer.peer_id,
                    "payload": payload,
                }
                target = room.get(target_id)
                if target is None:
                    await send(
                        peer,
                        {"type": "error", "code": "PEER_NOT_FOUND"},
                    )
                else:
                    await send(target, forwarded)
                continue

            await send(
                peer,
                {"type": "error", "code": "UNKNOWN_MESSAGE_TYPE"},
            )
    except (WebSocketDisconnect, ConnectionClosed):
        pass
    finally:
        await leave_room(peer, recoverable=True)
        peers.pop(peer.peer_id, None)
