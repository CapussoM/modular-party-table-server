from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from websockets.exceptions import ConnectionClosed

from admob_ssv import AdMobSsvVerifier, RewardStore

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
ADMOB_REWARDED_AD_UNIT_ID = os.getenv(
    "ADMOB_REWARDED_AD_UNIT_ID",
    "ca-app-pub-7010865599450469/6336433932",
)


@dataclass
class Peer:
    peer_id: str
    websocket: WebSocket
    room_code: str | None = None
    profile: dict[str, Any] = field(default_factory=dict)
    last_seen: float = field(default_factory=time.monotonic)
    rate_window_started: float = field(default_factory=time.monotonic)
    rate_window_messages: int = 0
    game_id: str = ""
    public_room: bool = False
    max_players: int = MAX_ROOM_PEERS


rooms: dict[str, dict[str, Peer]] = {}
peers: dict[str, Peer] = {}
admob_ssv = AdMobSsvVerifier(ADMOB_REWARDED_AD_UNIT_ID)
verified_rewards = RewardStore()


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


def room_host(room: dict[str, Peer]) -> Peer | None:
    return next(iter(room.values()), None)


async def send(peer: Peer, message: dict[str, Any]) -> bool:
    try:
        await peer.websocket.send_json(message)
        return True
    except (RuntimeError, WebSocketDisconnect, ConnectionClosed):
        return False


async def leave_room(peer: Peer) -> None:
    if not peer.room_code:
        return

    room = rooms.get(peer.room_code)
    if room:
        room.pop(peer.peer_id, None)
        for other in list(room.values()):
            await send(
                other,
                {"type": "peer_left", "peerId": peer.peer_id},
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
            await leave_room(peer)
            peers.pop(peer.peer_id, None)
            with suppress(RuntimeError):
                await peer.websocket.close(code=1001, reason="inactive")


@asynccontextmanager
async def lifespan(_: FastAPI):
    cleanup_task = asyncio.create_task(cleanup_rooms())
    yield
    cleanup_task.cancel()
    with suppress(asyncio.CancelledError):
        await cleanup_task


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


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "rooms": len(rooms),
        "peers": len(peers),
    }


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
            message = await websocket.receive_json()
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

            if message_type == "leave_room":
                await leave_room(peer)
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
                host = room_host(room)
                if host is None or len(room) >= host.max_players:
                    await send(
                        peer,
                        {"type": "error", "code": "ROOM_FULL"},
                    )
                    continue

                await leave_room(peer)
                peer.profile = sanitize_profile(message.get("profile", {}))
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
    except WebSocketDisconnect:
        pass
    finally:
        await leave_room(peer)
        peers.pop(peer.peer_id, None)
