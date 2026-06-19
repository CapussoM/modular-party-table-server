from __future__ import annotations

import asyncio
import os
import secrets
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel


ROOM_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ROOM_TTL_SECONDS = int(os.getenv("ROOM_TTL_SECONDS", "600"))
PUBLIC_JOIN_BASE_URL = os.getenv(
    "PUBLIC_JOIN_BASE_URL", "https://example.com/join"
).rstrip("/")


@dataclass
class Peer:
    peer_id: str
    websocket: WebSocket
    room_code: str | None = None
    last_seen: float = field(default_factory=time.monotonic)


rooms: dict[str, dict[str, Peer]] = {}
peers: dict[str, Peer] = {}


def create_room_code() -> str:
    while True:
        code = "".join(secrets.choice(ROOM_ALPHABET) for _ in range(6))
        if code not in rooms:
            return code


async def send(peer: Peer, message: dict[str, Any]) -> bool:
    try:
        await peer.websocket.send_json(message)
        return True
    except (RuntimeError, WebSocketDisconnect):
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
    await send(peer, {"type": "connected", "peerId": peer.peer_id})

    try:
        while True:
            message = await websocket.receive_json()
            peer.last_seen = time.monotonic()
            message_type = message.get("type")

            if message_type == "ping":
                await send(peer, {"type": "pong"})
                continue

            if message_type == "create_room":
                await leave_room(peer)
                code = create_room_code()
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

            if message_type == "join_room":
                code = str(message.get("roomCode", "")).upper()
                room = rooms.get(code)
                if room is None:
                    await send(
                        peer,
                        {"type": "error", "code": "ROOM_NOT_FOUND"},
                    )
                    continue

                await leave_room(peer)
                existing_peer_ids = list(room)
                peer.room_code = code
                room[peer.peer_id] = peer
                await send(
                    peer,
                    {
                        "type": "room_joined",
                        "roomCode": code,
                        "peerIds": existing_peer_ids,
                    },
                )
                for other in list(room.values()):
                    if other.peer_id != peer.peer_id:
                        await send(
                            other,
                            {
                                "type": "peer_joined",
                                "peerId": peer.peer_id,
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
                if not peer.room_code:
                    await send(
                        peer,
                        {"type": "error", "code": "NOT_IN_ROOM"},
                    )
                    continue

                room = rooms.get(peer.room_code, {})
                target_id = str(message.get("targetPeerId", ""))
                forwarded = {
                    "type": "app",
                    "fromPeerId": peer.peer_id,
                    "payload": message.get("payload", {}),
                }
                if target_id:
                    target = room.get(target_id)
                    if target is None:
                        await send(
                            peer,
                            {"type": "error", "code": "PEER_NOT_FOUND"},
                        )
                    else:
                        await send(target, forwarded)
                else:
                    for other in list(room.values()):
                        if other.peer_id != peer.peer_id:
                            await send(other, forwarded)
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
