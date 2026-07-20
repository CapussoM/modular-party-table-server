from __future__ import annotations

import asyncio
import json
import os
import random
import time
from contextlib import suppress

import websockets


SERVER_URL = os.getenv("SIGNALING_URL", "ws://127.0.0.1:8080/ws")
MESSAGE_COUNT = int(os.getenv("JITTER_MESSAGE_COUNT", "24"))
RANDOM_SEED = int(os.getenv("JITTER_RANDOM_SEED", "7319"))


async def receive(socket, expected_type: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"timed out waiting for {expected_type}")
        message = json.loads(
            await asyncio.wait_for(socket.recv(), timeout=remaining)
        )
        if message.get("type") == expected_type:
            return message


async def connect_client():
    socket = await websockets.connect(
        SERVER_URL,
        open_timeout=5,
        ping_interval=10,
        ping_timeout=5,
    )
    connected = await receive(socket, "connected")
    assert connected.get("relayEnabled") is True
    return socket, connected


async def run() -> None:
    rng = random.Random(RANDOM_SEED)
    host, host_connected = await connect_client()
    guest, guest_connected = await connect_client()
    recovered_guest = None
    started_at = time.monotonic()
    try:
        await host.send(json.dumps({
            "type": "create_room",
            "profile": {"display_name": "Jitter Host"},
            "gameId": "hidden_word",
            "public": False,
            "maxPlayers": 4,
        }))
        room_code = (await receive(host, "room_created"))["roomCode"]
        await guest.send(json.dumps({
            "type": "join_room",
            "roomCode": room_code,
            "profile": {"display_name": "Jitter Guest", "ready": True},
        }))
        await receive(guest, "room_joined")
        await receive(host, "peer_joined")

        # Ordered lobby-like payloads with a deterministic 20-250 ms jitter.
        received_revisions: list[int] = []
        for revision in range(1, MESSAGE_COUNT + 1):
            await asyncio.sleep(rng.uniform(0.02, 0.25))
            await guest.send(json.dumps({
                "type": "app",
                "targetPeerId": host_connected["peerId"],
                "payload": {
                    "kind": "unstable_network_probe",
                    "revision": revision,
                    "ready": revision % 2 == 0,
                },
            }))
            relayed = await receive(host, "app")
            assert relayed["fromPeerId"] == guest_connected["peerId"]
            received_revisions.append(int(relayed["payload"]["revision"]))
        assert received_revisions == list(range(1, MESSAGE_COUNT + 1))

        # Simulate a small connection jump without sending leave_room.
        old_guest_id = guest_connected["peerId"]
        guest.transport.abort()
        left = await receive(host, "peer_left")
        assert left["peerId"] == old_guest_id
        await asyncio.sleep(rng.uniform(0.25, 0.65))

        recovered_guest, recovered_connected = await connect_client()
        assert recovered_connected["peerId"] != old_guest_id
        await recovered_guest.send(json.dumps({
            "type": "join_room",
            "roomCode": room_code,
            "profile": {
                "display_name": "Jitter Guest",
                "ready": False,
            },
        }))
        rejoined = await receive(recovered_guest, "room_joined")
        assert rejoined["roomCode"] == room_code
        joined_notice = await receive(host, "peer_joined")
        assert joined_notice["peerId"] == recovered_connected["peerId"]

        await host.send(json.dumps({
            "type": "app",
            "targetPeerId": recovered_connected["peerId"],
            "payload": {
                "kind": "lobby_snapshot",
                "revision": MESSAGE_COUNT + 1,
                "players": ["host", "recovered_guest"],
            },
        }))
        recovered_snapshot = await receive(recovered_guest, "app")
        assert recovered_snapshot["fromPeerId"] == host_connected["peerId"]
        assert recovered_snapshot["payload"]["revision"] == MESSAGE_COUNT + 1

        elapsed = time.monotonic() - started_at
        print(
            f"PASS: relay {SERVER_URL} preserved "
            f"{MESSAGE_COUNT} ordered updates with 20-250 ms jitter and "
            f"accepted a clean room rejoin after a connection jump ({elapsed:.2f}s)"
        )
    finally:
        if recovered_guest is not None:
            await recovered_guest.close()
        with suppress(Exception):
            await guest.close()
        await host.close()


if __name__ == "__main__":
    asyncio.run(run())
