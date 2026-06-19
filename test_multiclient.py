from __future__ import annotations

import asyncio
import json
import os

import websockets


SERVER_URL = os.getenv("SIGNALING_URL", "ws://127.0.0.1:8080/ws")


async def receive(socket) -> dict:
    return json.loads(await asyncio.wait_for(socket.recv(), timeout=3))


async def run() -> None:
    async with (
        websockets.connect(SERVER_URL) as host,
        websockets.connect(SERVER_URL) as guest_one,
        websockets.connect(SERVER_URL) as guest_two,
    ):
        host_connected = await receive(host)
        guest_one_connected = await receive(guest_one)
        guest_two_connected = await receive(guest_two)

        await host.send(json.dumps({"type": "create_room"}))
        created = await receive(host)
        room_code = created["roomCode"]

        await guest_one.send(
            json.dumps({"type": "join_room", "roomCode": room_code})
        )
        joined_one = await receive(guest_one)
        host_notice_one = await receive(host)

        await guest_two.send(
            json.dumps({"type": "join_room", "roomCode": room_code})
        )
        joined_two = await receive(guest_two)
        host_notice_two = await receive(host)
        guest_one_notice = await receive(guest_one)

        assert host_connected["type"] == "connected"
        assert guest_one_connected["type"] == "connected"
        assert guest_two_connected["type"] == "connected"
        assert joined_one["type"] == "room_joined"
        assert joined_two["type"] == "room_joined"
        assert host_notice_one["type"] == "peer_joined"
        assert host_notice_two["type"] == "peer_joined"
        assert guest_one_notice["type"] == "peer_joined"

        await guest_one.send(
            json.dumps({
                "type": "app",
                "targetPeerId": host_connected["peerId"],
                "payload": {
                    "kind": "hidden_word_vote",
                    "target_player_id": guest_two_connected["peerId"],
                },
            })
        )
        relayed_vote = await receive(host)
        assert relayed_vote["type"] == "app"
        assert relayed_vote["fromPeerId"] == guest_one_connected["peerId"]
        assert relayed_vote["payload"]["kind"] == "hidden_word_vote"

        print(
            f"PASS: room {room_code} connected one host, two guests, "
            "and relayed a private vote"
        )

    async with (
        websockets.connect(SERVER_URL) as host_one,
        websockets.connect(SERVER_URL) as host_two,
        websockets.connect(SERVER_URL) as invalid_guest,
    ):
        await receive(host_one)
        await receive(host_two)
        await receive(invalid_guest)
        await host_one.send(json.dumps({"type": "create_room"}))
        await host_two.send(json.dumps({"type": "create_room"}))
        room_one = (await receive(host_one))["roomCode"]
        room_two = (await receive(host_two))["roomCode"]
        assert room_one != room_two

        await invalid_guest.send(
            json.dumps({"type": "join_room", "roomCode": "ZZZZZZ"})
        )
        error = await receive(invalid_guest)
        assert error == {"type": "error", "code": "ROOM_NOT_FOUND"}

        print(
            "PASS: room codes are unique and unknown codes are rejected"
        )


if __name__ == "__main__":
    asyncio.run(run())
