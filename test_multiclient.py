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

        await host.send(json.dumps({
            "type": "create_room",
            "profile": {"display_name": "Host"},
        }))
        created = await receive(host)
        room_code = created["roomCode"]

        await guest_one.send(
            json.dumps({
                "type": "join_room",
                "roomCode": room_code,
                "profile": {"display_name": "Guest One"},
            })
        )
        joined_one = await receive(guest_one)
        host_notice_one = await receive(host)

        await guest_two.send(
            json.dumps({
                "type": "join_room",
                "roomCode": room_code,
                "profile": {"display_name": "Guest Two"},
            })
        )
        joined_two = await receive(guest_two)
        host_notice_two = await receive(host)
        guest_one_notice = await receive(guest_one)

        assert host_connected["type"] == "connected"
        assert host_connected["relayEnabled"] is False
        assert guest_one_connected["type"] == "connected"
        assert guest_two_connected["type"] == "connected"
        assert joined_one["type"] == "room_joined"
        assert joined_two["type"] == "room_joined"
        assert joined_one["peers"][0]["profile"]["display_name"] == "Host"
        assert host_notice_one["profile"]["display_name"] == "Guest One"
        assert host_notice_one["type"] == "peer_joined"
        assert host_notice_two["type"] == "peer_joined"
        assert guest_one_notice["type"] == "peer_joined"

        await guest_two.send(json.dumps({"type": "leave_room"}))
        assert (await receive(guest_two))["type"] == "room_left"
        host_left_notice = await receive(host)
        guest_one_left_notice = await receive(guest_one)
        assert host_left_notice == {
            "type": "peer_left",
            "peerId": guest_two_connected["peerId"],
        }
        assert guest_one_left_notice == host_left_notice

        await guest_one.send(json.dumps({
            "type": "offer",
            "targetPeerId": host_connected["peerId"],
            "sdp": "test-offer",
        }))
        relayed_offer = await receive(host)
        assert relayed_offer["type"] == "offer"
        assert relayed_offer["fromPeerId"] == guest_one_connected["peerId"]
        assert relayed_offer["sdp"] == "test-offer"

        await guest_one.send(json.dumps({
            "type": "app",
            "targetPeerId": host_connected["peerId"],
            "payload": {"kind": "must_not_be_relayed"},
        }))
        relay_error = await receive(guest_one)
        assert relay_error == {"type": "error", "code": "RELAY_DISABLED"}

        print(
            f"PASS: room {room_code} connected one host, two guests, "
            "and forwarded WebRTC signaling"
        )

    async with (
        websockets.connect(SERVER_URL) as host,
        websockets.connect(SERVER_URL) as guest_one,
        websockets.connect(SERVER_URL) as guest_two,
    ):
        host_connected = await receive(host)
        guest_one_connected = await receive(guest_one)
        guest_two_connected = await receive(guest_two)

        await host.send(json.dumps({
            "type": "create_room",
            "profile": {"display_name": "Host"},
        }))
        room_code = (await receive(host))["roomCode"]
        for socket, name in [
            (guest_one, "Guest One"),
            (guest_two, "Guest Two"),
        ]:
            await socket.send(json.dumps({
                "type": "join_room",
                "roomCode": room_code,
                "profile": {"display_name": name},
            }))
            await receive(socket)
            await receive(host)
            if socket is guest_two:
                await receive(guest_one)

        await host.send(json.dumps({"type": "leave_room"}))
        assert (await receive(host))["type"] == "room_left"
        guest_one_left = await receive(guest_one)
        guest_one_host = await receive(guest_one)
        guest_two_left = await receive(guest_two)
        guest_two_host = await receive(guest_two)
        assert guest_one_left == {
            "type": "peer_left",
            "peerId": host_connected["peerId"],
        }
        assert guest_two_left == guest_one_left
        assert guest_one_host == {
            "type": "host_changed",
            "peerId": guest_one_connected["peerId"],
        }
        assert guest_two_host == guest_one_host

        await guest_two.send(json.dumps({
            "type": "offer",
            "targetPeerId": guest_one_connected["peerId"],
            "sdp": "new-host-offer",
        }))
        relayed_offer = await receive(guest_one)
        assert relayed_offer["type"] == "offer"
        assert relayed_offer["fromPeerId"] == guest_two_connected["peerId"]
        assert relayed_offer["sdp"] == "new-host-offer"

        print("PASS: room host migrates automatically when the host leaves")

    async with (
        websockets.connect(SERVER_URL) as host_one,
        websockets.connect(SERVER_URL) as host_two,
        websockets.connect(SERVER_URL) as invalid_guest,
    ):
        await receive(host_one)
        await receive(host_two)
        await receive(invalid_guest)
        await host_one.send(json.dumps({"type": "create_room", "profile": {}}))
        await host_two.send(json.dumps({"type": "create_room", "profile": {}}))
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

    async with (
        websockets.connect(SERVER_URL) as limited_host,
        websockets.connect(SERVER_URL) as allowed_guest,
        websockets.connect(SERVER_URL) as rejected_guest,
    ):
        for socket in [limited_host, allowed_guest, rejected_guest]:
            await receive(socket)
        await limited_host.send(json.dumps({
            "type": "create_room",
            "maxPlayers": 2,
            "profile": {"display_name": "Limited Host"},
        }))
        limited_code = (await receive(limited_host))["roomCode"]
        await allowed_guest.send(json.dumps({
            "type": "join_room",
            "roomCode": limited_code,
            "profile": {"display_name": "Allowed Guest"},
        }))
        assert (await receive(allowed_guest))["type"] == "room_joined"
        await receive(limited_host)
        await rejected_guest.send(json.dumps({
            "type": "join_room",
            "roomCode": limited_code,
            "profile": {"display_name": "Rejected Guest"},
        }))
        assert await receive(rejected_guest) == {
            "type": "error",
            "code": "ROOM_FULL",
        }
        print("PASS: host-selected room capacity is enforced by the server")

    async with (
        websockets.connect(SERVER_URL) as private_host,
        websockets.connect(SERVER_URL) as color_host,
        websockets.connect(SERVER_URL) as sketch_host,
        websockets.connect(SERVER_URL) as quick_guest,
    ):
        for socket in [private_host, color_host, sketch_host, quick_guest]:
            await receive(socket)
        await private_host.send(json.dumps({
            "type": "create_room",
            "gameId": "color_clash",
            "public": False,
            "profile": {"display_name": "Private"},
        }))
        private_code = (await receive(private_host))["roomCode"]
        await color_host.send(json.dumps({
            "type": "create_room",
            "gameId": "color_clash",
            "public": True,
            "profile": {"display_name": "Color Host"},
        }))
        color_code = (await receive(color_host))["roomCode"]
        await sketch_host.send(json.dumps({
            "type": "create_room",
            "gameId": "sketch_relay",
            "public": True,
            "profile": {"display_name": "Sketch Host"},
        }))
        await receive(sketch_host)
        await quick_guest.send(json.dumps({
            "type": "quick_join",
            "gameId": "color_clash",
            "profile": {"display_name": "Quick Guest"},
        }))
        joined = await receive(quick_guest)
        notice = await receive(color_host)
        assert joined["type"] == "room_joined"
        assert joined["roomCode"] == color_code
        assert joined["roomCode"] != private_code
        assert notice["profile"]["display_name"] == "Quick Guest"
        await color_host.send(json.dumps({
            "type": "set_room_public",
            "public": False,
        }))
        public_update = await receive(color_host)
        assert public_update == {
            "type": "room_public_updated",
            "public": False,
        }
        print("PASS: quick join selects a public room for the requested game")

    async with websockets.connect(SERVER_URL) as first_player:
        await receive(first_player)
        await first_player.send(json.dumps({
            "type": "quick_join",
            "gameId": "rapid_quiz",
            "profile": {"display_name": "First Player"},
        }))
        created = await receive(first_player)
        assert created["type"] == "room_created"
        print("PASS: first quick-match player becomes the lobby leader")


if __name__ == "__main__":
    asyncio.run(run())
