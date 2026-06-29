from __future__ import annotations

import base64
import asyncio
import json
import time
from urllib.parse import quote

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec

from admob_ssv import AdMobSsvVerifier, RewardStore


def encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def test_valid_callback_and_replay_protection() -> None:
    private_key = ec.generate_private_key(ec.SECP256R1())
    verifier = AdMobSsvVerifier("ca-app-pub-7010865599450469/6336433932")
    verifier._keys = {7: private_key.public_key()}
    verifier._keys_loaded_at = time.monotonic()
    custom_data = quote(
        json.dumps(
            {
                "nonce": "a" * 48,
                "placement_id": "lobby_player_unlock",
                "game_id": "hidden_identity",
                "room_code": "ABC123",
            },
            separators=(",", ":"),
        ),
        safe="",
    )
    content = (
        "ad_network=1"
        "&ad_unit=ca-app-pub-7010865599450469%2F6336433932"
        f"&custom_data={custom_data}"
        "&reward_amount=1"
        "&reward_item=unlock"
        f"&timestamp={int(time.time() * 1000)}"
        "&transaction_id=txn-1"
        "&user_id=player-1"
    )
    signature = private_key.sign(
        content.encode("utf-8"),
        ec.ECDSA(hashes.SHA256()),
    )
    reward = asyncio.run(
        verifier.verify(
            f"{content}&signature={encode(signature)}&key_id=7".encode("ascii")
        )
    )

    assert reward.nonce == "a" * 48
    assert reward.game_id == "hidden_identity"
    store = RewardStore()
    assert store.put(reward)
    assert not store.put(reward)
    assert store.get(reward.nonce, "player-1") == reward
    assert store.get(reward.nonce, "other") is None


def test_ios_rewarded_unit_is_accepted_with_multi_unit_config() -> None:
    private_key = ec.generate_private_key(ec.SECP256R1())
    verifier = AdMobSsvVerifier([
        "ca-app-pub-7010865599450469/6336433932",
        "ca-app-pub-7010865599450469/2585459402",
    ])
    verifier._keys = {7: private_key.public_key()}
    verifier._keys_loaded_at = time.monotonic()
    custom_data = quote(
        json.dumps({"nonce": "i" * 48}, separators=(",", ":")),
        safe="",
    )
    content = (
        "ad_network=1"
        "&ad_unit=ca-app-pub-7010865599450469%2F2585459402"
        f"&custom_data={custom_data}"
        "&reward_amount=1"
        "&reward_item=unlock"
        f"&timestamp={int(time.time() * 1000)}"
        "&transaction_id=txn-ios"
        "&user_id=player-ios"
    )
    signature = private_key.sign(
        content.encode("utf-8"),
        ec.ECDSA(hashes.SHA256()),
    )

    reward = asyncio.run(
        verifier.verify(
            f"{content}&signature={encode(signature)}&key_id=7".encode("ascii")
        )
    )

    assert reward.nonce == "i" * 48
    assert reward.player_id == "player-ios"


def test_modified_callback_is_rejected() -> None:
    private_key = ec.generate_private_key(ec.SECP256R1())
    verifier = AdMobSsvVerifier("expected")
    verifier._keys = {7: private_key.public_key()}
    verifier._keys_loaded_at = time.monotonic()
    content = (
        "ad_network=1&ad_unit=expected"
        "&custom_data=%7B%22nonce%22%3A%22"
        + "b" * 48
        + "%22%7D"
        "&reward_amount=1&reward_item=unlock"
        f"&timestamp={int(time.time() * 1000)}"
        "&transaction_id=txn-2&user_id=player-1"
    )
    signature = private_key.sign(
        content.encode("utf-8"),
        ec.ECDSA(hashes.SHA256()),
    )
    query = (
        f"{content}&signature={encode(signature)}&key_id=7"
    ).replace("reward_amount=1", "reward_amount=2")

    try:
        asyncio.run(verifier.verify(query.encode("ascii")))
        raise AssertionError("modified callback was accepted")
    except ValueError as error:
        assert str(error) == "invalid_signature"
