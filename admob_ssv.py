from __future__ import annotations

import asyncio
import base64
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec


ADMOB_KEYS_URL = "https://www.gstatic.com/admob/reward/verifier-keys.json"
KEY_CACHE_SECONDS = 24 * 60 * 60
REWARD_TTL_SECONDS = 15 * 60
MAX_CALLBACK_AGE_MS = 60 * 60 * 1000
MAX_FUTURE_SKEW_MS = 5 * 60 * 1000


@dataclass
class VerifiedReward:
    nonce: str
    player_id: str
    placement_id: str
    game_id: str
    room_code: str
    transaction_id: str
    reward_amount: int
    reward_item: str
    verified_at: float


class AdMobSsvVerifier:
    def __init__(self, expected_ad_units: str | list[str] | tuple[str, ...]) -> None:
        if isinstance(expected_ad_units, str):
            expected_ad_units = [expected_ad_units]
        self.expected_ad_units = {
            unit.strip() for unit in expected_ad_units if unit.strip()
        }
        self._keys: dict[int, ec.EllipticCurvePublicKey] = {}
        self._keys_loaded_at = 0.0
        self._key_lock = asyncio.Lock()

    async def verify(self, raw_query: bytes) -> VerifiedReward:
        signed_content, signature, key_id, values = self._parse(raw_query)
        public_key = await self._get_key(key_id)
        try:
            public_key.verify(signature, signed_content, ec.ECDSA(hashes.SHA256()))
        except InvalidSignature as error:
            raise ValueError("invalid_signature") from error

        ad_unit = self._one(values, "ad_unit")
        if self.expected_ad_units and ad_unit not in self.expected_ad_units:
            raise ValueError("unexpected_ad_unit")

        timestamp_ms = int(self._one(values, "timestamp"))
        now_ms = int(time.time() * 1000)
        if timestamp_ms < now_ms - MAX_CALLBACK_AGE_MS:
            raise ValueError("expired_callback")
        if timestamp_ms > now_ms + MAX_FUTURE_SKEW_MS:
            raise ValueError("future_callback")

        custom_data_raw = self._one(values, "custom_data")
        try:
            custom_data = json.loads(custom_data_raw)
        except json.JSONDecodeError as error:
            raise ValueError("invalid_custom_data") from error
        if not isinstance(custom_data, dict):
            raise ValueError("invalid_custom_data")

        nonce = str(custom_data.get("nonce", ""))
        if len(nonce) < 32 or len(nonce) > 128:
            raise ValueError("invalid_nonce")

        return VerifiedReward(
            nonce=nonce,
            player_id=self._one(values, "user_id"),
            placement_id=str(custom_data.get("placement_id", ""))[:128],
            game_id=str(custom_data.get("game_id", ""))[:128],
            room_code=str(custom_data.get("room_code", ""))[:32],
            transaction_id=self._one(values, "transaction_id"),
            reward_amount=int(self._one(values, "reward_amount")),
            reward_item=self._one(values, "reward_item"),
            verified_at=time.time(),
        )

    def _parse(
        self, raw_query: bytes
    ) -> tuple[bytes, bytes, int, dict[str, list[str]]]:
        try:
            query = raw_query.decode("ascii")
        except UnicodeDecodeError as error:
            raise ValueError("invalid_query_encoding") from error

        marker = "&signature="
        signature_index = query.find(marker)
        if signature_index < 0:
            raise ValueError("missing_signature")
        key_marker = "&key_id="
        key_index = query.find(key_marker, signature_index + len(marker))
        if key_index < 0:
            raise ValueError("missing_key_id")
        if query.find("&", key_index + len(key_marker)) >= 0:
            raise ValueError("unexpected_trailing_parameters")

        signed_content = query[:signature_index].encode("utf-8")
        signature_text = query[
            signature_index + len(marker) : key_index
        ]
        key_text = query[key_index + len(key_marker) :]
        try:
            signature = self._decode_urlsafe(signature_text)
            key_id = int(key_text)
        except (ValueError, TypeError) as error:
            raise ValueError("invalid_signature_metadata") from error

        values = parse_qs(
            query[:signature_index],
            keep_blank_values=True,
            strict_parsing=True,
        )
        return signed_content, signature, key_id, values

    async def _get_key(self, key_id: int) -> ec.EllipticCurvePublicKey:
        if (
            key_id not in self._keys
            or time.monotonic() - self._keys_loaded_at >= KEY_CACHE_SECONDS
        ):
            await self._refresh_keys()
        key = self._keys.get(key_id)
        if key is None:
            raise ValueError("unknown_key_id")
        return key

    async def _refresh_keys(self) -> None:
        async with self._key_lock:
            if (
                self._keys
                and time.monotonic() - self._keys_loaded_at < KEY_CACHE_SECONDS
            ):
                return
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(ADMOB_KEYS_URL)
                response.raise_for_status()
                payload = response.json()

            keys: dict[int, ec.EllipticCurvePublicKey] = {}
            for item in payload.get("keys", []):
                key = serialization.load_pem_public_key(
                    item["pem"].encode("ascii")
                )
                if isinstance(key, ec.EllipticCurvePublicKey):
                    keys[int(item["keyId"])] = key
            if not keys:
                raise ValueError("no_verification_keys")
            self._keys = keys
            self._keys_loaded_at = time.monotonic()

    @staticmethod
    def _decode_urlsafe(value: str) -> bytes:
        padding = "=" * (-len(value) % 4)
        return base64.urlsafe_b64decode(value + padding)

    @staticmethod
    def _one(values: dict[str, list[str]], name: str) -> str:
        items = values.get(name, [])
        if len(items) != 1 or not items[0]:
            raise ValueError(f"missing_{name}")
        return items[0]


class RewardStore:
    def __init__(self) -> None:
        self._by_nonce: dict[str, VerifiedReward] = {}
        self._transactions: set[str] = set()

    def put(self, reward: VerifiedReward) -> bool:
        self.cleanup()
        if reward.transaction_id in self._transactions:
            return False
        self._transactions.add(reward.transaction_id)
        self._by_nonce[reward.nonce] = reward
        return True

    def get(self, nonce: str, player_id: str) -> VerifiedReward | None:
        self.cleanup()
        reward = self._by_nonce.get(nonce)
        if reward is None or reward.player_id != player_id:
            return None
        return reward

    def cleanup(self) -> None:
        expiry = time.time() - REWARD_TTL_SECONDS
        expired = [
            nonce
            for nonce, reward in self._by_nonce.items()
            if reward.verified_at < expiry
        ]
        for nonce in expired:
            reward = self._by_nonce.pop(nonce)
            self._transactions.discard(reward.transaction_id)
