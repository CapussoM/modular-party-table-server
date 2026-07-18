from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


SESSION_TTL_SECONDS = 90 * 24 * 60 * 60
MAX_PROFILE_BYTES = 256 * 1024

SYNCED_PROFILE_KEYS = {
    "locale",
    "nickname",
    "avatar",
    "owned_entitlements",
    "active_subscription_entitlements",
    "sound_enabled",
    "ui_theme",
    "font_style",
    "avatar_render_style",
    "adult_team_names_enabled",
    "adult_team_names_opt_in_v2",
    "coins",
    "rewarded_ads_completed",
    "rewarded_ads_since_chest",
    "unopened_chests",
    "premium_chests",
    "mission_state",
    "avatar_inventory",
    "avatar_equipment",
}


@dataclass(frozen=True)
class CloudProfile:
    user_id: str
    revision: int
    profile: dict[str, Any]
    updated_at: int


@dataclass(frozen=True)
class CloudSession:
    token: str
    expires_at: int
    profile: CloudProfile
    created: bool


class ProfileRevisionConflict(Exception):
    def __init__(self, current: CloudProfile) -> None:
        super().__init__("profile_revision_conflict")
        self.current = current


def sanitize_cloud_profile(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    result = {
        key: value[key]
        for key in SYNCED_PROFILE_KEYS
        if key in value
    }
    result["locale"] = _bounded_string(result.get("locale", "it"), 16)
    result["nickname"] = _bounded_string(
        result.get("nickname", "Giocatore"),
        40,
    )
    result["avatar"] = _bounded_string(result.get("avatar", "modular"), 64)
    result["owned_entitlements"] = _string_list(
        result.get("owned_entitlements", ["base"]),
        256,
        128,
    )
    if "base" not in result["owned_entitlements"]:
        result["owned_entitlements"].insert(0, "base")
    result["active_subscription_entitlements"] = _string_list(
        result.get("active_subscription_entitlements", []),
        64,
        128,
    )
    result["avatar_inventory"] = _string_list(
        result.get("avatar_inventory", []),
        2048,
        128,
    )
    result["avatar_equipment"] = _string_dict(
        result.get("avatar_equipment", {}),
        64,
        64,
        128,
    )
    result["sound_enabled"] = bool(result.get("sound_enabled", True))
    result["ui_theme"] = _bounded_string(
        result.get("ui_theme", "base"),
        32,
    )
    result["font_style"] = _bounded_string(
        result.get("font_style", "normal"),
        32,
    )
    result["avatar_render_style"] = _bounded_string(
        result.get("avatar_render_style", "classic"),
        32,
    )
    result["adult_team_names_enabled"] = bool(
        result.get("adult_team_names_enabled", False)
    )
    result["adult_team_names_opt_in_v2"] = bool(
        result.get("adult_team_names_opt_in_v2", True)
    )
    for key in (
        "coins",
        "rewarded_ads_completed",
        "rewarded_ads_since_chest",
        "unopened_chests",
    ):
        result[key] = _bounded_non_negative_int(
            result.get(key, 0),
            2_000_000_000,
        )
    premium = result.get("premium_chests", {})
    if not isinstance(premium, dict):
        premium = {}
    result["premium_chests"] = {
        tier: _bounded_non_negative_int(premium.get(tier, 0), 1_000_000)
        for tier in ("rare", "epic", "legendary")
    }
    result["mission_state"] = _sanitize_mission_state(
        result.get("mission_state", {})
    )

    encoded = json.dumps(
        result,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > MAX_PROFILE_BYTES:
        raise ValueError("profile_too_large")
    return result


class CloudProfileStore:
    def __init__(self, path: str, identity_pepper: str = "") -> None:
        self.path = path
        self._pepper = identity_pepper.encode("utf-8")
        self._lock = threading.RLock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def create_session(
        self,
        provider: str,
        subject: str,
        initial_profile: dict[str, Any],
        now: Optional[int] = None,
    ) -> CloudSession:
        timestamp = int(time.time()) if now is None else now
        provider = _bounded_string(provider, 64)
        subject = _bounded_string(subject, 512)
        if not provider or not subject:
            raise ValueError("invalid_platform_identity")
        subject_hash = self._identity_hash(provider, subject)
        profile = sanitize_cloud_profile(initial_profile)
        token = secrets.token_urlsafe(48)
        token_hash = self._token_hash(token)
        expires_at = timestamp + SESSION_TTL_SECONDS
        created = False

        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            identity = connection.execute(
                """
                SELECT user_id
                FROM cloud_identities
                WHERE provider = ? AND subject_hash = ?
                """,
                (provider, subject_hash),
            ).fetchone()
            if identity is None:
                created = True
                user_id = str(uuid.uuid4())
                connection.execute(
                    """
                    INSERT INTO cloud_users(user_id, created_at, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (user_id, timestamp, timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO cloud_identities(
                        provider, subject_hash, user_id, created_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (provider, subject_hash, user_id, timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO cloud_profiles(
                        user_id, revision, profile_json, updated_at
                    )
                    VALUES (?, 1, ?, ?)
                    """,
                    (
                        user_id,
                        json.dumps(profile, ensure_ascii=False),
                        timestamp,
                    ),
                )
            else:
                user_id = str(identity["user_id"])

            connection.execute(
                """
                INSERT INTO cloud_sessions(
                    token_hash, user_id, created_at, expires_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (token_hash, user_id, timestamp, expires_at),
            )
            connection.execute(
                "DELETE FROM cloud_sessions WHERE expires_at <= ?",
                (timestamp,),
            )
            current = self._profile_from_connection(connection, user_id)
            connection.commit()

        return CloudSession(
            token=token,
            expires_at=expires_at,
            profile=current,
            created=created,
        )

    def get_profile_for_token(
        self,
        token: str,
        now: Optional[int] = None,
    ) -> Optional[CloudProfile]:
        timestamp = int(time.time()) if now is None else now
        token_hash = self._token_hash(token)
        with self._lock, self._connect() as connection:
            session = connection.execute(
                """
                SELECT user_id
                FROM cloud_sessions
                WHERE token_hash = ? AND expires_at > ?
                """,
                (token_hash, timestamp),
            ).fetchone()
            if session is None:
                return None
            return self._profile_from_connection(
                connection,
                str(session["user_id"]),
            )

    def update_profile_for_token(
        self,
        token: str,
        expected_revision: int,
        profile: dict[str, Any],
        now: Optional[int] = None,
    ) -> Optional[CloudProfile]:
        timestamp = int(time.time()) if now is None else now
        token_hash = self._token_hash(token)
        sanitized = sanitize_cloud_profile(profile)
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                """
                SELECT user_id
                FROM cloud_sessions
                WHERE token_hash = ? AND expires_at > ?
                """,
                (token_hash, timestamp),
            ).fetchone()
            if session is None:
                connection.rollback()
                return None
            user_id = str(session["user_id"])
            current = self._profile_from_connection(connection, user_id)
            if current.revision != expected_revision:
                connection.rollback()
                raise ProfileRevisionConflict(current)
            next_revision = current.revision + 1
            connection.execute(
                """
                UPDATE cloud_profiles
                SET revision = ?, profile_json = ?, updated_at = ?
                WHERE user_id = ?
                """,
                (
                    next_revision,
                    json.dumps(sanitized, ensure_ascii=False),
                    timestamp,
                    user_id,
                ),
            )
            connection.execute(
                "UPDATE cloud_users SET updated_at = ? WHERE user_id = ?",
                (timestamp, user_id),
            )
            connection.commit()
            return CloudProfile(
                user_id=user_id,
                revision=next_revision,
                profile=sanitized,
                updated_at=timestamp,
            )

    def revoke_session(self, token: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                "DELETE FROM cloud_sessions WHERE token_hash = ?",
                (self._token_hash(token),),
            )
            connection.commit()

    def _initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS cloud_users (
                    user_id TEXT PRIMARY KEY,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cloud_identities (
                    provider TEXT NOT NULL,
                    subject_hash TEXT NOT NULL,
                    user_id TEXT NOT NULL REFERENCES cloud_users(user_id),
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY(provider, subject_hash)
                );

                CREATE TABLE IF NOT EXISTS cloud_profiles (
                    user_id TEXT PRIMARY KEY REFERENCES cloud_users(user_id),
                    revision INTEGER NOT NULL,
                    profile_json TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cloud_sessions (
                    token_hash TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES cloud_users(user_id),
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS cloud_sessions_user_idx
                    ON cloud_sessions(user_id);
                CREATE INDEX IF NOT EXISTS cloud_sessions_expiry_idx
                    ON cloud_sessions(expires_at);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def _profile_from_connection(
        self,
        connection: sqlite3.Connection,
        user_id: str,
    ) -> CloudProfile:
        row = connection.execute(
            """
            SELECT revision, profile_json, updated_at
            FROM cloud_profiles
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("cloud_profile_missing")
        payload = json.loads(str(row["profile_json"]))
        return CloudProfile(
            user_id=user_id,
            revision=int(row["revision"]),
            profile=sanitize_cloud_profile(payload),
            updated_at=int(row["updated_at"]),
        )

    def _identity_hash(self, provider: str, subject: str) -> str:
        message = f"{provider}:{subject}".encode("utf-8")
        if self._pepper:
            return hmac.new(
                self._pepper,
                message,
                hashlib.sha256,
            ).hexdigest()
        return hashlib.sha256(message).hexdigest()

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()


def cloud_profile_response(profile: CloudProfile) -> dict[str, Any]:
    return {
        "user_id": profile.user_id,
        "revision": profile.revision,
        "profile": profile.profile,
        "updated_at": profile.updated_at,
    }


def _bounded_string(value: Any, max_length: int) -> str:
    return str(value).strip()[:max_length]


def _bounded_non_negative_int(value: Any, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 0
    return max(0, min(parsed, maximum))


def _string_list(value: Any, max_items: int, max_length: int) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value[:max_items]:
        candidate = _bounded_string(item, max_length)
        if candidate and candidate not in result:
            result.append(candidate)
    return result


def _string_dict(
    value: Any,
    max_items: int,
    max_key_length: int,
    max_value_length: int,
) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, str] = {}
    for raw_key, raw_value in list(value.items())[:max_items]:
        key = _bounded_string(raw_key, max_key_length)
        item = _bounded_string(raw_value, max_value_length)
        if key and item:
            result[key] = item
    return result


def _sanitize_mission_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    claims = value.get("claims", {})
    sanitized_claims: dict[str, bool | str] = {}
    if isinstance(claims, dict):
        for raw_key, raw_value in list(claims.items())[:256]:
            key = _bounded_string(raw_key, 128)
            if not key:
                continue
            if isinstance(raw_value, bool):
                sanitized_claims[key] = raw_value
            else:
                sanitized_claims[key] = _bounded_string(raw_value, 32)
    return {
        "last_ad_day": _bounded_string(value.get("last_ad_day", ""), 32),
        "last_ad_ordinal": max(
            -999999,
            min(
                _safe_int(value.get("last_ad_ordinal", -999999), -999999),
                10_000_000,
            ),
        ),
        "ad_streak": _bounded_non_negative_int(
            value.get("ad_streak", 0),
            1_000_000,
        ),
        "chests_opened": _bounded_non_negative_int(
            value.get("chests_opened", 0),
            1_000_000_000,
        ),
        "claims": sanitized_claims,
    }


def _safe_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback
