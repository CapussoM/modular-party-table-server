from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature


@dataclass(frozen=True)
class VerifiedPlatformIdentity:
    provider: str
    subject: str


class PlatformIdentityError(Exception):
    def __init__(self, code: str, status_code: int = 401) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


class PlatformIdentityVerifier:
    def __init__(
        self,
        *,
        google_client_id: str = "",
        google_client_secret: str = "",
        apple_bundle_id: str = "",
        apple_root_fingerprints: Optional[set[str]] = None,
        apple_root_certificates: Optional[list[x509.Certificate]] = None,
        allow_debug: bool = False,
    ) -> None:
        self.google_client_id = google_client_id.strip()
        self.google_client_secret = google_client_secret.strip()
        self.apple_bundle_id = apple_bundle_id.strip()
        self.apple_root_certificates = apple_root_certificates or []
        configured_fingerprints = {
            _normalize_fingerprint(item)
            for item in (apple_root_fingerprints or set())
            if _normalize_fingerprint(item)
        }
        self.apple_root_fingerprints = configured_fingerprints or {
            certificate.fingerprint(hashes.SHA256()).hex().lower()
            for certificate in self.apple_root_certificates
        }
        self.allow_debug = allow_debug

    @classmethod
    def from_environment(cls) -> PlatformIdentityVerifier:
        root_certificates = _load_apple_root_certificates()
        return cls(
            google_client_id=os.getenv("GOOGLE_PGS_CLIENT_ID", ""),
            google_client_secret=os.getenv("GOOGLE_PGS_CLIENT_SECRET", ""),
            apple_bundle_id=os.getenv(
                "APPLE_BUNDLE_ID",
                "com.stegosaurini.partygames",
            ),
            apple_root_fingerprints=set(
                os.getenv("APPLE_ROOT_CA_SHA256", "").split(",")
            ),
            apple_root_certificates=root_certificates,
            allow_debug=(
                os.getenv("CLOUD_ALLOW_DEBUG_IDENTITY", "false").lower()
                == "true"
            ),
        )

    async def verify(
        self,
        provider: str,
        credential: str,
    ) -> VerifiedPlatformIdentity:
        provider = provider.strip()
        credential = credential.strip()
        if not provider or not credential:
            raise PlatformIdentityError("platform_identity_missing")
        if provider == "google_play_games":
            return await self._verify_google_play_games(credential)
        if provider == "apple_app_store":
            return self._verify_apple_app_transaction(credential)
        if provider == "debug" and self.allow_debug:
            return VerifiedPlatformIdentity(provider="debug", subject=credential)
        raise PlatformIdentityError("platform_identity_provider_unsupported")

    async def _verify_google_play_games(
        self,
        server_auth_code: str,
    ) -> VerifiedPlatformIdentity:
        if not self.google_client_id or not self.google_client_secret:
            raise PlatformIdentityError(
                "google_play_games_not_configured",
                503,
            )
        async with httpx.AsyncClient(timeout=10.0) as client:
            token_response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": self.google_client_id,
                    "client_secret": self.google_client_secret,
                    "code": server_auth_code,
                    "grant_type": "authorization_code",
                    "redirect_uri": "",
                },
            )
            if token_response.status_code != 200:
                raise PlatformIdentityError(
                    "google_play_games_code_rejected"
                )
            access_token = str(
                token_response.json().get("access_token", "")
            ).strip()
            if not access_token:
                raise PlatformIdentityError(
                    "google_play_games_token_missing"
                )
            player_response = await client.get(
                "https://games.googleapis.com/games/v1/players/me",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if player_response.status_code != 200:
                raise PlatformIdentityError(
                    "google_play_games_player_rejected"
                )
            player_id = str(player_response.json().get("playerId", "")).strip()
            if not player_id:
                raise PlatformIdentityError(
                    "google_play_games_player_missing"
                )
            return VerifiedPlatformIdentity(
                provider="google_play_games",
                subject=player_id,
            )

    def _verify_apple_app_transaction(
        self,
        signed_transaction: str,
    ) -> VerifiedPlatformIdentity:
        if not self.apple_bundle_id or not self.apple_root_fingerprints:
            raise PlatformIdentityError(
                "apple_app_store_not_configured",
                503,
            )
        try:
            header_segment, payload_segment, signature_segment = (
                signed_transaction.split(".")
            )
            header = json.loads(_decode_base64url(header_segment))
            payload = json.loads(_decode_base64url(payload_segment))
            certificates = [
                x509.load_der_x509_certificate(base64.b64decode(encoded))
                for encoded in header.get("x5c", [])
            ]
        except (ValueError, TypeError, json.JSONDecodeError) as error:
            raise PlatformIdentityError(
                "apple_app_transaction_invalid"
            ) from error
        if header.get("alg") != "ES256" or len(certificates) < 2:
            raise PlatformIdentityError("apple_app_transaction_invalid")

        now = time.time()
        for certificate in certificates:
            if (
                certificate.not_valid_before_utc.timestamp() > now
                or certificate.not_valid_after_utc.timestamp() < now
            ):
                raise PlatformIdentityError(
                    "apple_app_transaction_certificate_expired"
                )
        for child, issuer in zip(certificates, certificates[1:]):
            _verify_certificate_signature(child, issuer)

        root = certificates[-1]
        root_fingerprint = root.fingerprint(hashes.SHA256()).hex().lower()
        if root_fingerprint not in self.apple_root_fingerprints:
            trusted_root = next(
                (
                    candidate
                    for candidate in self.apple_root_certificates
                    if candidate.subject == root.issuer
                    and candidate.fingerprint(hashes.SHA256())
                    .hex()
                    .lower()
                    in self.apple_root_fingerprints
                ),
                None,
            )
            if trusted_root is None:
                raise PlatformIdentityError(
                    "apple_app_transaction_untrusted_root"
                )
            _verify_certificate_signature(root, trusted_root)

        try:
            signature = _decode_base64url_bytes(signature_segment)
            if len(signature) != 64:
                raise ValueError("invalid_es256_signature")
            der_signature = encode_dss_signature(
                int.from_bytes(signature[:32], "big"),
                int.from_bytes(signature[32:], "big"),
            )
            public_key = certificates[0].public_key()
            if not isinstance(public_key, ec.EllipticCurvePublicKey):
                raise ValueError("invalid_es256_key")
            public_key.verify(
                der_signature,
                f"{header_segment}.{payload_segment}".encode("ascii"),
                ec.ECDSA(hashes.SHA256()),
            )
        except Exception as error:
            raise PlatformIdentityError(
                "apple_app_transaction_signature_invalid"
            ) from error

        bundle_id = str(payload.get("bundleId", "")).strip()
        transaction_id = str(payload.get("appTransactionId", "")).strip()
        if bundle_id != self.apple_bundle_id or not transaction_id:
            raise PlatformIdentityError(
                "apple_app_transaction_payload_invalid"
            )
        return VerifiedPlatformIdentity(
            provider="apple_app_store",
            subject=transaction_id,
        )


def _verify_certificate_signature(
    certificate: x509.Certificate,
    issuer: x509.Certificate,
) -> None:
    if certificate.issuer != issuer.subject:
        raise PlatformIdentityError(
            "apple_app_transaction_certificate_chain_invalid"
        )
    public_key = issuer.public_key()
    try:
        if isinstance(public_key, rsa.RSAPublicKey):
            public_key.verify(
                certificate.signature,
                certificate.tbs_certificate_bytes,
                padding.PKCS1v15(),
                certificate.signature_hash_algorithm,
            )
        elif isinstance(public_key, ec.EllipticCurvePublicKey):
            public_key.verify(
                certificate.signature,
                certificate.tbs_certificate_bytes,
                ec.ECDSA(certificate.signature_hash_algorithm),
            )
        else:
            raise TypeError("unsupported_certificate_key")
    except Exception as error:
        raise PlatformIdentityError(
            "apple_app_transaction_certificate_chain_invalid"
        ) from error


def _decode_base64url(value: str) -> str:
    return _decode_base64url_bytes(value).decode("utf-8")


def _decode_base64url_bytes(value: str) -> bytes:
    padding_length = (-len(value)) % 4
    return base64.urlsafe_b64decode(value + ("=" * padding_length))


def _normalize_fingerprint(value: str) -> str:
    return "".join(character for character in value.lower() if character in "0123456789abcdef")


def _load_apple_root_certificates() -> list[x509.Certificate]:
    result: list[x509.Certificate] = []
    certificate_dir = Path(__file__).resolve().parent / "certs"
    for filename in ("AppleRootCA-G2.cer", "AppleRootCA-G3.cer"):
        path = certificate_dir / filename
        if not path.is_file():
            continue
        result.append(x509.load_der_x509_certificate(path.read_bytes()))
    return result
