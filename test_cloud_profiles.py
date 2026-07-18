from __future__ import annotations

from fastapi.testclient import TestClient

import main
from cloud_profiles import CloudProfileStore, ProfileRevisionConflict
from platform_identity import PlatformIdentityVerifier


def test_platform_identity_restores_same_cloud_profile(tmp_path) -> None:
    store = CloudProfileStore(
        str(tmp_path / "profiles.sqlite3"),
        "test-pepper",
    )
    first = store.create_session(
        "google_play_games",
        "player-123",
        {
            "nickname": "Marco",
            "coins": 250,
            "avatar_inventory": ["face_round"],
            "purchase_account_id": "must-not-sync",
        },
        now=1000,
    )
    updated = store.update_profile_for_token(
        first.token,
        first.profile.revision,
        {
            **first.profile.profile,
            "coins": 750,
            "avatar_inventory": ["face_round", "hair_long"],
        },
        now=1001,
    )
    assert updated is not None

    second = store.create_session(
        "google_play_games",
        "player-123",
        {"nickname": "Other device", "coins": 0},
        now=1002,
    )

    assert second.created is False
    assert second.profile.user_id == first.profile.user_id
    assert second.profile.revision == 2
    assert second.profile.profile["nickname"] == "Marco"
    assert second.profile.profile["coins"] == 750
    assert second.profile.profile["avatar_inventory"] == [
        "face_round",
        "hair_long",
    ]
    assert "purchase_account_id" not in second.profile.profile


def test_revision_conflict_preserves_newer_profile(tmp_path) -> None:
    store = CloudProfileStore(str(tmp_path / "profiles.sqlite3"))
    session = store.create_session(
        "apple_app_store",
        "app-transaction-123",
        {"coins": 100},
        now=2000,
    )
    store.update_profile_for_token(
        session.token,
        1,
        {"coins": 200},
        now=2001,
    )

    try:
        store.update_profile_for_token(
            session.token,
            1,
            {"coins": 999},
            now=2002,
        )
        raise AssertionError("Expected ProfileRevisionConflict")
    except ProfileRevisionConflict as error:
        assert error.current.revision == 2
        assert error.current.profile["coins"] == 200


def test_cloud_api_session_get_update_and_conflict(tmp_path) -> None:
    original_store = main.cloud_profiles
    original_verifier = main.platform_identity
    main.cloud_profiles = CloudProfileStore(
        str(tmp_path / "profiles.sqlite3"),
        "test-pepper",
    )
    main.platform_identity = PlatformIdentityVerifier(allow_debug=True)
    client = TestClient(main.app)
    try:
        session_response = client.post(
            "/v1/cloud/session/platform",
            json={
                "provider": "debug",
                "credential": "automatic-player-id",
                "initial_profile": {
                    "nickname": "Giocatore cloud",
                    "coins": 300,
                },
            },
        )
        assert session_response.status_code == 200
        session = session_response.json()
        headers = {
            "Authorization": f"Bearer {session['session_token']}",
        }

        profile_response = client.get(
            "/v1/cloud/profile",
            headers=headers,
        )
        assert profile_response.status_code == 200
        assert profile_response.json()["profile"]["coins"] == 300

        update_response = client.put(
            "/v1/cloud/profile",
            headers=headers,
            json={
                "expected_revision": 1,
                "profile": {
                    **profile_response.json()["profile"],
                    "coins": 550,
                },
            },
        )
        assert update_response.status_code == 200
        assert update_response.json()["revision"] == 2

        conflict_response = client.put(
            "/v1/cloud/profile",
            headers=headers,
            json={
                "expected_revision": 1,
                "profile": {"coins": 999},
            },
        )
        assert conflict_response.status_code == 409
        current = conflict_response.json()["detail"]["current"]
        assert current["revision"] == 2
        assert current["profile"]["coins"] == 550
    finally:
        client.close()
        main.cloud_profiles = original_store
        main.platform_identity = original_verifier
