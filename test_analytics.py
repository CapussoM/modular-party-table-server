from fastapi.testclient import TestClient

import main


def test_analytics_event_is_validated_and_queued(monkeypatch):
    recorded = []
    monkeypatch.setattr(main.analytics, "record", lambda event: recorded.append(event) or True)
    with TestClient(main.app) as client:
        response = client.post("/v1/analytics/events", json={
            "event_name": "game_started",
            "session_id": "session-1",
            "properties": {"game_id": "hidden_word", "player_count": 4},
        })
    assert response.status_code == 202
    assert response.json() == {"accepted": True}
    assert recorded[0]["event_name"] == "game_started"


def test_analytics_event_rejects_invalid_name():
    with TestClient(main.app) as client:
        response = client.post("/v1/analytics/events", json={
            "event_name": "Invalid event name",
        })
    assert response.status_code == 422


def test_health_exposes_safe_analytics_status():
    with TestClient(main.app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["analytics"] in {"disabled", "connected", "unavailable"}


def test_room_creation_records_kpi(monkeypatch):
    recorded = []
    monkeypatch.setattr(main.analytics, "record", lambda event: recorded.append(event) or True)
    with TestClient(main.app) as client:
        with client.websocket_connect("/ws") as socket:
            socket.receive_json()
            socket.send_json({
                "type": "create_room",
                "gameId": "hidden_word",
                "public": False,
                "maxPlayers": 8,
                "profile": {"display_name": "Test"},
            })
            assert socket.receive_json()["type"] == "room_created"
    event = next(item for item in recorded if item["event_name"] == "room_created")
    assert event["properties"] == {
        "game_id": "hidden_word",
        "public": False,
        "max_players": 8,
        "join_method": "create",
    }
