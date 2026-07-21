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
