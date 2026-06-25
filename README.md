# FastAPI development server

This single FastAPI application provides:

- REST mock endpoints for profiles and rewarded-ad tokens
- WebSocket room presence and signaling
- SDP offer, answer, and ICE forwarding
- temporary room presence and cleanup
- deep-link metadata
- public-room matchmaking filtered by game
- automatic public-room creation when no compatible lobby is available
- host-selected room capacity enforced for code joins and matchmaking
- cryptographically verified AdMob rewarded SSV callbacks

Normal gameplay travels over direct WebRTC data channels and does not pass
through this server. Game state remains host-authoritative.

`ALLOW_APP_RELAY=true` enables a targeted WebSocket fallback for peers whose
networks cannot establish a direct route. It defaults to `false` for the lowest
possible server bandwidth. `MAX_ROOM_PEERS`, `MAX_SIGNAL_BYTES`, and
`MAX_APP_BYTES` bound server work and memory use. Connections exceeding
`MAX_MESSAGES_PER_SECOND` are closed.

Drawing games use compact normalized strokes. The default `MAX_APP_BYTES` is
512 KB so complete Telesketch chains can also travel through the relay fallback.

The canonical source is the `server/` directory in the private
`modular-party-table` repository. A GitHub Actions workflow publishes this
directory to the deploy-only repository, which triggers Render automatically.

## Start

```bash
cd server
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

Open `http://127.0.0.1:8080/docs` for the REST API documentation.

## Render Free deployment

The repository root includes `render.yaml`. It creates one free Python web
service with `server/` as its root directory.

Production endpoints:

```text
https://modular-party-table.onrender.com/health
wss://modular-party-table.onrender.com/ws
https://modular-party-table.onrender.com/admob/ssv
```

Configure that HTTPS URL as the server-side verification callback for the
Android `UnlockAd` rewarded unit. The server verifies Google's ECDSA signature,
the production ad unit ID, callback age and transaction replay before exposing
the short-lived result to the app.

Render Free sleeps after 15 minutes without inbound traffic. A new HTTP request
or WebSocket connection wakes it, which can take about one minute. Connected
Godot clients send a heartbeat every 20 seconds, so an active room remains
awake.

## Test three signaling clients

Keep the server running, then use another terminal:

```bash
cd server
source .venv/bin/activate
python test_multiclient.py
```

The script tests room presence, signaling and public quick join. It verifies
that private rooms are ignored and that matchmaking selects a public room for
the requested game.
