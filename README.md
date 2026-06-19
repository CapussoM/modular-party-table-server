# FastAPI development server

This single FastAPI application provides:

- REST mock endpoints for profiles and rewarded-ad tokens
- WebSocket room signaling
- SDP offer, answer, and ICE forwarding
- temporary room presence and cleanup
- deep-link metadata

It does not process or store gameplay. Game state remains host-authoritative.

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
```

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

The script creates one host and joins two guests through independent WebSocket
connections. This tests room presence and signaling, not Godot gameplay
replication.
