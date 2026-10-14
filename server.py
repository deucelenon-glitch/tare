#!/usr/bin/env python3
"""TARE client support — visitor <-> agent messaging.

Real-time support chat for the TARE site. Visitors open the widget, send messages;
an agent replies via POST /api/reply (shared key). Every inbound visitor message is
pushed to ntfy topic LifeOS so a submission never lands silently.

Binds 127.0.0.1 by default (inbound endpoint). Static site mounted LAST.
"""
import json
import secrets
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)
DB = DATA / "support.db"
KEY_FILE = BASE / ".reply-key"
NTFY_URL = "https://ntfy.sh"
NTFY_TOPIC = "LifeOS"

app = FastAPI(title="tare-support", docs_url=None, redoc_url=None)

# --- reply key (generated once, chmod 600, never public) ---
if KEY_FILE.exists():
    REPLY_KEY = KEY_FILE.read_text().strip()
else:
    REPLY_KEY = secrets.token_urlsafe(24)
    KEY_FILE.write_text(REPLY_KEY)
    KEY_FILE.chmod(0o600)

with sqlite3.connect(DB) as conn:
    conn.execute("""CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        role TEXT NOT NULL,
        client_id TEXT,
        text TEXT,
        remote_addr TEXT,
        user_agent TEXT)""")
    conn.commit()

# --- in-memory hub ---
class Hub:
    def __init__(self):
        self.sockets = set()

    async def join(self, ws):
        self.sockets.add(ws)

    def leave(self, ws):
        self.sockets.discard(ws)

    async def broadcast(self, payload: dict):
        dead = []
        for ws in list(self.sockets):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.sockets.discard(ws)


hub = Hub()

# --- rate limit: addr -> [monotonic ts], max 8 per 20s ---
_hits: dict[str, list[float]] = {}
RATE_N, RATE_WINDOW = 8, 20.0


def _rate_ok(addr: str) -> bool:
    now = time.monotonic()
    hist = [t for t in _hits.get(addr, []) if now - t < RATE_WINDOW]
    if len(hist) >= RATE_N:
        _hits[addr] = hist
        return False
    hist.append(now)
    _hits[addr] = hist
    return True


def _ntfy(title: str, body: str, tags: str = "incoming_envelope"):
    """Push to ntfy. Text in the JSON BODY (never a header)."""
    payload = json.dumps({"topic": NTFY_TOPIC, "title": title, "message": body,
                          "tags": tags.split(",")}).encode("utf-8")
    try:
        req = urllib.request.Request(NTFY_URL, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            ok = resp.status == 200
        print(f"[ntfy] ok={ok} {title}", flush=True)
    except Exception as e:  # never let a push failure break the chat
        print(f"[ntfy] ok=False error={e}", flush=True)


def _store(role: str, text: str, client_id: str, addr: str, ua: str) -> int:
    rec = {
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "role": role, "client_id": client_id, "text": text,
        "remote_addr": addr, "user_agent": ua,
    }
    with sqlite3.connect(DB) as conn:
        cur = conn.execute("""INSERT INTO messages
            (created_at, role, client_id, text, remote_addr, user_agent)
            VALUES (:created_at, :role, :client_id, :text, :remote_addr, :user_agent)""", rec)
        conn.commit()
        return cur.lastrowid


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await hub.join(ws)
    addr = ws.client.host if ws.client else "?"
    ua = ws.headers.get("user-agent", "")
    client_id = ws.query_params.get("client", "anon")
    try:
        while True:
            msg = await ws.receive_json()
            text = (msg.get("text") or "").strip()
            if not text:
                continue
            if not _rate_ok(addr):
                await ws.send_json({"role": "system", "text": "Slow down a moment — too many messages."})
                continue
            mid = _store("visitor", text, client_id, addr, ua)
            await hub.broadcast({"id": mid, "role": "visitor", "client": client_id, "text": text})
            _ntfy("TARE support", f"[{client_id}] {text}")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        hub.leave(ws)


@app.post("/api/reply")
async def reply(request: Request, x_chat_key: str = Header(default="")):
    if x_chat_key != REPLY_KEY:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    body = await request.json()
    text = (body.get("text") or "").strip()
    if not text:
        return JSONResponse({"ok": False, "error": "empty"}, status_code=400)
    mid = _store("agent", text, body.get("client", "broadcast"), "", "agent")
    await hub.broadcast({"id": mid, "role": "agent", "text": text})
    return {"ok": True, "id": mid}


@app.get("/api/messages")
async def messages(limit: int = 50):
    with sqlite3.connect(DB) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM messages ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return {"count": len(rows), "messages": [dict(r) for r in reversed(rows)]}


@app.get("/api/count")
async def count():
    with sqlite3.connect(DB) as conn:
        return {"count": conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]}


@app.post("/a2a/v1")
async def a2a_v1(request: Request, authorization: str = Header(default="")):
    """A2A 1.0 JSON-RPC binding. Accepts SendMessage; routes the text to support
    and records it as an agent-to-agent task. Bearer-gated like the reply route."""
    token = authorization.replace("Bearer ", "").strip()
    if token != REPLY_KEY:
        return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32001, "message": "unauthorized"}},
                            status_code=401)
    body = await request.json()
    method = body.get("method", "")
    rid = body.get("id")
    if method in ("SendMessage", "message/send", "SendStreamingMessage"):
        params = body.get("params", {}) or {}
        msg = params.get("message", {}) or {}
        text = ""
        for part in (msg.get("parts") or []):
            if isinstance(part, dict) and part.get("text"):
                text += part["text"]
        if not text:
            text = json.dumps(params.get("artifacts", []))[:1000]
        text = text.strip() or "(empty task)"
        mid = _store("agent", f"[A2A task] {text}", "a2a", "", "a2a-client")
        _ntfy("TARE A2A task", text[:300])
        await hub.broadcast({"id": mid, "role": "agent", "text": f"[A2A] {text}"})
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "taskId": f"tare-{mid}", "contextId": f"ctx-{mid}",
            "status": {"state": "completed"},
            "artifacts": [{"artifactId": f"art-{mid}", "parts": [{
                "text": "Task accepted by TARE. An extraction lane has been queued and an agent will follow up on this channel."}]}],
            "history": []}}
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"method not found: {method}"}}


@app.get("/.well-known/agent-card.json")
async def agent_card():
    return JSONResponse(json.loads((BASE / ".well-known" / "agent-card.json").read_text()))


@app.get("/.well-known/agent.json")
async def agent_card_legacy():
    return JSONResponse(json.loads((BASE / ".well-known" / "agent-card.json").read_text()))


app.mount("/", StaticFiles(directory=str(BASE), html=True), name="site")  # LAST

if __name__ == "__main__":
    import uvicorn
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8123
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    uvicorn.run(app, host=host, port=port, log_level="warning")
