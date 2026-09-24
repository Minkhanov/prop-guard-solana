"""Panel server. Runs inside the guard process and reads the shared `Guard` object.

Endpoints:
  GET /              static page
  GET /api/state     latest snapshot + rule summary + stream health + recent alerts
  GET /api/health    stream health only (for uptime checks)
  GET /events        SSE stream of /api/state payloads (pushed on every engine tick)
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

STATIC = Path(__file__).with_name("static")


def create_app(guard: Any) -> FastAPI:
    app = FastAPI(title="Prop Guard", version="0.1.0", docs_url=None, redoc_url=None)

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return (STATIC / "index.html").read_text(encoding="utf-8")

    @app.get("/api/state")
    async def state() -> JSONResponse:
        return JSONResponse(guard.payload())

    @app.get("/api/health")
    async def health() -> JSONResponse:
        h = guard.metrics.to_dict()
        stale = guard.settings.stream_stale_sec
        # stream_ok  = the primary stream delivered data recently (pings do not count; bootstrap-only silence is not ok)
        # ok         = the engine is being fed by *some* path (stream or RPC fallback) within the staleness budget
        stream_ok = bool(h["messages_total"]) and h["silence_sec"] < stale
        fed = h["update_age_sec"] < max(stale, guard.settings.fallback_poll_interval_ms / 1000 * 3)
        ok = stream_ok or (h["fallback_active"] and fed)
        body = {"ok": ok, "stream_ok": stream_ok, "degraded": ok and not stream_ok, **h,
                "alerting": guard.router.stats(), "thresholds": {"stream_stale_sec": stale,
                "fallback_silence_sec": guard.settings.fallback_silence_sec, "oracle_stale_sec": guard.settings.oracle_stale_sec}}
        return JSONResponse(body, status_code=200 if ok else 503)

    @app.get("/events")
    async def events() -> StreamingResponse:
        queue: asyncio.Queue = guard.subscribe()

        async def gen():
            try:
                yield f"data: {json.dumps(guard.payload())}\n\n"
                while True:
                    try:
                        payload = await asyncio.wait_for(queue.get(), timeout=15)
                        yield f"data: {json.dumps(payload)}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
            finally:
                guard.unsubscribe(queue)

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app


async def serve(guard: Any, host: str, port: int) -> None:
    import uvicorn
    config = uvicorn.Config(create_app(guard), host=host, port=port, log_level="warning", lifespan="off")
    server = uvicorn.Server(config)
    await server.serve()
