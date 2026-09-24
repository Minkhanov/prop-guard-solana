"""Solami Mirage transport: Yellowstone `SubscribeUpdate` frames over a plain WebSocket.

    wss://ws.solami.dev/mirage/stream/{subscription_id}?api_key=KEY

The subscription (saved filter) is created once in the Solami dashboard or via
`POST https://api.solami.dev/mirage/create` with an API key that carries the
`MirageManage` role (`Authorization: Bearer`); the stream itself needs `MirageStream`.
Frames are binary protobuf, decoded with the same generated stubs as gRPC, so the
engine sees identical `AccountUpdate`s.
Close codes: 4029 = concurrent-stream limit, 4002 = bandwidth+balance exhausted,
1001 = node restart (reconnect).

Status 2026-09-24: written against the Solami docs, NOT exercised live — the trial key
has no Mirage role (`/mirage/list` → 403 "missing required permission: MirageView").
"""
from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from ..decode.borsh import b58encode
from .base import AccountUpdate, SlotUpdate, Transport, UpdateHandler
from .grpc_yellowstone import _load_stubs

log = logging.getLogger("propguard.mirage")

CLOSE_CODES = {4029: "concurrent stream limit reached", 4002: "bandwidth and balance exhausted", 1001: "node restart"}


class MirageTransport(Transport):
    name = "mirage"

    def __init__(self, url: str, api_key: str, *, metrics: Any | None = None, backoff_max_s: float = 30.0):
        self.base_url = url
        self.api_key = api_key
        self.url = f"{url}?api_key={api_key}" if api_key else url
        self.metrics = metrics
        self.backoff_max_s = backoff_max_s
        self._stop = asyncio.Event()
        self.connected = asyncio.Event()

    def _redact(self, text: str) -> str:
        """`websockets` exceptions (InvalidStatus / InvalidURI) may carry the full URI incl. the key."""
        return text.replace(self.api_key, "***") if self.api_key else text

    async def run(self, handler: UpdateHandler) -> None:
        import websockets
        geyser_pb2, _ = _load_stubs()
        backoff = 1.0
        first = True
        while not self._stop.is_set():
            if not first:
                if self.metrics:
                    self.metrics.on_reconnect()
                await asyncio.sleep(backoff + random.uniform(0, backoff / 2))
                backoff = min(backoff * 2, self.backoff_max_s)
                if self._stop.is_set():
                    break
            first = False
            try:
                log.info("Mirage connect %s", self.base_url)
                async with websockets.connect(self.url, max_size=64 * 1024 * 1024, ping_interval=15) as ws:
                    async for frame in ws:
                        if isinstance(frame, str):
                            log.debug("mirage text frame: %s", self._redact(frame[:200]))
                            continue
                        update = geyser_pb2.SubscribeUpdate.FromString(frame)
                        kind = update.WhichOneof("update_oneof")
                        if kind in ("ping", "pong"):
                            if self.metrics:
                                self.metrics.on_ping(len(frame))
                            continue
                        if not self.connected.is_set():
                            self.connected.set()
                            if self.metrics:
                                self.metrics.on_connected()
                        backoff = 1.0
                        if self.metrics:
                            self.metrics.on_message(len(frame), list(update.filters))
                        if kind == "account":
                            acc = update.account.account
                            await handler(AccountUpdate(b58encode(acc.pubkey), b58encode(acc.owner), bytes(acc.data),
                                                        update.account.slot, acc.write_version, source="mirage",
                                                        filter_name=(update.filters[0] if update.filters else "")))
                        elif kind == "slot":
                            await handler(SlotUpdate(update.slot.slot, geyser_pb2.SlotStatus.Name(update.slot.status).lower(), source="mirage"))
                log.warning("Mirage stream ended — reconnecting")
            except websockets.ConnectionClosed as exc:
                reason = CLOSE_CODES.get(exc.code, self._redact(exc.reason or ""))
                if self.metrics:
                    self.metrics.on_error(f"ws-close-{exc.code}")
                if exc.code == 4002:
                    raise RuntimeError("Mirage closed: bandwidth and balance exhausted (top up on solami.dev)") from None
                log.warning("Mirage closed (%s %s) — reconnecting in ~%.0fs", exc.code, reason, backoff)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — InvalidStatus / InvalidURI / OSError: redact and retry
                if self.metrics:
                    self.metrics.on_error(self._redact(f"ws {type(exc).__name__}"))
                log.warning("Mirage connection error %s: %s — reconnecting in ~%.0fs",
                            type(exc).__name__, self._redact(str(exc)), backoff)
            finally:
                self.connected.clear()

    async def close(self) -> None:
        self._stop.set()


def mirage_filter(wallet_positions: list[str], custodies: list[str], oracles: list[str], commitment: str) -> dict[str, Any]:
    """Suggested body for POST https://api.solami.dev/mirage/create (label + filter) when you create the
    saved subscription by hand.

    Known limitation (not implemented): a Mirage saved filter lists explicit addresses, so a position
    PDA the wallet opens on a *new* market is not in the stream until the subscription is updated
    (`POST /mirage/update`). Prop Guard does not do that yet — with TRANSPORT=mirage, watch the
    `unpriced`/`position_open` alerts or restart the guard after opening a new market.
    """
    return {"accounts": wallet_positions + custodies + oracles, "slots": True, "commitment": commitment}


__all__ = ["MirageTransport", "mirage_filter", "CLOSE_CODES"]
