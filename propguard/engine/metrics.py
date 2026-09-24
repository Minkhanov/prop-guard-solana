"""Stream-health metrics: what we track and how healthy the data path is.

Vocabulary (also used by the panel and /api/health):
  message      = a frame that carried data (account or slot update). Pings/pongs are
                 counted separately (`pings_total`) and never reset the silence timer —
                 an empty subscription that only exchanges keep-alives is *not* healthy.
  silence_sec  = seconds since the last data message from the primary transport
  update_age   = seconds since the engine last applied *any* update (incl. RPC fallback)
  slot_lag     = RPC head slot − last stream slot, clamped at 0; `slot_lag_raw` keeps the
                 sign (a negative value means the processed-commitment stream is ahead of
                 the RPC head polled a few seconds ago — that is normal)
  active_path  = which wire feeds the engine right now: the configured transport or
                 "rpc-fallback" while the stream is silent
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class StreamMetrics:
    transport: str = "rpc"
    started_at: float = field(default_factory=time.time)
    messages_total: int = 0
    bytes_total: int = 0
    pings_total: int = 0
    by_filter: dict[str, int] = field(default_factory=dict)
    account_updates: int = 0
    slot_updates: int = 0
    reconnects: int = 0
    connects: int = 0
    errors: int = 0
    last_error: str = ""
    last_error_at: float = 0.0
    last_message_at: float = 0.0
    last_update_at: float = 0.0        # any update applied to the engine (stream or fallback)
    last_stream_slot: int = 0
    last_rpc_slot: int = 0
    last_rpc_slot_at: float = 0.0
    oracle_age_sec: float = 0.0
    fallback_active: bool = False
    fallback_count: int = 0
    fallback_since: float = 0.0
    replays: int = 0                   # reconnects that used from_slot replay
    rebootstraps: int = 0              # reconnects that had to re-bootstrap over RPC
    _window: deque = field(default_factory=lambda: deque(maxlen=5000), repr=False)

    # ---- ingestion ---------------------------------------------------------------
    def on_message(self, nbytes: int, filters: list[str] | None = None) -> None:
        """A data frame from the primary transport (never a ping)."""
        now = time.time()
        self.messages_total += 1
        self.bytes_total += nbytes
        self.last_message_at = now
        self._window.append(now)
        for f in filters or []:
            self.by_filter[f] = self.by_filter.get(f, 0) + 1

    def on_ping(self, nbytes: int = 0) -> None:
        """Keep-alive traffic: counted, but it is not data and does not end a silence."""
        self.pings_total += 1
        self.bytes_total += nbytes

    def on_account(self, slot: int) -> None:
        self.account_updates += 1
        self.last_stream_slot = max(self.last_stream_slot, slot)
        self.last_update_at = time.time()

    def on_slot(self, slot: int) -> None:
        self.slot_updates += 1
        self.last_stream_slot = max(self.last_stream_slot, slot)
        self.last_update_at = time.time()

    def on_rpc_slot(self, slot: int) -> None:
        self.last_rpc_slot = slot
        self.last_rpc_slot_at = time.time()

    def on_stream_started(self) -> None:
        """Start the silence clock when the transport starts — a stream that never delivers a single
        frame must look exactly as silent as one that died (otherwise fallback/alerts never fire)."""
        if not self.last_message_at:
            self.last_message_at = time.time()

    def on_connected(self) -> None:
        self.connects += 1

    def on_reconnect(self, *, replayed: bool | None = None) -> None:
        self.reconnects += 1
        if replayed is True:
            self.replays += 1
        elif replayed is False:
            self.rebootstraps += 1

    def on_error(self, what: str) -> None:
        self.errors += 1
        self.last_error = what
        self.last_error_at = time.time()

    def on_fallback(self, active: bool) -> None:
        if active and not self.fallback_active:
            self.fallback_count += 1
            self.fallback_since = time.time()
        if not active:
            self.fallback_since = 0.0
        self.fallback_active = active

    # ---- derived ---------------------------------------------------------------
    def rate_per_sec(self, window_s: float = 10.0) -> float:
        cutoff = time.time() - window_s
        n = sum(1 for t in self._window if t >= cutoff)
        return n / window_s

    def silence_sec(self) -> float:
        return time.time() - self.last_message_at if self.last_message_at else 0.0

    def update_age_sec(self) -> float:
        return time.time() - self.last_update_at if self.last_update_at else 0.0

    def slot_lag_raw(self) -> int | None:
        """RPC head − stream slot, signed — None until both are known."""
        if self.last_stream_slot and self.last_rpc_slot:
            return self.last_rpc_slot - self.last_stream_slot
        return None

    def slot_lag(self) -> int | None:
        raw = self.slot_lag_raw()
        return None if raw is None else max(0, raw)

    @property
    def active_path(self) -> str:
        return "rpc-fallback" if self.fallback_active else self.transport

    def to_dict(self) -> dict[str, Any]:
        return {
            "transport": self.transport,
            "active_path": self.active_path,
            "uptime_sec": round(time.time() - self.started_at, 1),
            "messages_total": self.messages_total,
            "pings_total": self.pings_total,
            "bytes_total": self.bytes_total,
            "msg_per_sec_10s": round(self.rate_per_sec(), 2),
            "by_filter": dict(self.by_filter),
            "account_updates": self.account_updates,
            "slot_updates": self.slot_updates,
            "connects": self.connects,
            "reconnects": self.reconnects,
            "replays": self.replays,
            "rebootstraps": self.rebootstraps,
            "errors": self.errors,
            "last_error": self.last_error,
            "silence_sec": round(self.silence_sec(), 1),
            "update_age_sec": round(self.update_age_sec(), 1),
            "last_stream_slot": self.last_stream_slot,
            "last_rpc_slot": self.last_rpc_slot,
            "slot_lag": self.slot_lag(),
            "slot_lag_raw": self.slot_lag_raw(),
            "oracle_age_sec": round(self.oracle_age_sec, 1),
            "fallback_active": self.fallback_active,
            "fallback_count": self.fallback_count,
            "fallback_for_sec": round(time.time() - self.fallback_since, 1) if self.fallback_active and self.fallback_since else 0.0,
        }
