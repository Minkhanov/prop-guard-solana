"""The guard loop: transport -> state -> rules -> alerts, plus panel fan-out.

Resilience (streaming transports):
  * the transport reconnects on its own (backoff, `from_slot` replay, RPC re-bootstrap when
    the gap is larger than the replay window);
  * a supervisor watches the stream's data silence: after FALLBACK_SILENCE_SEC without data,
    an RPC polling transport takes over (`active_path = rpc-fallback`) and is stopped as soon
    as the stream delivers again — the guard never goes blind quietly;
  * an optional health journal (JSON lines) records the metrics every N seconds.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from .alerts import AlertContext, AlertRouter, ConsoleSink, TelegramSink
from .config import Settings
from .constants import CUSTODIES, DOVES_ORACLES_FALLBACK
from .engine.metrics import StreamMetrics
from .engine.rules import Alert, RuleEngine
from .engine.state import GuardState
from .transports.base import AccountUpdate, SlotUpdate, Transport
from .transports.rpc import RpcClient, RpcPollingTransport, fetch_wallet_positions

log = logging.getLogger("propguard.guard")


class Guard:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.state = GuardState(settings.wallet)
        self.metrics = StreamMetrics(transport=settings.transport)
        self.rules = RuleEngine(settings)
        self.alert_ctx = AlertContext(wallet=settings.wallet, transport=settings.transport)
        sinks: list[Any] = [ConsoleSink()]
        if settings.telegram_enabled or settings.alerts_dry_run:
            sinks.append(TelegramSink(settings.telegram_bot_token, settings.telegram_chat_id, ctx=self.alert_ctx,
                                      min_level=settings.telegram_min_level, dry_run=settings.telegram_dry_run))
        self.router = AlertRouter(sinks, cooldown_min=settings.alert_cooldown_min)
        self.rpc = RpcClient(settings.rpc_url, settings.solami_api_key if settings.rpc_is_solami else "",
                             rps=settings.rpc_rps, is_solami=settings.rpc_is_solami)
        self._subscribers: list[asyncio.Queue] = []
        self._dirty = asyncio.Event()
        self._last_payload: dict[str, Any] = {}
        self.transport: Transport | None = None
        self.fallback: RpcPollingTransport | None = None
        self._fallback_task: asyncio.Task | None = None
        self.ready = False   # rules run only after the bootstrap snapshot is complete
        self.started_at = time.time()

    # ---- panel fan-out -----------------------------------------------------------
    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        if q in self._subscribers:
            self._subscribers.remove(q)

    def payload(self) -> dict[str, Any]:
        return self._last_payload or self._build_payload(self.state.snapshot(), [])

    def _build_payload(self, snap, alerts) -> dict[str, Any]:
        self.metrics.oracle_age_sec = snap.oracle_age_sec
        return {
            "snapshot": snap.to_dict(),
            "rules": self.rules.summary(snap),
            "health": self.metrics.to_dict(),
            "alerts": [a.to_dict() for a in self.router.history[-30:]],
            "alerting": self.router.stats(),
            "thresholds": {"liq_warn": self.settings.liq_distance_warn_pct, "liq_crit": self.settings.liq_distance_crit_pct,
                           "daily_loss_limit_pct": self.settings.daily_loss_limit_pct, "max_exposure_x": self.settings.max_exposure_x,
                           "stream_stale_sec": self.settings.stream_stale_sec, "oracle_stale_sec": self.settings.oracle_stale_sec,
                           "fallback_silence_sec": self.settings.fallback_silence_sec},
            "transport_filters": self._filter_summary(),
        }

    def _filter_summary(self) -> dict[str, Any]:
        return {"positions_owner": self.settings.wallet, "custodies": list(CUSTODIES.values()),
                "oracles": self.state.oracle_pubkeys() or list(DOVES_ORACLES_FALLBACK.values())}

    # ---- update handling -----------------------------------------------------------
    async def on_update(self, upd: AccountUpdate | SlotUpdate) -> None:
        if isinstance(upd, AccountUpdate):
            if upd.source in ("rpc", "rpc-fallback"):
                # polling delivers a batch per poll: count the batch as data for the *active* path
                if upd.source == "rpc":
                    self.metrics.on_message(len(upd.data), ["rpc"])
                self.metrics.account_updates += 1
                self.metrics.last_update_at = time.time()
            elif upd.source == "bootstrap":
                self.metrics.last_update_at = time.time()
            else:
                self.metrics.on_account(upd.slot)
        else:
            if upd.source in ("rpc", "rpc-fallback", "bootstrap"):
                self.metrics.on_rpc_slot(upd.slot)
                if upd.source == "rpc":
                    self.metrics.on_slot(upd.slot)
                self.metrics.last_update_at = time.time()
            else:
                self.metrics.on_slot(upd.slot)
        if self.state.apply(upd) in ("position", "oracle", "custody") and self.ready:
            self._dirty.set()

    async def _tick_loop(self, min_interval: float = 0.25) -> None:
        while True:
            await self._dirty.wait()
            self._dirty.clear()
            snap = self.state.snapshot()
            alerts = self.rules.evaluate(snap, self.metrics.to_dict())
            await self.router.dispatch(alerts)
            self._publish(snap, alerts)
            await asyncio.sleep(min_interval)

    def _publish(self, snap, alerts) -> None:
        self._last_payload = self._build_payload(snap, alerts)
        for q in list(self._subscribers):
            if not q.full():
                q.put_nowait(self._last_payload)

    async def _rpc_head_loop(self, every: float = 5.0) -> None:
        """Reference head slot for lag metrics + a periodic health tick (streaming transports)."""
        while True:
            try:
                self.metrics.on_rpc_slot(await self.rpc.get_slot("processed"))
            except Exception as exc:  # noqa: BLE001
                self.metrics.on_error(f"rpc-head: {exc}")
            self._dirty.set()
            await asyncio.sleep(every)

    # ---- stream fallback supervisor ---------------------------------------------------
    async def _fallback_loop(self, check_every: float = 1.0) -> None:
        threshold = self.settings.fallback_silence_sec
        if threshold <= 0:
            return
        while True:
            await asyncio.sleep(check_every)
            silence = self.metrics.silence_sec()
            if not self.metrics.fallback_active and self.metrics.last_message_at and silence > threshold:
                await self._start_fallback(silence)
            elif self.metrics.fallback_active and silence < min(2.0, threshold):
                await self._stop_fallback()

    async def _start_fallback(self, silence: float) -> None:
        custodies = list(CUSTODIES.values())
        oracles = self.state.oracle_pubkeys() or list(DOVES_ORACLES_FALLBACK.values())
        self.fallback = RpcPollingTransport(self.rpc, self.settings.wallet, interval_ms=self.settings.fallback_poll_interval_ms,
                                            commitment=self.settings.commitment, extra_accounts=custodies + oracles,
                                            source="rpc-fallback")
        self.metrics.on_fallback(True)
        self._fallback_task = asyncio.create_task(self._run_fallback(), name="rpc-fallback")
        await self.router.dispatch([Alert("warn", "fallback_on", f"fallback:on:{self.metrics.fallback_count}",
                                          f"{self.settings.transport} stream silent for {silence:.0f}s — RPC polling took over "
                                          f"(every {self.settings.fallback_poll_interval_ms} ms) until the stream is back")])
        self._dirty.set()

    async def _run_fallback(self) -> None:
        try:
            await self.fallback.run(self.on_update)  # type: ignore[union-attr]
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — fallback must not kill the guard either
            self.metrics.on_error(f"fallback: {exc}")
            log.warning("RPC fallback failed: %s", exc)

    async def _stop_fallback(self) -> None:
        since = self.metrics.fallback_since
        if self.fallback:
            await self.fallback.close()
        if self._fallback_task:
            self._fallback_task.cancel()
            try:
                await self._fallback_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        polls = self.fallback.polls if self.fallback else 0
        self.fallback, self._fallback_task = None, None
        self.metrics.on_fallback(False)
        await self.router.dispatch([Alert("info", "fallback_off", f"fallback:off:{self.metrics.fallback_count}",
                                          f"{self.settings.transport} stream is back after {time.time() - since:.0f}s "
                                          f"({polls} RPC polls covered the gap) — fallback stopped")])
        self._dirty.set()

    # ---- health journal ---------------------------------------------------------------------
    def health_record(self) -> dict[str, Any]:
        snap = self.state.snapshot()
        summary = self.rules.summary(snap)
        return {"ts": round(time.time(), 3), **self.metrics.to_dict(),
                "positions": len(snap.positions), "prices": snap.to_dict()["prices"],
                "min_liq_distance_pct": summary["min_liq_distance_pct"], "daily_pnl_usd": summary["daily_pnl_usd"],
                "rpc_calls": self.rpc.stats["calls"], "rpc_errors": self.rpc.stats["errors"],
                "rpc_latency_ms": round(self.rpc.stats["last_latency_ms"], 1), "alerting": self.router.stats()}

    async def _health_log_loop(self, path: str, every: float) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        log.info("health journal: %s every %.0fs", p, every)
        while True:
            try:
                with p.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(self.health_record(), separators=(",", ":")) + "\n")
            except OSError as exc:
                log.warning("health journal write failed: %s", exc)
            await asyncio.sleep(every)

    # ---- wiring ---------------------------------------------------------------------
    async def bootstrap_accounts(self) -> None:
        """Positions + oracles over RPC (streaming transports): initial snapshot and re-bootstrap
        after a gap that the gRPC replay window cannot cover."""
        s = self.settings
        oracles = self.state.oracle_pubkeys() or list(DOVES_ORACLES_FALLBACK.values())
        _, positions = await fetch_wallet_positions(self.rpc, s.wallet, "confirmed")
        for p in positions:
            await self.on_update(AccountUpdate(p.pubkey, p.owner, p.data, p.slot, source="bootstrap"))
        slot, oracle_updates = await self.rpc.get_multiple_accounts(oracles, "confirmed")
        for u in oracle_updates:
            if u is not None:
                await self.on_update(AccountUpdate(u.pubkey, u.owner, u.data, u.slot, source="bootstrap"))
        await self.on_update(SlotUpdate(slot, "confirmed", source="bootstrap"))
        log.info("bootstrap: %d position accounts, %d oracles, slot %d", len(positions), len(oracles), slot)

    async def build_transport(self) -> Transport:
        s = self.settings
        custodies = list(CUSTODIES.values())
        # bootstrap custodies first so we know the live oracle addresses
        _, cust_updates = await self.rpc.get_multiple_accounts(custodies, "confirmed")
        for u in cust_updates:
            if u is not None:
                await self.on_update(AccountUpdate(u.pubkey, u.owner, u.data, u.slot, source="bootstrap"))
        oracles = self.state.oracle_pubkeys() or list(DOVES_ORACLES_FALLBACK.values())
        if s.transport == "rpc":
            t = RpcPollingTransport(self.rpc, s.wallet, interval_ms=s.poll_interval_ms, commitment=s.commitment,
                                    extra_accounts=custodies + oracles)
            await t.bootstrap(self.on_update)   # complete snapshot before any rule fires
            return t
        # streaming transports: bootstrap positions + oracles over RPC, then stream
        await self.bootstrap_accounts()
        if s.transport == "grpc":
            from .transports.grpc_yellowstone import GrpcTransport
            return GrpcTransport(s.grpc_endpoint, s.grpc_key, s.wallet, custodies, oracles, commitment=s.commitment,
                                 metrics=self.metrics, head_slot=lambda: self.rpc.get_slot("processed"),
                                 rebootstrap=self.bootstrap_accounts, tls_server_name=s.solami_grpc_tls_server_name)
        from .transports.mirage import MirageTransport
        return MirageTransport(s.mirage_url, s.solami_api_key, metrics=self.metrics)

    async def run(self, *, with_panel: bool = False) -> None:
        self.transport = await self.build_transport()
        self.metrics.transport = self.transport.name
        self.alert_ctx.transport = self.transport.name
        self.ready = True
        self._dirty.set()   # first evaluation on the complete bootstrap snapshot (anchors the day)
        tasks = [asyncio.create_task(self.transport.run(self.on_update), name="transport"),
                 asyncio.create_task(self._tick_loop(), name="tick"),
                 asyncio.create_task(self._rpc_head_loop(), name="rpc-head")]
        if self.transport.name != "rpc":
            tasks.append(asyncio.create_task(self._fallback_loop(), name="fallback-supervisor"))
        if self.settings.health_log_file:
            tasks.append(asyncio.create_task(self._health_log_loop(self.settings.health_log_file, self.settings.health_log_every_sec),
                                             name="health-log"))
        if with_panel:
            from .panel.server import serve
            tasks.append(asyncio.create_task(serve(self, self.settings.panel_host, self.settings.panel_port), name="panel"))
            log.info("panel: http://%s:%d", self.settings.panel_host, self.settings.panel_port)
        log.info("guard started: wallet=%s transport=%s telegram=%s", self.settings.wallet, self.transport.name,
                 self.router.stats().get("telegram", {}).get("mode", "off"))
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in done:
                if t.exception():
                    raise t.exception()  # type: ignore[misc]
        finally:
            for t in tasks:
                t.cancel()
            if self._fallback_task:
                self._fallback_task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.transport.close()
            await self.router.close()
            await self.rpc.close()

    async def once(self) -> dict[str, Any]:
        """One-shot snapshot (bootstrap only) — used by `propguard positions`."""
        t = await self.build_transport()
        if isinstance(t, RpcPollingTransport) and not t.bootstrapped:
            await t.bootstrap(self.on_update)
        snap = self.state.snapshot()
        await self.rpc.close()
        return {"snapshot": snap.to_dict(), "rules": self.rules.summary(snap), "decode_errors": self.state.decode_errors,
                "ts": time.time()}
