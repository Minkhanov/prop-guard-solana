"""Prop-firm rules evaluated on every snapshot.

Rules (mirroring what a funded-account trader lives with on a CEX prop desk):
  R1 daily loss   — realized + unrealized net PnL since the day anchor vs a % of the base
  R2 liquidation  — distance from mark to liquidation price per position
  R3 exposure     — total position size / base and per-position leverage
  R4 data health  — stale oracle / silent stream (a guard that is blind must say so)
  E  events       — position opened / increased / reduced / closed (informational)

Day anchor — honest version. The trading day is the UTC day. The anchor (base equity and
per-position PnL baseline) is taken:
  * at 00:00 UTC when the guard is running through the rollover        → anchor_source = "utc_midnight"
  * at the first snapshot after a (re)start on a new day                → anchor_source = "first_observation"
    (the loss/profit a position made *before* the guard started that day is not visible on-chain
     without historical prices, so it is NOT counted — the panel and every daily alert say so);
  * restored from the state file when restarted on the same day        → keeps the stored source.
Positions opened today (on-chain `openTime` ≥ 00:00 UTC) are always baselined at zero plus the
estimated open fee, whatever the anchor source — their whole PnL is today's.

Daily PnL accounting (per open position: net_pnl = pnl − accrued borrow fee − close fee):
  daily = realized_today + Σ (net_pnl_now − baseline)
  * increase          : the program settles the accrued borrow fee (it leaves net_pnl) and charges an
                        open fee from the delivered collateral (never visible in net_pnl) → both are
                        added to the baseline so they stay counted as today's cost;
  * partial close (f) : realized += on-chain Δ realisedPnlUsd (exact, program-side) − f × baseline;
                        baseline ← (1 − f) × (baseline + settled borrow fee);
  * full close        : the program zeroes realisedPnlUsd, so the last mark is used (≈, labelled);
  * collateral add/remove without a size change is not PnL; a borrow-fee settlement that comes
    with it is charged.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import Settings
from .state import AccountSnapshot

log = logging.getLogger("propguard.rules")

LEVELS = ("info", "warn", "crit")
ANCHOR_SOURCES = ("utc_midnight", "first_observation", "account_size")


@dataclass(frozen=True)
class Alert:
    level: str
    code: str
    key: str          # dedup key (code + subject)
    text: str
    ts: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {"level": self.level, "code": self.code, "key": self.key, "text": self.text, "ts": self.ts}


def utc_day(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def utc_day_start(ts: float) -> float:
    d = datetime.fromtimestamp(ts, tz=timezone.utc)
    return d.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def utc_hhmm(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M")


@dataclass
class DailyBook:
    day: str = ""
    anchor_equity_usd: float = 0.0
    anchor_ts: float = 0.0
    anchor_source: str = ""                                          # see ANCHOR_SOURCES
    anchor_net_pnl: dict[str, float] = field(default_factory=dict)   # position -> net pnl baseline
    anchor_realised: dict[str, float] = field(default_factory=dict)  # position -> on-chain realisedPnlUsd at baseline
    realized_today_usd: float = 0.0
    fees_today_usd: float = 0.0                                      # open fees + settled borrow fees charged today
    peak_daily_pnl: float = 0.0
    trough_daily_pnl: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DailyBook":
        b = cls()
        for k, v in d.items():
            if hasattr(b, k):
                setattr(b, k, v)
        return b

    def anchor_label(self) -> str:
        """Short human label used by the panel and alerts."""
        if not self.day:
            return "no anchor yet"
        hhmm = utc_hhmm(self.anchor_ts) if self.anchor_ts else "?"
        if self.anchor_source == "utc_midnight":
            return "00:00 UTC (observed live)"
        if self.anchor_source == "first_observation":
            return f"first observation {hhmm} UTC (guard started mid-day; earlier PnL of positions opened before today is not counted)"
        return f"{self.anchor_source or 'unknown'} {hhmm} UTC"


class RuleEngine:
    def __init__(self, settings: Settings, state_file: str | None = None):
        self.s = settings
        self.state_path = Path(state_file or settings.state_file)
        self.book = DailyBook()
        self.last_positions: dict[str, Any] = {}   # pubkey -> PositionView (previous snapshot)
        self.last_alerts: list[Alert] = []
        self.initialized = False                   # first snapshot = existing positions, not "opens"
        self._load()

    # ---- persistence -----------------------------------------------------------
    def _load(self) -> None:
        if self.state_path.is_file():
            try:
                raw = json.loads(self.state_path.read_text(encoding="utf-8"))
                if raw.get("wallet") == self.s.wallet:
                    self.book = DailyBook.from_dict(raw.get("book", {}))
                    if self.book.day and not self.book.anchor_source:   # state written by an older build
                        self.book.anchor_source = "first_observation"
                    log.info("restored daily book for %s (%s, anchor %s)", self.s.wallet, self.book.day, self.book.anchor_label())
            except (ValueError, OSError) as exc:
                log.warning("cannot read state file %s: %s", self.state_path, exc)

    def _save(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"wallet": self.s.wallet, "book": self.book.to_dict(), "saved_at": time.time()}, indent=1),
                           encoding="utf-8")
            tmp.replace(self.state_path)
        except OSError as exc:
            log.warning("cannot write state file %s: %s", self.state_path, exc)

    # ---- base for % rules --------------------------------------------------------
    def base_usd(self) -> float:
        if self.s.account_size_usd > 0:
            return self.s.account_size_usd
        return self.book.anchor_equity_usd

    def base_label(self) -> str:
        if self.s.account_size_usd > 0:
            return "ACCOUNT_SIZE_USD (config)"
        return f"equity at anchor — {self.book.anchor_label()}"

    def daily_pnl(self, snap: AccountSnapshot) -> float:
        unreal = sum(v.net_pnl_usd - self.book.anchor_net_pnl.get(v.pubkey, 0.0) for v in snap.positions)
        return self.book.realized_today_usd + unreal

    # ---- anchoring -------------------------------------------------------------------
    def _baseline_for(self, v: Any, day_start: float) -> float:
        """PnL baseline for a position at anchor time."""
        if v.open_time and v.open_time >= day_start:
            fee = v.open_fee_estimate_usd(v.size_usd)     # opened today: everything it did is today's, incl. the open fee
            self.book.fees_today_usd += fee
            return fee
        return v.net_pnl_usd

    def _anchor_day(self, snap: AccountSnapshot, today: str, *, live_rollover: bool) -> Alert:
        day_start = utc_day_start(snap.ts)
        source = "utc_midnight" if live_rollover else "first_observation"
        self.book = DailyBook(day=today, anchor_equity_usd=snap.equity_usd, anchor_ts=snap.ts, anchor_source=source)
        self.book.anchor_net_pnl = {v.pubkey: self._baseline_for(v, day_start) for v in snap.positions}
        self.book.anchor_realised = {v.pubkey: v.realised_pnl_usd for v in snap.positions}
        opened_today = sum(1 for v in snap.positions if v.open_time and v.open_time >= day_start)
        note = (" — positions opened before today are baselined at their current PnL (the part made earlier "
                "today is not counted)") if source == "first_observation" and len(snap.positions) > opened_today else ""
        return Alert("info", "day_anchor", f"day_anchor:{today}",
                     f"Trading day {today} UTC anchored at {self.book.anchor_label().split(' (')[0]}: equity ${snap.equity_usd:,.2f}, "
                     f"{len(snap.positions)} open position(s), {opened_today} opened today{note}")

    # ---- evaluation ----------------------------------------------------------------
    def evaluate(self, snap: AccountSnapshot, health: dict[str, Any] | None = None) -> list[Alert]:
        alerts: list[Alert] = []
        today = utc_day(snap.ts)
        day_start = utc_day_start(snap.ts)
        if self.book.day != today:
            # a rollover observed while running (previous book was yesterday) is a true 00:00 UTC anchor
            live_rollover = self.initialized and bool(self.book.day)
            alerts.append(self._anchor_day(snap, today, live_rollover=live_rollover))
        cur = {v.pubkey: v for v in snap.positions}
        # events: open / close / size change / fee settlements
        for pk, v in cur.items():
            prev = self.last_positions.get(pk)
            if prev is None:
                if self.initialized:
                    fee = v.open_fee_estimate_usd(v.size_usd)
                    self.book.anchor_net_pnl[pk] = fee            # today's open: charge its open fee
                    self.book.anchor_realised[pk] = v.realised_pnl_usd
                    self.book.fees_today_usd += fee
                    alerts.append(Alert("info", "position_open", f"open:{pk}:{int(snap.ts)}",
                                        f"OPEN {v.market} {v.side.upper()} ${v.size_usd:,.0f} @ {v.entry_price:,.2f} "
                                        f"({v.leverage:.1f}x), liq {v.liq_price:,.2f}, open fee ≈ ${fee:,.2f}"))
                elif pk not in self.book.anchor_net_pnl:          # restored book, position unknown to it
                    self.book.anchor_net_pnl[pk] = self._baseline_for(v, day_start)
                    self.book.anchor_realised[pk] = v.realised_pnl_usd
                continue
            settled_fee = prev.borrow_fee_usd if v.cumulative_interest_snapshot != prev.cumulative_interest_snapshot else 0.0
            size_delta = v.size_usd - prev.size_usd
            if size_delta > 0.5:
                open_fee = v.open_fee_estimate_usd(size_delta)
                self.book.anchor_net_pnl[pk] = self.book.anchor_net_pnl.get(pk, 0.0) + settled_fee + open_fee
                self.book.fees_today_usd += settled_fee + open_fee
                alerts.append(Alert("info", "position_change", f"chg:{pk}:{int(snap.ts)}",
                                    f"INCREASE {v.market} {v.side.upper()} ${prev.size_usd:,.0f} -> ${v.size_usd:,.0f} "
                                    f"(fees charged today: open ≈ ${open_fee:,.2f}, borrow settled ${settled_fee:,.2f}), liq {v.liq_price:,.2f}"))
            elif size_delta < -0.5:
                frac = 1 - v.size_usd / prev.size_usd
                baseline = self.book.anchor_net_pnl.get(pk, 0.0)
                onchain_delta = v.realised_pnl_usd - prev.realised_pnl_usd
                if abs(onchain_delta) > 1e-9:
                    realized, how = onchain_delta - frac * baseline, "on-chain realisedPnlUsd"
                else:                                             # program did not book it: fall back to the last mark
                    realized, how = prev.net_pnl_usd * frac - frac * baseline, "last mark"
                self.book.realized_today_usd += realized
                self.book.anchor_net_pnl[pk] = (1 - frac) * (baseline + settled_fee)
                self.book.anchor_realised[pk] = v.realised_pnl_usd
                self.book.fees_today_usd += settled_fee * (1 - frac)
                alerts.append(Alert("info", "position_change", f"chg:{pk}:{int(snap.ts)}",
                                    f"REDUCE {v.market} {v.side.upper()} ${prev.size_usd:,.0f} -> ${v.size_usd:,.0f} "
                                    f"— realized today {realized:+,.2f} USD ({how})"))
            elif settled_fee:                                     # collateral add/remove: fee settled, size unchanged
                self.book.anchor_net_pnl[pk] = self.book.anchor_net_pnl.get(pk, 0.0) + settled_fee
                self.book.fees_today_usd += settled_fee
                if abs(v.collateral_usd - prev.collateral_usd) > 0.5:
                    alerts.append(Alert("info", "collateral_change", f"coll:{pk}:{int(snap.ts)}",
                                        f"COLLATERAL {v.market} {v.side.upper()} ${prev.collateral_usd:,.0f} -> ${v.collateral_usd:,.0f} "
                                        f"(not PnL; borrow fee settled ${settled_fee:,.2f}), liq {v.liq_price:,.2f}"))
        for pk, prev in self.last_positions.items():
            if pk not in cur:
                realized = prev.net_pnl_usd - self.book.anchor_net_pnl.pop(pk, 0.0)
                self.book.anchor_realised.pop(pk, None)
                self.book.realized_today_usd += realized
                alerts.append(Alert("info", "position_close", f"close:{pk}:{int(snap.ts)}",
                                    f"CLOSE {prev.market} {prev.side.upper()} ${prev.size_usd:,.0f} — realized today ≈ {realized:+,.2f} USD "
                                    f"(last mark {prev.mark_price:,.2f}; the program zeroes realisedPnlUsd on full close)"))
        self.last_positions = cur
        self.initialized = True

        # R1 daily loss
        base = self.base_usd()
        dpnl = self.daily_pnl(snap)
        self.book.peak_daily_pnl = max(self.book.peak_daily_pnl, dpnl)
        self.book.trough_daily_pnl = min(self.book.trough_daily_pnl, dpnl)
        anchor = self.book.anchor_label().split(" (")[0]
        if base > 0:
            limit = base * self.s.daily_loss_limit_pct / 100
            used = -dpnl / limit if dpnl < 0 else 0.0
            if used >= 1.0:
                alerts.append(Alert("crit", "daily_loss", "daily_loss:crit",
                                    f"DAILY LOSS LIMIT HIT: {dpnl:+,.2f} USD ({-dpnl / base * 100:.2f}% of ${base:,.0f}) — stop trading today "
                                    f"[anchor: {anchor}]"))
            elif used >= 0.8:
                alerts.append(Alert("warn", "daily_loss", "daily_loss:80",
                                    f"Daily loss at {used * 100:.0f}% of limit: {dpnl:+,.2f} USD of -{limit:,.0f} [anchor: {anchor}]"))
            elif used >= 0.5:
                alerts.append(Alert("info", "daily_loss", "daily_loss:50",
                                    f"Daily loss at {used * 100:.0f}% of limit: {dpnl:+,.2f} USD of -{limit:,.0f} [anchor: {anchor}]"))
        # R2 liquidation distance
        for v in snap.positions:
            d = v.liq_distance_pct
            if d <= self.s.liq_distance_crit_pct:
                alerts.append(Alert("crit", "liq_distance", f"liq:{v.pubkey}:crit",
                                    f"LIQUIDATION {d:.2f}% away: {v.market} {v.side.upper()} ${v.size_usd:,.0f} "
                                    f"mark {v.mark_price:,.2f} liq {v.liq_price:,.2f}"))
            elif d <= self.s.liq_distance_warn_pct:
                alerts.append(Alert("warn", "liq_distance", f"liq:{v.pubkey}:warn",
                                    f"Liquidation {d:.2f}% away: {v.market} {v.side.upper()} ${v.size_usd:,.0f} "
                                    f"mark {v.mark_price:,.2f} liq {v.liq_price:,.2f}"))
        # R3 exposure
        if base > 0 and snap.exposure_usd / base > self.s.max_exposure_x:
            alerts.append(Alert("warn", "exposure", "exposure:total",
                                f"Exposure {snap.exposure_usd / base:.1f}x of base ${base:,.0f} exceeds {self.s.max_exposure_x:.0f}x"))
        # R4 data health
        if snap.positions and snap.oracle_age_sec > self.s.oracle_stale_sec:
            alerts.append(Alert("warn", "oracle_stale", "health:oracle",
                                f"Oracle price is {snap.oracle_age_sec:.0f}s old — risk numbers may be stale"))
        if health:
            silence = health.get("silence_sec", 0)
            if health.get("messages_total", 0) and silence > self.s.stream_stale_sec:
                path = health.get("active_path") or health.get("transport")
                extra = " — RPC fallback is feeding the guard" if health.get("fallback_active") else " — check connection"
                alerts.append(Alert("warn", "stream_stale", "health:stream",
                                    f"No data from the {health.get('transport')} stream for {silence:.0f}s (active path: {path}){extra}"))
            lag = health.get("slot_lag")
            if lag is not None and lag > 50:
                alerts.append(Alert("warn", "slot_lag", "health:lag", f"Stream is {lag} slots behind RPC head"))
        self.last_alerts = alerts
        self._save()
        return alerts

    def summary(self, snap: AccountSnapshot) -> dict[str, Any]:
        base = self.base_usd()
        dpnl = self.daily_pnl(snap)
        limit = base * self.s.daily_loss_limit_pct / 100 if base > 0 else 0.0
        return {
            "day": self.book.day, "base_usd": round(base, 2), "base_source": self.base_label(),
            "anchor_source": self.book.anchor_source, "anchor_ts": self.book.anchor_ts,
            "anchor_label": self.book.anchor_label(), "anchor_equity_usd": round(self.book.anchor_equity_usd, 2),
            "daily_pnl_usd": round(dpnl, 2),
            "daily_loss_limit_usd": round(limit, 2),
            "daily_limit_used_pct": round(max(0.0, -dpnl) / limit * 100, 1) if limit else 0.0,
            "realized_today_usd": round(self.book.realized_today_usd, 2),
            "fees_today_usd": round(self.book.fees_today_usd, 2),
            "peak_daily_pnl": round(self.book.peak_daily_pnl, 2), "trough_daily_pnl": round(self.book.trough_daily_pnl, 2),
            "exposure_x": round(snap.exposure_usd / base, 2) if base else 0.0,
            "max_exposure_x": self.s.max_exposure_x,
            "min_liq_distance_pct": round(min((v.liq_distance_pct for v in snap.positions), default=0.0), 2),
        }
