"""Account cache + derived account snapshot.

Feeds on `AccountUpdate`s from any transport. Keeps the latest bytes per account,
decodes lazily, and builds `PositionView`s with live mark prices from the Doves
oracles referenced by the custodies.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from ..constants import CUSTODY_SYMBOL, DOVES_PROGRAM, JUPITER_PERPS_PROGRAM
from ..decode.jupiter import (DovesPrice, Position, decode_custody, decode_doves_price_feed,
                              decode_position, is_position_account)
from ..math.jupiter_math import PositionView, build_view
from ..transports.base import AccountUpdate, SlotUpdate

log = logging.getLogger("propguard.state")


@dataclass
class AccountSnapshot:
    wallet: str
    ts: float
    slot: int
    positions: list[PositionView]
    equity_usd: float          # sum of net values of open positions (isolated margin)
    collateral_usd: float
    exposure_usd: float        # sum of position sizes
    unrealized_net_usd: float  # sum of net pnl
    prices: dict[str, float]
    oracle_age_sec: float
    unpriced: list[str] = field(default_factory=list)   # open positions we cannot value yet (custody/oracle missing)

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet, "ts": self.ts, "slot": self.slot,
            "equity_usd": round(self.equity_usd, 2), "collateral_usd": round(self.collateral_usd, 2),
            "exposure_usd": round(self.exposure_usd, 2), "unrealized_net_usd": round(self.unrealized_net_usd, 2),
            "prices": {k: round(v, 4) for k, v in self.prices.items()}, "oracle_age_sec": round(self.oracle_age_sec, 1),
            "positions": [p.__dict__ for p in self.positions], "unpriced": list(self.unpriced),
        }


@dataclass
class GuardState:
    wallet: str
    positions: dict[str, Position] = field(default_factory=dict)
    custodies: dict[str, dict[str, Any]] = field(default_factory=dict)
    oracles: dict[str, DovesPrice] = field(default_factory=dict)       # by oracle pubkey
    oracle_for_custody: dict[str, str] = field(default_factory=dict)   # custody -> oracle pubkey
    last_slot: int = 0
    last_update_at: float = 0.0
    decode_errors: int = 0
    account_versions: dict[str, tuple[int, int]] = field(default_factory=dict)   # pubkey -> (slot, write_version)
    raw: dict[str, bytes] = field(default_factory=dict)                            # pubkey -> last decoded bytes
    stale_dropped: int = 0
    unchanged_skipped: int = 0

    # ---- ingestion -------------------------------------------------------------
    def apply(self, upd: AccountUpdate | SlotUpdate) -> str | None:
        """Apply one update. Returns a change tag ("position", "custody", "oracle", "slot") or None."""
        self.last_update_at = time.time()
        if isinstance(upd, SlotUpdate):
            self.last_slot = max(self.last_slot, upd.slot)
            return "slot"
        self.last_slot = max(self.last_slot, upd.slot)
        # Ordering guard: a replayed / buffered frame (stream reconnect, proxy stall) or a polled snapshot
        # must never overwrite a newer state of the same account. Older slot -> drop; same slot -> keep the
        # higher write_version (0 = unknown, e.g. RPC: accepted at the same slot, dropped only if older).
        prev = self.account_versions.get(upd.pubkey)
        if prev is not None:
            if upd.slot < prev[0] or (upd.slot == prev[0] and upd.write_version <= prev[1] and prev[1] > 0):
                self.stale_dropped += 1
                return None
        self.account_versions[upd.pubkey] = (upd.slot, upd.write_version)
        if self.raw.get(upd.pubkey) == upd.data:          # same bytes (typical for RPC polls): nothing to decode
            self.unchanged_skipped += 1
            return None
        self.raw[upd.pubkey] = upd.data
        try:
            if upd.owner == JUPITER_PERPS_PROGRAM:
                if is_position_account(upd.data):
                    pos = decode_position(upd.pubkey, upd.data)
                    if self.wallet and pos.owner != self.wallet:   # empty wallet = accept every owner (demo picker)
                        return None
                    self.positions[upd.pubkey] = pos
                    return "position"
                if upd.pubkey in CUSTODY_SYMBOL or upd.pubkey in self.custodies:
                    c = decode_custody(upd.pubkey, upd.data)
                    self.custodies[upd.pubkey] = c
                    # Jupiter moved to the aggregated Doves feed (AgPriceFeed) in mid-2026; the legacy
                    # `dovesOracle` PriceFeed accounts are no longer updated (checked 2026-09-24: 111 days old).
                    ag = c.get("dovesAgOracle") or ""
                    self.oracle_for_custody[upd.pubkey] = ag if ag and not ag.startswith("1111111") else c["dovesOracle"]
                    return "custody"
            elif upd.owner == DOVES_PROGRAM:
                self.oracles[upd.pubkey] = decode_doves_price_feed(upd.pubkey, upd.data)
                return "oracle"
        except ValueError as exc:
            self.decode_errors += 1
            log.warning("decode error for %s: %s", upd.pubkey, exc)
        return None

    # ---- derived ---------------------------------------------------------------
    def oracle_pubkeys(self) -> list[str]:
        return sorted({v for v in self.oracle_for_custody.values()})

    def market_name(self, custody_pubkey: str) -> str:
        return CUSTODY_SYMBOL.get(custody_pubkey, custody_pubkey[:6])

    def snapshot(self, now: float | None = None) -> AccountSnapshot:
        now = now or time.time()
        views: list[PositionView] = []
        prices: dict[str, float] = {}
        for cust_pk, oracle_pk in self.oracle_for_custody.items():
            o = self.oracles.get(oracle_pk)
            if o:
                prices[self.market_name(cust_pk)] = o.price_usd
        # oracle age: only the market feeds that price our marks. Collateral is stored on-chain in USD
        # (collateralUsd), so its feed is irrelevant — and stablecoin feeds can be stale for months
        # (USDT AgPriceFeed: 112 days old on 2026-09-24), which produced a permanent false "oracle stale".
        relevant = {p.custody for p in self.positions.values() if p.is_open}
        if not relevant:
            relevant = {c for c, d in self.custodies.items() if not d.get("isStable")}
        ages = [now - self.oracles[self.oracle_for_custody[c]].timestamp
                for c in relevant if self.oracle_for_custody.get(c) in self.oracles]
        oldest_oracle = max(ages, default=0.0)
        unpriced: list[str] = []
        for pos in self.positions.values():
            if not pos.is_open:
                continue
            custody = self.custodies.get(pos.custody)
            coll = self.custodies.get(pos.collateral_custody)
            oracle = self.oracles.get(self.oracle_for_custody.get(pos.custody, ""))
            if not (custody and coll and oracle):
                unpriced.append(pos.pubkey)   # open, but not valuable yet — NOT the same as closed
                continue
            views.append(build_view(pos, self.market_name(pos.custody), custody, coll,
                                    oracle.price_1e6(), oracle.timestamp, int(now)))
        return AccountSnapshot(
            wallet=self.wallet, ts=now, slot=self.last_slot, positions=views,
            equity_usd=sum(v.net_value_usd for v in views),
            collateral_usd=sum(v.collateral_usd for v in views),
            exposure_usd=sum(v.size_usd for v in views),
            unrealized_net_usd=sum(v.net_pnl_usd for v in views),
            prices=prices, oracle_age_sec=oldest_oracle, unpriced=unpriced,
        )
