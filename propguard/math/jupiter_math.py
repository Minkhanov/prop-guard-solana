"""Jupiter Perps position math, ported 1:1 from the reference TypeScript examples
(julianfssen/jupiter-perps-anchor-idl-parsing: get-liquidation-price.ts,
get-position-pnl.ts, get-borrow-fee-and-funding-rate.ts).

All money values are integers in 1e-6 USD unless stated otherwise. Python ints are
arbitrary precision, so the BN semantics carry over; division is floor like BN.div.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..constants import BPS_POWER, DBPS_POWER, DEBT_POWER, HOURS_IN_YEAR, RATE_POWER
from ..decode.jupiter import Position


def div_ceil(a: int, b: int) -> int:
    """BN.divmod semantics (truncate toward zero), then round away from zero on a remainder."""
    q, m = divmod(abs(a), abs(b))
    if m:
        q += 1
    return q if (a >= 0) == (b > 0) else -q


# ---- borrow fee ------------------------------------------------------------

def _debt(custody: dict[str, Any]) -> int:
    d = max(custody["debt"] - custody["borrowLendInterestsAccured"], 0)
    return div_ceil(d, DEBT_POWER)


def theoretically_owned(custody: dict[str, Any]) -> int:
    return custody["assets"]["owned"] + _debt(custody)


def total_locked(custody: dict[str, Any]) -> int:
    return custody["assets"]["locked"] + _debt(custody)


def hourly_borrow_rate(custody: dict[str, Any], *, borrow_curve: bool = False) -> int:
    """Hourly rate scaled by RATE_POWER (1e9)."""
    owned = theoretically_owned(custody)
    locked = total_locked(custody)
    frs = custody["borrowsFundingRateState"] if borrow_curve else custody["fundingRateState"]
    linear = custody["fundingRateState"]["hourlyFundingDbps"] != 0
    if linear:
        hourly = frs["hourlyFundingDbps"] * RATE_POWER // DBPS_POWER
        return div_ceil(locked * hourly, owned) if owned > 0 and locked > 0 else 0
    j = custody["jumpRateState"]
    util = locked * RATE_POWER // owned if owned > 0 and locked > 0 else 0
    if util <= j["targetUtilizationRate"]:
        yearly = (div_ceil((j["targetRateBps"] - j["minRateBps"]) * util, j["targetUtilizationRate"])
                  + j["minRateBps"]) * RATE_POWER // BPS_POWER
    else:
        rate_diff = max(0, j["maxRateBps"] - j["targetRateBps"])
        util_diff = max(0, util - j["targetUtilizationRate"])
        denom = max(0, RATE_POWER - j["targetUtilizationRate"])
        if denom == 0:
            raise ZeroDivisionError("jump-rate denominator is 0")
        yearly = (div_ceil(rate_diff * util_diff, denom) + j["targetRateBps"]) * RATE_POWER // BPS_POWER
    return yearly // HOURS_IN_YEAR


def current_funding_rate(custody: dict[str, Any], now: int) -> int:
    if custody["assets"]["owned"] == 0:
        return 0
    interval = now - custody["fundingRateState"]["lastUpdate"]
    return div_ceil(hourly_borrow_rate(custody) * interval, 3600)


def cumulative_interest(custody: dict[str, Any], now: int) -> int:
    frs = custody["fundingRateState"]
    if now > frs["lastUpdate"]:
        return frs["cumulativeInterestRate"] + current_funding_rate(custody, now)
    return frs["cumulativeInterestRate"]


def borrow_fee_usd(pos: Position, collateral_custody: dict[str, Any], now: int) -> int:
    if pos.size_usd == 0:
        return 0
    interest = cumulative_interest(collateral_custody, now) - pos.cumulative_interest_snapshot
    return div_ceil(interest * pos.size_usd, RATE_POWER)


def borrow_apr_pct(custody: dict[str, Any]) -> float:
    return hourly_borrow_rate(custody, borrow_curve=True) / RATE_POWER * HOURS_IN_YEAR * 100


# ---- close fee & liquidation ----------------------------------------------

def close_fee_usd(pos: Position, custody: dict[str, Any]) -> int:
    impact_bps = div_ceil(pos.size_usd * BPS_POWER, custody["pricing"]["tradeImpactFeeScalar"])
    total_bps = custody["decreasePositionBps"] + impact_bps
    return pos.size_usd * total_bps // BPS_POWER


def open_fee_usd(size_delta_usd: int, custody: dict[str, Any]) -> int:
    """Fee charged by the program when a position is opened or increased by `size_delta_usd`
    (1e-6 USD): `increasePositionBps` + price-impact bps, same shape as the close fee.
    Used by the daily book to charge today's opens/increases (the fee is deducted from the
    delivered collateral on-chain, so it is invisible in the position's unrealized PnL)."""
    if size_delta_usd <= 0:
        return 0
    impact_bps = div_ceil(size_delta_usd * BPS_POWER, custody["pricing"]["tradeImpactFeeScalar"])
    return size_delta_usd * (custody["increasePositionBps"] + impact_bps) // BPS_POWER


def liquidation_price(pos: Position, custody: dict[str, Any], collateral_custody: dict[str, Any], now: int) -> int:
    """Liquidation price in 1e-6 USD (same formula as the reference implementation)."""
    total_fee = close_fee_usd(pos, custody) + borrow_fee_usd(pos, collateral_custody, now)
    max_loss = pos.size_usd * BPS_POWER // custody["pricing"]["maxLeverage"] + total_fee
    margin = pos.collateral_usd
    diff = abs(max_loss - margin) * pos.price // pos.size_usd if pos.size_usd else 0
    if pos.side == "long":
        return pos.price + diff if max_loss > margin else pos.price - diff
    return pos.price - diff if max_loss > margin else pos.price + diff


# ---- PnL -------------------------------------------------------------------

def pnl_before_fees(pos: Position, mark_price: int) -> int:
    """Signed PnL in 1e-6 USD at `mark_price` (1e-6 USD), before fees."""
    if pos.size_usd == 0 or pos.price == 0:
        return 0
    delta = abs(mark_price - pos.price)
    pnl = pos.size_usd * delta // pos.price
    profit = mark_price > pos.price if pos.side == "long" else pos.price > mark_price
    return pnl if profit else -pnl


def liq_distance_pct(side: str, mark_price: int, liq_price: int) -> float:
    """% move from mark to liquidation (positive = still alive; <=0 = liquidatable)."""
    if mark_price <= 0:
        return 0.0
    if side == "long":
        return (mark_price - liq_price) / mark_price * 100
    return (liq_price - mark_price) / mark_price * 100


@dataclass(frozen=True)
class PositionView:
    """Everything the risk engine and the panel need about one open position."""
    pubkey: str
    market: str
    side: str
    entry_price: float
    mark_price: float
    size_usd: float
    collateral_usd: float
    leverage: float
    pnl_usd: float            # before fees
    borrow_fee_usd: float
    close_fee_usd: float
    net_pnl_usd: float        # pnl - fees
    net_value_usd: float      # collateral + net pnl (what you'd get on close, approx.)
    liq_price: float
    liq_distance_pct: float
    borrow_apr_pct: float
    oracle_age_sec: float
    # raw on-chain facts the daily book needs (all optional for tests)
    open_time: int = 0
    update_time: int = 0
    realised_pnl_usd: float = 0.0          # program-side realized PnL of partial closes (resets to 0 on full close)
    cumulative_interest_snapshot: int = 0  # changes whenever the program settled borrow fees on the position
    open_fee_bps: float = 0.0              # increasePositionBps of the market custody
    impact_fee_scalar: int = 0             # pricing.tradeImpactFeeScalar of the market custody

    def open_fee_estimate_usd(self, size_delta_usd: float) -> float:
        """Estimated open/increase fee for `size_delta_usd` (USD) on this market."""
        if size_delta_usd <= 0 or not self.impact_fee_scalar:
            return 0.0
        d = int(round(size_delta_usd * 10**6))
        impact_bps = div_ceil(d * BPS_POWER, self.impact_fee_scalar)
        return d * (self.open_fee_bps + impact_bps) / BPS_POWER / 10**6


def build_view(pos: Position, market: str, custody: dict[str, Any], collateral_custody: dict[str, Any],
               mark_1e6: int, oracle_ts: int, now: int) -> PositionView:
    pnl = pnl_before_fees(pos, mark_1e6)
    bf = borrow_fee_usd(pos, collateral_custody, now)
    cf = close_fee_usd(pos, custody)
    liq = liquidation_price(pos, custody, collateral_custody, now)
    net = pnl - bf - cf
    u = 10**6
    return PositionView(
        pubkey=pos.pubkey, market=market, side=pos.side,
        entry_price=pos.price / u, mark_price=mark_1e6 / u,
        size_usd=pos.size_usd / u, collateral_usd=pos.collateral_usd / u, leverage=pos.leverage,
        pnl_usd=pnl / u, borrow_fee_usd=bf / u, close_fee_usd=cf / u, net_pnl_usd=net / u,
        net_value_usd=(pos.collateral_usd + net) / u,
        liq_price=liq / u, liq_distance_pct=liq_distance_pct(pos.side, mark_1e6, liq),
        borrow_apr_pct=borrow_apr_pct(collateral_custody), oracle_age_sec=max(0, now - oracle_ts),
        open_time=pos.open_time, update_time=pos.update_time, realised_pnl_usd=pos.realised_pnl_usd / u,
        cumulative_interest_snapshot=pos.cumulative_interest_snapshot,
        open_fee_bps=float(custody.get("increasePositionBps", 0)), impact_fee_scalar=int(custody["pricing"]["tradeImpactFeeScalar"]),
    )
