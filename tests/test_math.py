"""Position math checked against hand-computed numbers (all in 1e-6 USD)."""
from propguard.constants import BPS_POWER, RATE_POWER
from propguard.decode.jupiter import Position
from propguard.math.jupiter_math import (borrow_fee_usd, build_view, close_fee_usd, div_ceil, liq_distance_pct,
                                         liquidation_price, pnl_before_fees)

U = 10**6


def pos(side="long", price=100 * U, size=1000 * U, coll=100 * U, snapshot=0):
    return Position("p", "w", "pool", "cust", "coll", 0, 0, side, price, size, coll, 0, snapshot, 0, 255, 216)


def custody(max_leverage=500 * BPS_POWER, impact_scalar=10**12, close_bps=6, cum_rate=0, hourly_dbps=0):
    """Minimal custody dict with the fields the math uses."""
    return {
        "pricing": {"maxLeverage": max_leverage, "tradeImpactFeeScalar": impact_scalar},
        "decreasePositionBps": close_bps,
        "fundingRateState": {"cumulativeInterestRate": cum_rate, "lastUpdate": 0, "hourlyFundingDbps": hourly_dbps},
        "borrowsFundingRateState": {"cumulativeInterestRate": cum_rate, "lastUpdate": 0, "hourlyFundingDbps": hourly_dbps},
        "jumpRateState": {"minRateBps": 0, "maxRateBps": 0, "targetRateBps": 0, "targetUtilizationRate": 1},
        "assets": {"owned": 0, "locked": 0},
        "debt": 0, "borrowLendInterestsAccured": 0,
    }


def test_div_ceil():
    assert div_ceil(10, 5) == 2 and div_ceil(11, 5) == 3 and div_ceil(-11, 5) == -3


def test_pnl_long_short():
    p = pos("long")
    assert pnl_before_fees(p, 110 * U) == 100 * U      # +10 % on $1000
    assert pnl_before_fees(p, 90 * U) == -100 * U
    s = pos("short")
    assert pnl_before_fees(s, 90 * U) == 100 * U
    assert pnl_before_fees(s, 110 * U) == -100 * U
    assert pnl_before_fees(pos(size=0), 200 * U) == 0


def test_close_fee_and_no_borrow_when_idle():
    p = pos()
    c = custody(close_bps=6, impact_scalar=10**12)
    impact_bps = div_ceil(p.size_usd * BPS_POWER, 10**12)   # 1000e6*1e4/1e12 = 10 bps
    assert impact_bps == 10
    assert close_fee_usd(p, c) == p.size_usd * (6 + 10) // BPS_POWER   # $1.60
    assert borrow_fee_usd(p, c, now=3600) == 0


def test_borrow_fee_from_snapshot_delta():
    p = pos(snapshot=RATE_POWER)                     # snapshot taken at cumulative = 1e9
    c = custody(cum_rate=RATE_POWER + RATE_POWER // 1000)   # rate grew by 0.1 %
    assert borrow_fee_usd(p, c, now=0) == div_ceil((RATE_POWER // 1000) * p.size_usd, RATE_POWER)  # $1.00


def test_liquidation_price_long_10x():
    # $1000 long at $100 with $100 collateral (10x); max leverage 500x => max loss = size/500 + fees
    p = pos("long")
    c = custody()
    liq = liquidation_price(p, c, c, now=0)
    total_fee = close_fee_usd(p, c)
    max_loss = p.size_usd * BPS_POWER // c["pricing"]["maxLeverage"] + total_fee   # 2e6 + 1.6e6
    diff = abs(max_loss - p.collateral_usd) * p.price // p.size_usd                 # (100-3.6)/1000*100 = 9.64
    assert liq == p.price - diff
    assert 90 * U < liq < 91 * U
    assert 9.5 < liq_distance_pct("long", 100 * U, liq) < 9.7


def test_liquidation_price_short_mirror():
    p = pos("short")
    c = custody()
    liq = liquidation_price(p, c, c, now=0)
    assert 109 * U < liq < 110 * U
    assert 9.5 < liq_distance_pct("short", 100 * U, liq) < 9.7
    assert liq_distance_pct("short", 120 * U, liq) < 0     # already past liquidation


def test_build_view_consistency():
    p = pos("long")
    c = custody()
    v = build_view(p, "SOL", c, c, mark_1e6=105 * U, oracle_ts=1000, now=1010)
    assert v.market == "SOL" and v.side == "long" and v.leverage == 10
    assert v.pnl_usd == 50.0 and v.close_fee_usd == 1.6 and v.borrow_fee_usd == 0
    assert v.net_pnl_usd == 48.4 and v.net_value_usd == 148.4
    assert v.oracle_age_sec == 10
