from pathlib import Path

from propguard.config import Settings
from propguard.engine.rules import RuleEngine, utc_day
from propguard.engine.state import AccountSnapshot
from propguard.math.jupiter_math import PositionView

DAY = 1_790_000_000  # 2026-09-21 14:13:20 UTC


def view(pk="p1", size=1000.0, coll=100.0, net=0.0, liq_dist=10.0, side="long"):
    return PositionView(pk, "SOL", side, 100.0, 100.0 + net / 10, size, coll, size / coll, net, 0.0, 0.0, net,
                        coll + net, 90.0, liq_dist, 20.0, 1.0)


def snap(ts, positions):
    return AccountSnapshot("w", ts, 1, positions, sum(v.net_value_usd for v in positions),
                           sum(v.collateral_usd for v in positions), sum(v.size_usd for v in positions),
                           sum(v.net_pnl_usd for v in positions), {"SOL": 100.0}, 1.0)


def engine(tmp_path: Path, **kw) -> RuleEngine:
    s = Settings(wallet="w", account_size_usd=kw.pop("base", 1000.0), daily_loss_limit_pct=5.0,
                 liq_distance_warn_pct=5.0, liq_distance_crit_pct=2.0, max_exposure_x=10.0, **kw)
    return RuleEngine(s, state_file=str(tmp_path / "state.json"))


def codes(alerts):
    return [(a.level, a.code) for a in alerts]


def test_day_anchor_and_daily_loss_thresholds(tmp_path):
    e = engine(tmp_path)
    a = e.evaluate(snap(DAY, [view(net=0.0)]))
    assert ("info", "day_anchor") in codes(a)
    assert e.book.day == utc_day(DAY)
    # -$30 of a $50 limit (60 %) -> info, -$45 (90 %) -> warn, -$60 -> crit
    assert ("info", "daily_loss") in codes(e.evaluate(snap(DAY + 10, [view(net=-30.0)])))
    assert ("warn", "daily_loss") in codes(e.evaluate(snap(DAY + 20, [view(net=-45.0)])))
    assert ("crit", "daily_loss") in codes(e.evaluate(snap(DAY + 30, [view(net=-60.0)])))
    assert e.summary(snap(DAY + 30, [view(net=-60.0)]))["daily_limit_used_pct"] == 120.0


def test_close_books_realized_and_collateral_change_is_not_pnl(tmp_path):
    e = engine(tmp_path)
    e.evaluate(snap(DAY, [view(net=10.0)]))            # anchor: position already +10
    e.evaluate(snap(DAY + 5, [view(net=-20.0)]))       # now -20 => daily -30
    assert e.daily_pnl(snap(DAY + 5, [view(net=-20.0)])) == -30.0
    a = e.evaluate(snap(DAY + 10, []))                 # closed at last mark => realized -30
    assert ("info", "position_close") in codes(a)
    assert e.book.realized_today_usd == -30.0
    # new position with bigger collateral: no pnl change
    a = e.evaluate(snap(DAY + 20, [view(pk="p2", coll=500.0, net=0.0)]))
    assert ("info", "position_open") in codes(a)
    assert e.daily_pnl(snap(DAY + 20, [view(pk="p2", coll=500.0, net=0.0)])) == -30.0


def test_new_day_resets_anchor(tmp_path):
    e = engine(tmp_path)
    e.evaluate(snap(DAY, [view(net=-40.0)]))
    a = e.evaluate(snap(DAY + 86_400, [view(net=-40.0)]))
    assert ("info", "day_anchor") in codes(a)
    assert e.book.realized_today_usd == 0.0 and ("crit", "daily_loss") not in codes(a)


def test_liquidation_and_exposure_rules(tmp_path):
    e = engine(tmp_path)
    a = e.evaluate(snap(DAY, [view(liq_dist=4.0)]))
    assert ("warn", "liq_distance") in codes(a)
    a = e.evaluate(snap(DAY + 1, [view(liq_dist=1.5)]))
    assert ("crit", "liq_distance") in codes(a)
    a = e.evaluate(snap(DAY + 2, [view(size=20_000.0, coll=2_000.0)]))   # 20x of $1000 base
    assert ("warn", "exposure") in codes(a)


def test_state_persists_across_restart(tmp_path):
    e = engine(tmp_path)
    e.evaluate(snap(DAY, [view(net=0.0)]))
    e.evaluate(snap(DAY + 5, []))   # close at 0 => realized 0, but the day is anchored
    e2 = engine(tmp_path)
    assert e2.book.day == utc_day(DAY)


def test_health_rules(tmp_path):
    e = engine(tmp_path, stream_stale_sec=20)
    a = e.evaluate(snap(DAY, [view()]), {"messages_total": 10, "silence_sec": 45, "transport": "grpc", "slot_lag": 120})
    assert ("warn", "stream_stale") in codes(a) and ("warn", "slot_lag") in codes(a)


# ---- daily book: fees, partial closes, anchor honesty (added 2026-09-24, phase 2) ----------------
from dataclasses import replace as _replace

FEE_KW = dict(open_fee_bps=6.0, impact_fee_scalar=10**30)   # 6 bps + 1 bps impact (div_ceil never rounds to 0)


def test_increase_charges_settled_borrow_fee_and_open_fee(tmp_path):
    e = engine(tmp_path)
    v1 = _replace(view(net=-20.0), borrow_fee_usd=5.0, net_pnl_usd=-25.0, net_value_usd=75.0,
                  cumulative_interest_snapshot=1, **FEE_KW)
    e.evaluate(snap(DAY, [v1]))                           # baseline -25 (opened before today) -> daily 0
    assert e.daily_pnl(snap(DAY, [v1])) == 0.0
    v2 = _replace(view(size=2000.0, coll=200.0, net=-20.0), borrow_fee_usd=0.0, net_pnl_usd=-20.0, net_value_usd=180.0,
                  cumulative_interest_snapshot=2, **FEE_KW)   # program settled the $5 borrow fee, +$1000 size
    a = e.evaluate(snap(DAY + 5, [v2]))
    assert any(x.code == "position_change" and "INCREASE" in x.text for x in a)
    # the settled $5 and the 7 bps open fee on the added $1000 (= $0.70) are today's cost, not a gain
    assert round(e.daily_pnl(snap(DAY + 5, [v2])), 2) == -0.7
    assert round(e.book.fees_today_usd, 2) == 5.7


def test_partial_close_uses_onchain_realised_and_keeps_continuity(tmp_path):
    e = engine(tmp_path)
    e.evaluate(snap(DAY, [view(net=40.0)]))               # baseline +40 -> daily 0
    e.evaluate(snap(DAY + 5, [view(net=100.0)]))          # mark moved: daily +60
    assert e.daily_pnl(snap(DAY + 5, [view(net=100.0)])) == 60.0
    half = _replace(view(size=500.0, coll=50.0, net=50.0), realised_pnl_usd=48.0)   # program booked +48 (fees inside)
    a = e.evaluate(snap(DAY + 10, [half]))
    txt = [x.text for x in a if x.code == "position_change"][0]
    assert "REDUCE" in txt and "on-chain realisedPnlUsd" in txt
    # realized 48 - 0.5*40 = 28 ; remaining baseline 20 ; unrealized 50-20 = 30 ; daily 58 (= 60 minus $2 of fees)
    assert round(e.book.realized_today_usd, 2) == 28.0
    assert round(e.daily_pnl(snap(DAY + 10, [half])), 2) == 58.0


def test_position_opened_today_counts_from_zero_plus_open_fee(tmp_path):
    e = engine(tmp_path)
    v = _replace(view(net=-15.0), open_time=DAY + 100, **FEE_KW)   # opened today, before the guard started
    e.evaluate(snap(DAY + 200, [v]))
    assert round(e.daily_pnl(snap(DAY + 200, [v])), 2) == -15.7    # whole PnL is today's + $0.70 open fee


def test_anchor_source_first_observation_then_live_midnight(tmp_path):
    e = engine(tmp_path)
    a = e.evaluate(snap(DAY + 20_000, [view(net=-40.0)]))      # started mid-day (19:46 UTC)
    assert e.book.anchor_source == "first_observation" and e.book.anchor_ts == DAY + 20_000
    assert "first observation" in e.book.anchor_label()
    day_alert = [x for x in a if x.code == "day_anchor"][0]
    assert "not counted" in day_alert.text
    s = e.summary(snap(DAY + 20_000, [view(net=-40.0)]))
    assert s["anchor_source"] == "first_observation" and "first observation" in s["anchor_label"]
    a = e.evaluate(snap(DAY + 86_400, [view(net=-40.0)]))      # rollover observed while running
    assert e.book.anchor_source == "utc_midnight" and "00:00 UTC" in e.book.anchor_label()
    assert ("crit", "daily_loss") not in codes(a)
    a = e.evaluate(snap(DAY + 86_400 + 10, [view(net=-100.0)]))
    assert any(x.code == "daily_loss" and "[anchor: 00:00 UTC" in x.text for x in a)


def test_restart_same_day_keeps_anchor_source(tmp_path):
    e = engine(tmp_path)
    e.evaluate(snap(DAY + 100, [view(net=-40.0)]))
    e2 = engine(tmp_path)
    assert e2.book.anchor_source == "first_observation" and e2.book.anchor_ts == DAY + 100
    e2.evaluate(snap(DAY + 200, [view(net=-40.0), view(pk="p9", net=3.0)]))   # a position the book never saw
    assert e2.book.anchor_net_pnl["p9"] == 3.0 and e2.book.anchor_source == "first_observation"


def test_stream_stale_alert_mentions_fallback(tmp_path):
    e = engine(tmp_path, stream_stale_sec=20)
    a = e.evaluate(snap(DAY, [view()]), {"messages_total": 10, "silence_sec": 45, "transport": "grpc",
                                         "active_path": "rpc-fallback", "fallback_active": True, "slot_lag": 0})
    txt = [x.text for x in a if x.code == "stream_stale"][0]
    assert "rpc-fallback" in txt and "RPC fallback is feeding" in txt
