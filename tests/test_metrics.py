"""Stream-health metrics: pings are not data, lag is clamped, fallback is counted."""
import time

from propguard.engine.metrics import StreamMetrics


def test_pings_do_not_end_silence():
    m = StreamMetrics(transport="grpc")
    m.on_message(100, ["slots"])
    m.last_message_at = time.time() - 30          # 30 s of real silence
    for _ in range(5):
        m.on_ping(17)
    d = m.to_dict()
    assert d["pings_total"] == 5 and d["messages_total"] == 1
    assert d["silence_sec"] >= 29, "keep-alives must not reset the silence timer"
    assert d["bytes_total"] == 100 + 5 * 17


def test_slot_lag_clamped_but_raw_kept():
    m = StreamMetrics()
    assert m.slot_lag() is None
    m.on_slot(1_000); m.on_rpc_slot(982)          # processed stream ahead of the polled head
    assert m.slot_lag() == 0 and m.slot_lag_raw() == -18
    m.on_rpc_slot(1_060)
    assert m.slot_lag() == 60


def test_fallback_and_reconnect_counters():
    m = StreamMetrics(transport="grpc")
    m.on_fallback(True); m.on_fallback(True)
    assert m.fallback_count == 1 and m.active_path == "rpc-fallback"
    m.on_fallback(False)
    assert m.active_path == "grpc" and m.to_dict()["fallback_for_sec"] == 0.0
    m.on_reconnect(replayed=True); m.on_reconnect(replayed=False); m.on_reconnect()
    assert (m.reconnects, m.replays, m.rebootstraps) == (3, 1, 1)


def test_state_drops_stale_account_frames(mainnet_accounts):
    """A replayed/buffered oracle frame from an older slot must not overwrite a newer polled state."""
    from propguard.engine.state import GuardState
    from propguard.transports.base import AccountUpdate
    st = GuardState(wallet="")
    fx = mainnet_accounts["doves_ag_sol"]
    assert st.apply(AccountUpdate(fx["pubkey"], fx["owner"], fx["data"], 1_000, 5, source="grpc")) == "oracle"
    assert st.apply(AccountUpdate(fx["pubkey"], fx["owner"], fx["data"], 900, 9, source="grpc")) is None      # older slot
    assert st.apply(AccountUpdate(fx["pubkey"], fx["owner"], fx["data"], 1_000, 5, source="grpc")) is None    # duplicate
    assert st.apply(AccountUpdate(fx["pubkey"], fx["owner"], fx["data"], 1_000, 6, source="grpc")) == "oracle"  # newer write
    assert st.apply(AccountUpdate(fx["pubkey"], fx["owner"], fx["data"], 1_001, 0, source="rpc")) == "oracle"   # RPC, newer slot
    assert st.apply(AccountUpdate(fx["pubkey"], fx["owner"], fx["data"], 1_001, 0, source="rpc")) == "oracle"   # RPC re-poll, same slot ok
    assert st.stale_dropped == 2
