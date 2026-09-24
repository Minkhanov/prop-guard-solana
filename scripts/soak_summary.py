"""Summarise a Prop Guard health journal (JSONL written by `watch --health-log`).

Usage:  python scripts/soak_summary.py logs/health-long-YYYYMMDD-HHMM.jsonl [--md]

Prints duration, throughput, reliability counters, slot lag, oracle age, journal gaps and
alerting counters. With --md prints a Markdown table ready for docs/soak-*.md.
No secrets are read: the journal contains only public metrics.
"""
from __future__ import annotations

import json
import statistics
import sys
from datetime import datetime, timezone


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = min(len(s) - 1, max(0, round(q / 100 * (len(s) - 1))))
    return s[k]


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # Windows consoles default to a legacy code page
    path, md = argv[0], "--md" in argv
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    if len(rows) < 2:
        print("journal too short")
        return 1
    first, last = rows[0], rows[-1]
    t0, t1 = first["ts"], last["ts"]
    dur = t1 - t0
    gaps = [b["ts"] - a["ts"] for a, b in zip(rows, rows[1:])]
    lag = [r["slot_lag"] for r in rows if r.get("slot_lag") is not None]
    lag_missing = len(rows) - len(lag)
    oracle = [r["oracle_age_sec"] for r in rows if r.get("oracle_age_sec") is not None]
    silence = [r["silence_sec"] for r in rows if r.get("silence_sec") is not None]
    rate = [r["msg_per_sec_10s"] for r in rows  # skip the first 15 s: the 10 s window is still filling
            if r.get("msg_per_sec_10s") is not None and r.get("uptime_sec", 99) >= 15]
    rpc_lat = [r["rpc_latency_ms"] for r in rows if r.get("rpc_latency_ms")]
    msgs = last["messages_total"] - first["messages_total"]
    alert = last.get("alerting", {})
    tg = alert.get("telegram", {})

    def utc(ts: float) -> str:
        return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    stats = [
        ("Window", f"{utc(t0)} → {utc(t1)} ({dur / 60:.1f} min, {len(rows)} health samples)"),
        ("Transport", f"{last.get('transport')} (active path at end: {last.get('active_path')})"),
        ("Data messages", f"{last['messages_total']:,} total, {msgs / dur:.2f} msg/s average; "
                          f"10 s rate median {statistics.median(rate):.1f}, min {min(rate):.1f} (after the first 15 s)" if rate else ""),
        ("By filter", ", ".join(f"{k} {v:,}" for k, v in last.get("by_filter", {}).items())),
        ("Pings (not counted as data)", f"{last.get('pings_total', 0):,}"),
        ("Bytes", f"{last.get('bytes_total', 0) / 1e6:.2f} MB"),
        ("Connects / reconnects / replays / rebootstraps",
         f"{last.get('connects')} / {last.get('reconnects')} / {last.get('replays')} / {last.get('rebootstraps')}"),
        ("Stream errors", f"{last.get('errors')} (last: {last.get('last_error') or '—'})"),
        ("RPC fallback activations", f"{last.get('fallback_count')}"),
        ("Slot lag (stream vs RPC, clamped)", f"max {max(lag)}, p99 {pct(lag, 99)}, zero in {sum(1 for x in lag if x == 0) / len(lag):.1%} of samples"
                                               + (f" ({lag_missing} samples before the first RPC head)" if lag_missing else "") if lag else "—"),
        ("Stream silence", f"max {max(silence):.1f} s, p99 {pct(silence, 99):.1f} s"),
        ("Oracle age", f"median {statistics.median(oracle):.1f} s, p99 {pct(oracle, 99):.1f} s, max {max(oracle):.1f} s" if oracle else "—"),
        ("Side RPC calls", f"{last.get('rpc_calls')} calls, {last.get('rpc_errors')} errors, latency median {statistics.median(rpc_lat):.0f} ms" if rpc_lat else f"{last.get('rpc_calls')} calls"),
        ("Health journal gaps", f"max {max(gaps):.1f} s between samples (target 5 s)"),
        ("Alerts", f"delivered {alert.get('delivered')}, suppressed by dedup/cooldown {alert.get('suppressed')}; "
                   f"telegram mode {tg.get('mode')}, sent {tg.get('sent')}, failed {tg.get('failed')}"),
    ]
    if md:
        print("| Metric | Value |\n|---|---|")
        for k, v in stats:
            print(f"| {k} | {v} |")
    else:
        w = max(len(k) for k, _ in stats)
        for k, v in stats:
            print(f"{k:<{w}}  {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
