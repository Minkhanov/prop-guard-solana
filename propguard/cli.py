"""Command-line entry point.

    propguard check-config            validate .env / environment, print masked settings
    propguard positions [--wallet W]  one-shot: open positions with live risk numbers
    propguard watch [--panel]         run the guard (transport from TRANSPORT / --transport)
    propguard demo [--panel]          pick a large, live open position on mainnet and guard it
    propguard grpc-filters            print the Yellowstone subscribe filters we would send
    propguard telegram-test           send one test alert through the Telegram sink (or dry-run)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from typing import Any

from . import __version__
from .config import Settings, load_settings


def _setup_logging(level: str) -> None:
    for stream in (sys.stdout, sys.stderr):   # Windows cp1251 consoles; logging writes to stderr
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _settings(args: argparse.Namespace, *, need_wallet: bool) -> Settings:
    overrides = {k: getattr(args, k, None) for k in ("transport", "wallet", "solami_rpc_url", "solami_region", "commitment",
                                                     "health_log_file", "alerts_dry_run")}
    s = load_settings(getattr(args, "env", None), overrides)
    probs = s.problems(need_wallet=need_wallet)
    if probs:
        print("Configuration problems:", file=sys.stderr)
        for p in probs:
            print(f"  - {p}", file=sys.stderr)
        raise SystemExit(2)
    return s


def cmd_check_config(args: argparse.Namespace) -> int:
    s = load_settings(args.env, {k: getattr(args, k, None) for k in ("transport", "wallet", "solami_rpc_url", "solami_region")})
    probs = s.problems(need_wallet=False)
    print(f"propguard {__version__} — configuration ({args.env or '.env'} + environment)\n")
    for k, v in s.masked().items():
        src = s.source.get(k, "default")
        print(f"  {k:<26} {str(v):<40} [{src}]")
    print(f"\n  rpc_url        {s.rpc_url}\n  grpc_endpoint  {s.grpc_endpoint}\n  ws_url         {s.ws_url}")
    if s.mirage_subscription_id:
        print(f"  mirage_url     {s.mirage_url}")
    print(f"  telegram       {'live' if s.telegram_enabled and not s.alerts_dry_run else 'dry-run (no token or ALERTS_DRY_RUN=1)'}")
    if probs:
        print("\nProblems:")
        for p in probs:
            print(f"  - {p}")
        return 2
    print("\nOK — configuration is valid" + ("" if s.wallet else " (WALLET empty: `watch` needs it, `demo` does not)"))
    return 0


async def _positions(s: Settings) -> dict[str, Any]:
    from .guard import Guard
    return await Guard(s).once()


def cmd_positions(args: argparse.Namespace) -> int:
    s = _settings(args, need_wallet=True)
    s.transport = "rpc"
    out = asyncio.run(_positions(s))
    if args.json:
        print(json.dumps(out, indent=1))
        return 0
    snap = out["snapshot"]
    print(f"wallet {snap['wallet']}  slot {snap['slot']}  equity ${snap['equity_usd']:,.2f}  exposure ${snap['exposure_usd']:,.2f}")
    print(f"prices: " + ", ".join(f"{k} {v:,.2f}" for k, v in snap["prices"].items()))
    if not snap["positions"]:
        print("no open positions")
    for v in snap["positions"]:
        print(f"  {v['market']:<5} {v['side']:<5} size ${v['size_usd']:>12,.2f} coll ${v['collateral_usd']:>10,.2f} "
              f"{v['leverage']:5.1f}x entry {v['entry_price']:>10,.2f} mark {v['mark_price']:>10,.2f} "
              f"pnl {v['pnl_usd']:>+10,.2f} fees {v['borrow_fee_usd'] + v['close_fee_usd']:>8,.2f} "
              f"liq {v['liq_price']:>10,.2f} ({v['liq_distance_pct']:.2f}% away)")
    if out["decode_errors"]:
        print(f"decode errors: {out['decode_errors']}")
    return 0


async def pick_demo_wallet(s: Settings, *, min_liq_distance_pct: float = 3.0, max_pages: int = 30,
                           time_budget_s: float = 15.0, now: float | None = None) -> dict[str, Any]:
    """Largest open Jupiter Perps position among the accounts touched in the last DEMO_SCAN_HOURS
    that is still alive (≥ `min_liq_distance_pct` from liquidation, fresh oracle) -> its owner.

    Bounded: `changedSinceSlot` (Solami getProgramAccountsV2) instead of a > 400,000-account full
    scan, plus a page and time cap. Returns a dict with the pick and the scan statistics."""
    from .constants import CUSTODIES, DOVES_ORACLES_FALLBACK
    from .engine.state import GuardState
    from .transports.base import AccountUpdate
    from .transports.rpc import RpcClient, fetch_recent_positions
    t0 = time.perf_counter()
    rpc = RpcClient(s.rpc_url, s.solami_api_key if s.rpc_is_solami else "", rps=s.rpc_rps, is_solami=s.rpc_is_solami)
    try:
        since_slots = int(s.demo_scan_hours * 3600 / 0.4)
        _, accounts = await fetch_recent_positions(rpc, since_slots=since_slots, max_pages=max_pages, time_budget_s=time_budget_s)
        # one state for all candidates (owner filter off) so the same math prices every position
        st = GuardState(wallet="")
        _, cust = await rpc.get_multiple_accounts(list(CUSTODIES.values()), "confirmed")
        for u in cust:
            if u is not None:
                st.apply(u)
        oracles = st.oracle_pubkeys() or list(DOVES_ORACLES_FALLBACK.values())
        _, ors = await rpc.get_multiple_accounts(oracles, "confirmed")
        for u in ors:
            if u is not None:
                st.apply(u)
    finally:
        await rpc.close()
    from .decode.jupiter import decode_position
    candidates = []
    for a in accounts:
        try:
            p = decode_position(a.pubkey, a.data)
        except ValueError:
            continue
        if p.is_open:
            st.positions[p.pubkey] = p
            candidates.append(p)
    snap = st.snapshot(now)
    alive = [v for v in snap.positions if v.liq_distance_pct >= min_liq_distance_pct and v.oracle_age_sec <= s.oracle_stale_sec]
    alive.sort(key=lambda v: v.size_usd, reverse=True)
    owner_of = {p.pubkey: p.owner for p in candidates}
    result = {"scanned_accounts": len(accounts), "open_positions": len(candidates), "alive_positions": len(alive),
              "pages": rpc.last_scan_pages, "truncated": rpc.last_scan_truncated, "hours": s.demo_scan_hours,
              "elapsed_s": round(time.perf_counter() - t0, 2)}
    if not alive:
        raise SystemExit(f"demo: no live open position found in the last {s.demo_scan_hours:g}h "
                         f"({len(candidates)} open, {len(accounts)} scanned) — set WALLET explicitly")
    best = alive[0]
    result.update({"wallet": owner_of[best.pubkey], "position": best.pubkey, "market": best.market, "side": best.side,
                   "size_usd": round(best.size_usd, 2), "liq_distance_pct": round(best.liq_distance_pct, 2)})
    logging.getLogger("propguard").info(
        "demo pick: %s — %s %s $%s, %.2f%% from liquidation (scanned %d accounts / %d open / %d alive in %.1fs, %d pages%s)",
        result["wallet"], best.market, best.side, f"{best.size_usd:,.0f}", best.liq_distance_pct, len(accounts), len(candidates),
        len(alive), result["elapsed_s"], rpc.last_scan_pages, ", truncated" if rpc.last_scan_truncated else "")
    return result


def cmd_watch(args: argparse.Namespace, *, demo: bool = False) -> int:
    s = _settings(args, need_wallet=not demo)
    from .guard import Guard
    if demo and not s.wallet:
        s.wallet = asyncio.run(pick_demo_wallet(s))["wallet"]
    guard = Guard(s)
    try:
        asyncio.run(guard.run(with_panel=args.panel))
    except KeyboardInterrupt:
        print("stopped")
    return 0


def cmd_grpc_filters(args: argparse.Namespace) -> int:
    s = _settings(args, need_wallet=True)
    from .constants import CUSTODIES, DOVES_ORACLES_FALLBACK
    from .transports.grpc_yellowstone import describe_filters
    print(json.dumps({"endpoint": s.grpc_endpoint, "commitment": s.commitment,
                      "filters": describe_filters(s.wallet, list(CUSTODIES.values()), list(DOVES_ORACLES_FALLBACK.values()))}, indent=1))
    return 0


async def _telegram_test(s: Settings, text: str) -> int:
    from .alerts import AlertContext, TelegramSink
    from .engine.rules import Alert
    sink = TelegramSink(s.telegram_bot_token, s.telegram_chat_id, ctx=AlertContext(wallet=s.wallet or "(no wallet)", transport=s.transport),
                        min_level="info", dry_run=s.telegram_dry_run)
    try:
        await sink.send(Alert("warn", "test", "test", text))
    finally:
        await sink.close()
    mode = "dry-run" if sink.dry_run else "live"
    if sink.failed:
        print(f"telegram {mode}: FAILED — {sink.last_error}")
        return 1
    print(f"telegram {mode}: sent {sink.sent} message(s)" + (" (set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to deliver)" if sink.dry_run else ""))
    return 0


def cmd_telegram_test(args: argparse.Namespace) -> int:
    s = _settings(args, need_wallet=False)
    return asyncio.run(_telegram_test(s, args.text))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="propguard", description="Prop Guard — real-time prop-firm risk guard for Jupiter Perps traders (data by Solami)")
    p.add_argument("--version", action="version", version=f"propguard {__version__}")
    p.add_argument("--env", help="path to .env file (default: ./.env or $PROPGUARD_ENV)")
    p.add_argument("--log-level", default=None, help="DEBUG | INFO | WARNING")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--wallet", help="trader wallet (base58); overrides WALLET")
    common.add_argument("--transport", choices=("rpc", "grpc", "mirage"), help="overrides TRANSPORT")
    common.add_argument("--rpc-url", dest="solami_rpc_url", help="override RPC URL (e.g. a public RPC for a keyless smoke test)")
    common.add_argument("--region", dest="solami_region", choices=("ams", "fra", "nyc"), help="pin a Solami region")
    common.add_argument("--commitment", choices=("processed", "confirmed", "finalized"))
    common.add_argument("--dry-run", dest="alerts_dry_run", action="store_const", const=True, default=None,
                        help="log Telegram messages instead of sending them")
    runopts = argparse.ArgumentParser(add_help=False)
    runopts.add_argument("--panel", action="store_true", help="serve the web panel")
    runopts.add_argument("--health-log", dest="health_log_file", metavar="FILE",
                         help="append stream-health metrics as JSON lines to FILE (every HEALTH_LOG_EVERY_SEC)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check-config", parents=[common], help="validate configuration").set_defaults(fn=cmd_check_config)
    sp = sub.add_parser("positions", parents=[common], help="one-shot risk view of the wallet's open positions")
    sp.add_argument("--json", action="store_true"); sp.set_defaults(fn=cmd_positions)
    sw = sub.add_parser("watch", parents=[common, runopts], help="run the guard")
    sw.set_defaults(fn=cmd_watch)
    sd = sub.add_parser("demo", parents=[common, runopts], help="guard a large live open position picked on mainnet")
    sd.set_defaults(fn=lambda a: cmd_watch(a, demo=True))
    sub.add_parser("grpc-filters", parents=[common], help="print the Yellowstone subscribe filters").set_defaults(fn=cmd_grpc_filters)
    st = sub.add_parser("telegram-test", parents=[common], help="send one test alert via Telegram (dry-run without a token)")
    st.add_argument("--text", default="Prop Guard test alert — if you read this, delivery works")
    st.set_defaults(fn=cmd_telegram_test)
    return p


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # before --help output on Windows consoles
    args = build_parser().parse_args(argv)
    level = args.log_level or load_settings(args.env).log_level
    _setup_logging(level)
    try:
        return int(args.fn(args) or 0)
    except PermissionError as exc:
        print(f"auth error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
