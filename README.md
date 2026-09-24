# Prop Guard — a live prop-firm risk guard for Jupiter Perps traders, fed by Solami

[![ci](https://github.com/Minkhanov/prop-guard-solana/actions/workflows/ci.yml/badge.svg)](https://github.com/Minkhanov/prop-guard-solana/actions)
License: MIT · Python 3.11+ · Solana mainnet · read-only · not financial advice

**▶ Demo video (recorded live on mainnet): https://minkhanov.github.io/prop-guard-solana/demo/**

Prop Guard watches a trader's **Jupiter Perpetuals** positions on Solana mainnet **in real time** through
**Solami** (Yellowstone gRPC with `from_slot` replay, JSON-RPC bootstrap/polling, Mirage) and applies the
rules a funded prop-firm trader lives with on a CEX desk: a **daily loss limit** on the UTC day, an
**exposure ceiling**, **liquidation-distance alarms** and **data-health checks** — with **Telegram alerts** and
a **live panel that shows the health of the data stream** (msg/s, slot lag, reconnects, replays, fallback).

Built by a prop-firm challenge trader who runs exactly these guards on a centralized perps account and wanted the
same discipline on-chain. On Jupiter a liquidation costs the whole position collateral, and the wallet
does not ring when you are 2 % away. Prop Guard does — and it tells you when *it* has gone blind.

> Hackathon build for the Solami side track **"Build something live on Solana data"** (Crypto World's Fair,
> Superteam Earn, October 2026). Everything below was run against live mainnet on 2026-09-24 with a
> Solami Pro trial key; numbers quoted are from those runs.

---

## What it does

1. **Live position board** — every open Jupiter Perps position of a wallet: side, size, collateral, leverage,
   entry, mark (Doves aggregated oracle, on-chain), PnL before fees, accrued borrow fee + close fee, net PnL,
   **liquidation price** (Jupiter's own formula — matched `perps-api.jup.ag` to the cent: 102.79 vs 102.79),
   distance to liquidation, borrow APR.
2. **Daily loss limit on the UTC day** — realized + unrealized net PnL vs a % of your base; warnings at
   50 / 80 / 100 % of the limit; survives restarts (JSON state); the **anchor source is always explicit**
   (see *Honesty notes*). Fees charged by the program on opens/increases and settled borrow fees are
   counted as today's cost; partial closes use the on-chain `realisedPnlUsd` delta.
3. **Liquidation and exposure guard** — warn at ≤ 5 % from liquidation, critical at ≤ 2 % (configurable);
   total exposure > N× base; open / increase / reduce / close / collateral events with numbers.
4. **Telegram alerts** — plain Bot API, HTML, ≤ 1 msg/s, dedup + cooldown per rule, minimum level for
   remote delivery, and a **dry-run mode** that logs the exact message when there is no token.
5. **Web panel + stream-health metrics** (FastAPI + Server-Sent Events, one HTML file): transport, active
   path, data messages vs pings, msg/s, bytes, per-filter counts, silence, last-update age, stream slot vs
   RPC head (**slot lag**), oracle age, connects / reconnects / replays / re-bootstraps, fallback state.
6. **Resilience** — reconnect with backoff and `from_slot` replay; RPC re-bootstrap when the gap is larger
   than the replay window; **automatic fallback to RPC polling while the stream is silent** and automatic
   hand-back; an ordering guard that drops stale replayed frames. `scripts/chaos_proxy.py` lets you
   *pause*, *resume* and *drop* the gRPC connection to watch all of this happen.

## How Solami is the data path

Three interchangeable transports feed one engine; the risk logic never knows which wire the bytes came from.

| Transport | Solami endpoint | What it does | Plan |
|---|---|---|---|
| `rpc` | `https://rpc.solami.dev/sol?api_key=…` (query-parameter auth; header auth answers 401) | bootstrap: `getProgramAccountsV2` (paginated `limit`/`paginationKey`, memcmp filters, `changedSinceSlot` for the demo picker) for the wallet's Position PDAs, `getMultipleAccounts` (up to 1,000 keys — verified) for custodies + oracles, `getSlot` as the lag reference; polling on the Free plan; **automatic fallback** for the streams | Free+ |
| `grpc` | `grpc.solami.dev:443`, Yellowstone, metadata `x-token` | four scoped filters on one stream — `positions` (owner = Perps program + memcmp discriminator + memcmp owner @ 8), `custodies`, `oracles`, `slots`; server pings answered; `from_slot` replay on reconnect (Solami keeps 3,500 slots) | Pro+ |
| `mirage` | `wss://ws.solami.dev/mirage/stream/{id}?api_key=` | the same protobuf `SubscribeUpdate` frames over a WebSocket, decoded with the same stubs — one flag to switch (needs a key with the Mirage role; not exercised live yet) | Dev+ |

Region pinning (`SOLAMI_REGION=ams|fra|nyc`) rewrites every hostname. RPC calls are rate-limited
client-side to your plan's RPS, back off on 429, and never retry deterministic errors.

```
Solami RPC   ──bootstrap / poll / fallback──┐
Solami gRPC  ──SubscribeUpdate (replay)─────┤→ AccountUpdate / SlotUpdate → state (decoders, ordering guard)
Solami Mirage ──same frames over WS─────────┘        → snapshot (Jupiter math) → rules R1–R4 → alerts (Telegram, console)
                                                     → metrics → panel (SSE) + /api/health + health journal
```

Details: [ARCHITECTURE.md](ARCHITECTURE.md).

## Run it with your own Solami key (5 minutes)

Requirements: Python 3.11+ (3.12 tested on Windows 11 and Linux CI). No Node, no Rust, no Anchor client.
The Yellowstone protobuf stubs are committed; `grpcio-tools` is only needed to regenerate them.

```bash
git clone https://github.com/Minkhanov/prop-guard-solana && cd prop-guard-solana
python -m venv .venv && . .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
cp .env.example .env                                   # then put SOLAMI_API_KEY=sk_... in it
propguard check-config                                 # prints masked settings, exit 0 = good
pytest -q                                              # 56 tests, no network
```

Get the key at https://solami.dev → Dashboard → API keys → *Standard key*. The Free plan is enough for the
RPC transport; gRPC needs Pro (the hackathon sign-up link gives 7 days of Pro).

**Free plan (RPC only):**

```bash
propguard positions --wallet <TRADER_WALLET>           # one-shot risk view (≈ 2 s on Solami)
propguard watch --wallet <TRADER_WALLET> --panel       # guard + panel at http://127.0.0.1:8787
propguard demo --panel                                 # picks a large, live position on mainnet and guards it
```

**Pro plan (streams):**

```bash
propguard grpc-filters --wallet <TRADER_WALLET>        # print the Yellowstone subscribe request we send
propguard watch --wallet <TRADER_WALLET> --transport grpc --panel --health-log logs/health.jsonl
propguard watch --wallet <TRADER_WALLET> --transport mirage --panel   # needs MIRAGE_SUBSCRIPTION_ID
```

**Telegram:** create a bot with @BotFather, put `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env`, then
`propguard telegram-test` sends one message (without a token it prints the message — dry-run).
`TELEGRAM_MIN_LEVEL=warn` keeps chatter out of the phone; `--dry-run` / `ALERTS_DRY_RUN=1` never sends.

**Rules:** `ACCOUNT_SIZE_USD` (0 = use equity at the anchor), `DAILY_LOSS_LIMIT_PCT`, `MAX_EXPOSURE_X`,
`LIQ_DISTANCE_WARN_PCT`, `LIQ_DISTANCE_CRIT_PCT`, `ORACLE_STALE_SEC`, `STREAM_STALE_SEC`,
`FALLBACK_SILENCE_SEC` — all in `.env.example` with comments.

**Keyless smoke test** (public RPC, no Solami account, slow): `propguard positions --wallet <W> --rpc-url https://api.mainnet-beta.solana.com`.

## The panel and the health endpoint

`GET /api/health` → `200` when the engine is fed (stream, or RPC fallback while the stream is silent), `503`
otherwise. Fields: `ok`, `stream_ok`, `degraded`, `active_path`, `messages_total`, `pings_total` (kept
apart — a subscription that only exchanges keep-alives is not healthy), `msg_per_sec_10s`, `bytes_total`,
`by_filter`, `silence_sec`, `update_age_sec`, `last_stream_slot`, `last_rpc_slot`, `slot_lag` (clamped)
and `slot_lag_raw` (negative = the `processed` stream is ahead of the head polled a few seconds ago),
`oracle_age_sec`, `connects`, `reconnects`, `replays`, `rebootstraps`, `fallback_active`, `fallback_count`,
`errors`, `last_error`, alerting stats. The same block is in the panel and, with `--health-log`, in a
JSON-lines journal every `HEALTH_LOG_EVERY_SEC` seconds (no secrets in it).

The guard raises its own alerts when it goes blind: `stream_stale`, `oracle_stale`, `slot_lag`,
`fallback_on` / `fallback_off`.

## What we measured on mainnet (2026-09-24, Solami Pro trial, Windows 11, home connection)

| Scenario | Result |
|---|---|
| `positions` over Solami RPC | 2.1 s: bootstrap 8 Position accounts + 12 custodies/oracles, one open SOL long $6.97 M, liquidation 102.79 (= Jupiter API) |
| `demo` picker | `getProgramAccountsV2` + `changedSinceSlot` (24 h): 2,684 accounts / 1,150 open / 923 alive in **1.6 s, 3 pages** (a full scan would be > 400,000 accounts, 400 pages) |
| gRPC steady state | **7–8 data msg/s** (`slots` ≈ 3.6/s, `oracles` ≈ 2.5/s, `custodies` ≈ 1/s), 2 pings / 10 s, **slot lag 0** (stream 5–20 slots *ahead* of the polled head), oracle age 0.8–2 s, errors 0 |
| chaos: proxy paused (stream silent) | silence grows, `slot_lag` 13 → 120; **RPC fallback engaged after 18 s** (threshold 15 s), update age back to ≤ 1.3 s, 9 polls covered the gap |
| chaos: proxy resumed | buffered frames flushed (28–31 msg/s burst), stale frames dropped by the ordering guard, **fallback released after 3 s** |
| chaos: connection dropped | `UNAVAILABLE` → reconnect in 1 s, **`from_slot` replay of 6 slots**, `reconnects=1 replays=1`, stream healthy 3 s later |
| soak run | see `docs/soak-2026-09-24.md` (50 min gRPC, health journal) |

Replay was also checked on its own: subscribing with `from_slot = head − 300` delivers the first slot
update exactly at `head − 300`.

## Honesty notes (read before trusting a number)

* **Day anchor.** The trading day is the UTC day. If the guard runs through 00:00 UTC the anchor is
  the true midnight equity (`anchor: 00:00 UTC (observed live)`). If you start it mid-day, the anchor
  is the first observation and **the PnL that positions opened before today made earlier today is not
  counted** — the panel, the day-anchor alert and every daily-loss alert carry the anchor source.
  Positions opened today are always counted from zero (plus their open fee) using on-chain `openTime`.
  Set `ACCOUNT_SIZE_USD` if your prop rules use a fixed base.
* **Realized PnL.** Partial closes use the program's `realisedPnlUsd` delta (exact). On a full close
  the program zeroes that field, so the last mark before the close is used and the alert says "≈".
* **Net value vs the jup.ag UI.** Our close fee includes the price-impact fee (the API's `closeFees`
  shows only the base 6 bps, but Jupiter's liquidation price includes impact — that is why our
  liquidation matches to the cent); the UI's *value* also subtracts the already-paid open fee. Expect
  ≈ 1 % difference in *net value*, none in liquidation price, size, collateral, entry or borrow fee.
* **Oracle age** is measured on the market feed only; stable-coin feeds can be stale for months (the
  USDT `AgPriceFeed` was 112 days old on 2026-09-24) and are irrelevant because collateral is booked
  in USD on-chain.
* **Custody layout.** The vendored IDL covers 1,060 of the 2,000 bytes of a Custody account; everything
  the math uses lives in that prefix and is sanity-checked (`maxLeverage`, `decreasePositionBps`).
* **Mirage** is implemented against the docs, with a local WebSocket test, but has not been run against
  Solami: the trial key has no Mirage role (`/mirage/list` → 403 `MirageView`).
* **Not covered:** spot balances, other venues (Velocity/Flash/Adrena adapters are a roadmap item),
  transaction-level event parsing, multi-wallet mode, any trading action. Read-only, always.

## Tests

`pytest -q` — 56 tests, no network: decoders on mainnet fixtures captured 2026-09-24, exact money math
(incl. the `div_ceil` BN semantics), rules and the daily book (fees on increase, partial-close continuity,
anchor sources, restart), alert router (dry-run, min level, cooldown, token redaction), metrics (pings,
lag clamp), transports on mocks — a mock Yellowstone gRPC server (ping/pong, drop, reconnect with
`from_slot = last − 1`, gap beyond the window → re-bootstrap), the RPC polling transport and the demo
picker on `httpx.MockTransport`, Mirage on a local WebSocket server — and the guard's fallback supervisor.

## Project layout

```
propguard/
  cli.py                 check-config | positions | watch | demo | grpc-filters | telegram-test
  config.py              .env loader + validated Settings (masked in output)
  constants.py           program ids, custodies, oracle fallbacks, Solami hosts/limits
  guard.py               transport → state → rules → alerts/panel; fallback supervisor; health journal
  decode/                borsh.py (Borsh + IDL decoder), jupiter.py (Position, Custody, Doves), idl/
  math/jupiter_math.py   PnL, borrow fee, open/close fee, liquidation price, PositionView
  engine/                state.py (cache + ordering guard), rules.py (R1–R4, events, daily book), metrics.py
  transports/            rpc.py, grpc_yellowstone.py, mirage.py, yellowstone/ (protos + generated stubs)
  alerts/router.py       Telegram (live/dry-run) + console sinks, dedup/cooldown, message format
  panel/                 server.py (FastAPI, SSE), static/index.html
scripts/chaos_proxy.py   TLS-passthrough proxy: pause | resume | drop
tests/                   fixtures captured from mainnet 2026-09-24
```

## Roadmap

* Exact realized PnL on full close from the `DecreasePositionEvent` (gRPC transaction filter).
* Mirage live once a key with the Mirage role is available; Solami webhooks for position events.
* Multi-wallet mode; Velocity (ex-Drift) adapter once its program is open-sourced; Docker image.

## Credits and license

Jupiter Perps account layouts and formulas: [developers.jup.ag/docs/perps](https://developers.jup.ag/docs/perps)
and [julianfssen/jupiter-perps-anchor-idl-parsing](https://github.com/julianfssen/jupiter-perps-anchor-idl-parsing).
Yellowstone protos: [rpcpool/yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc). Data: [Solami](https://solami.dev).
MIT License — see [LICENSE](LICENSE).

*Prop Guard reads public on-chain data only, never holds keys and never sends transactions. Numbers are
estimates derived from on-chain state and can differ from the venue UI; do not trade on them blindly.*
