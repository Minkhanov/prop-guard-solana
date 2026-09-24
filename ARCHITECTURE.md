# Prop Guard — architecture

One process, one wallet, three interchangeable Solami data paths, one risk engine.

```
                         Solami (https://solami.dev)
  ┌───────────────────────┬──────────────────────────┬────────────────────────────┐
  │ JSON-RPC              │ Yellowstone gRPC         │ Mirage (WebSocket)         │
  │ rpc.solami.dev/sol    │ grpc.solami.dev:443      │ ws.solami.dev/mirage/…     │
  │ ?api_key=             │ metadata x-token         │ ?api_key=  (saved filter)  │
  └──────────┬────────────┴────────────┬─────────────┴──────────────┬─────────────┘
             │ bootstrap + polling      │ SubscribeUpdate            │ same protobuf frames
             │ (getProgramAccountsV2,   │ (accounts / slots / ping)  │ over a plain socket
             │  getMultipleAccounts,    │ from_slot replay           │
             │  getSlot)                │                            │
             ▼                          ▼                            ▼
   transports/rpc.py         transports/grpc_yellowstone.py   transports/mirage.py
             └──────────────────────────┼────────────────────────────┘
                                        ▼
                     AccountUpdate / SlotUpdate  (one contract, `source` + `filter_name` tags)
                                        │
                              engine/state.py
                   account cache · ordering guard (slot, write_version) · decoders
                   (decode/jupiter.py: Position by offsets, Custody via IDL, Doves feeds)
                                        │ snapshot() → PositionView[]  (math/jupiter_math.py)
                                        ▼
                              engine/rules.py
        R1 daily loss (UTC day, honest anchor) · R2 liquidation distance · R3 exposure
        R4 data health (oracle age, stream silence, slot lag) · events (open/increase/reduce/close)
                                        │ Alert[]
                     ┌──────────────────┴──────────────────┐
                     ▼                                     ▼
          alerts/router.py                        panel/server.py (FastAPI)
          console · Telegram (live | dry-run)     GET /  · /api/state · /api/health · /events (SSE)
          dedup key+level · cooldown · min level  engine/metrics.py → stream health block
                     ▲
          guard.py — the loop + supervisors:
            tick loop (evaluate on change) · RPC head loop (lag reference every 5 s)
            fallback supervisor (stream silent > N s → RPC polling, back when the stream resumes)
            health journal (JSON lines)
```

## Data path details

**Bootstrap (all transports, over RPC).** Custodies first (`getMultipleAccounts`, 6 keys) — that is where
the live oracle addresses (`custody.dovesAgOracle`) come from, nothing is hardcoded. Then the wallet's
Position PDAs via `getProgramAccountsV2` with two memcmp filters (discriminator at offset 0, owner at
offset 8) and paginated `limit`/`paginationKey`; then the oracles. Rules run only after this snapshot
is complete, so a restart never reports "position opened".

**gRPC (Pro plan).** One `SubscribeRequest` with four scoped filters — `positions` (owner = Perps program
+ memcmp discriminator + memcmp owner), `custodies` (6 pubkeys), `oracles` (the aggregated Doves feeds),
`slots` — at `processed` commitment. Server pings are answered with a ping request. A request that
carries `ping` is treated by Yellowstone as keep-alive only (filters are *not* installed), so the
subscription request never includes it. On reconnect: `from_slot = last_seen − 1` while the gap is
inside Solami's replay window (3,500 slots, with a safety margin); a larger gap or a rejected
`from_slot` triggers an RPC re-bootstrap and a fresh subscription from the head — counted as
`rebootstraps` in the metrics. Backoff 1 → 30 s with jitter.

**Mirage (Dev+ plan, streaming bandwidth).** The same `SubscribeUpdate` protobufs over a WebSocket, decoded
with the same stubs; a saved subscription id from the dashboard. Written to the docs, not exercised
live yet (the trial key has no Mirage role — `/mirage/list` → 403 `MirageView`).

**RPC polling (Free plan) and fallback.** `getMultipleAccounts` (positions + custodies + oracles in one
call — Solami accepts 1,000 keys per call) every `POLL_INTERVAL_MS`, wallet re-scan every 30 s. The
same transport is started automatically as a fallback when a stream is silent longer than
`FALLBACK_SILENCE_SEC` and stopped when the stream delivers again. Pings never count as data.

**Ordering guard.** The state keeps `(slot, write_version)` per account; a frame older than what is
already known (replayed or buffered during a stall, or a stale poll) is dropped, so a burst of
replayed frames after a reconnect cannot overwrite fresher data.

## Money math (exact integers, 1e-6 USD)

Ported 1:1 from Jupiter's reference TypeScript (`julianfssen/jupiter-perps-anchor-idl-parsing`):
PnL before fees, borrow fee from `cumulativeInterestRate − position.cumulativeInterestSnapshot` (with
the current hourly rate applied since the custody's last update), close fee = `decreasePositionBps` +
price-impact bps (`size × 1e4 / tradeImpactFeeScalar`, ceil), open fee = `increasePositionBps` + impact,
liquidation price = the reference formula with `maxLeverage` from the custody. Verified live on
2026-09-24 against `perps-api.jup.ag`: liquidation price 102.79 vs 102.79.

## Daily book (R1) — what is and is not known

* Baseline per open position = its net PnL at the anchor; positions opened today (on-chain `openTime`)
  are baselined at zero plus their estimated open fee.
* Increase: the program settles the accrued borrow fee (it disappears from the unrealized PnL) and
  charges an open fee from the delivered collateral — both are added to the baseline so they stay
  counted as today's cost. Collateral add/remove is not PnL.
* Partial close: realized = on-chain `realisedPnlUsd` delta (exact) − the closed fraction's baseline.
  Full close: the program zeroes `realisedPnlUsd`, so the last mark is used (labelled "≈").
* Anchor source is explicit everywhere: `utc_midnight` (guard was running through 00:00 UTC),
  `first_observation` (started mid-day — PnL made earlier today by positions opened before today is
  not counted, and the panel says so), or `ACCOUNT_SIZE_USD` as a fixed base.

## Modules

| Path | Role |
|---|---|
| `propguard/cli.py` | `check-config`, `positions`, `watch`, `demo`, `grpc-filters`, `telegram-test` |
| `propguard/config.py` | tiny `.env` loader, validated `Settings`, masked output |
| `propguard/guard.py` | wiring: transport → state → rules → alerts/panel; fallback supervisor; health journal |
| `propguard/transports/` | `rpc.py`, `grpc_yellowstone.py`, `mirage.py`, `yellowstone/` (vendored protos + generated stubs) |
| `propguard/decode/` | Borsh reader + IDL decoder, Jupiter Position/Custody, Doves PriceFeed/AgPriceFeed |
| `propguard/math/jupiter_math.py` | PnL, fees, liquidation, `PositionView` |
| `propguard/engine/` | `state.py` (cache + ordering guard), `rules.py` (R1–R4 + events + daily book), `metrics.py` |
| `propguard/alerts/router.py` | console / Telegram (live or dry-run) sinks, dedup + cooldown |
| `propguard/panel/` | FastAPI app (`/api/state`, `/api/health`, `/events`) + single-page dashboard |
| `scripts/chaos_proxy.py` | TLS-passthrough proxy with pause / resume / drop — for resilience demos |
| `tests/` | 50 tests: decoders on mainnet fixtures, math, rules/daily book, alerts, metrics, transports on mocks (gRPC server, RPC, WebSocket), guard fallback |
