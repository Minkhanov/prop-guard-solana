"""Solami JSON-RPC client + polling transport.

The RPC transport is the lowest common denominator (works on the Free plan) and
also serves as the bootstrap path for the streaming transports: it fetches the
initial set of position accounts, custodies and oracles, then either polls them
(TRANSPORT=rpc), hands the address list to gRPC / Mirage, or takes over as the
fallback while a stream is silent.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import time
from typing import Any

import httpx

from ..constants import CUSTODIES, JUPITER_PERPS_PROGRAM, POSITION_ACCOUNT_SIZE, POSITION_OWNER_OFFSET
from ..decode.borsh import b58encode
from ..decode.jupiter import POSITION_DISCRIMINATOR
from .base import AccountUpdate, SlotUpdate, Transport, UpdateHandler

log = logging.getLogger("propguard.rpc")

# JSON-RPC error codes that will not change on retry (bad request / method / params)
_DETERMINISTIC_RPC_ERRORS = {-32600, -32601, -32602, -32700}


class RpcError(RuntimeError):
    """A JSON-RPC `error` object returned by the node."""

    def __init__(self, method: str, code: int | None, message: str):
        super().__init__(f"RPC {method}: {code} {message}")
        self.method, self.code, self.message = method, code, message


class RateLimiter:
    """Token bucket: at most `rps` calls per second, smooth."""

    def __init__(self, rps: float):
        self.interval = 1.0 / max(rps, 0.01)
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
                now = time.monotonic()
            self._next = now + self.interval


class _RedactFilter(logging.Filter):
    """httpx logs every request URL at INFO/DEBUG — that URL carries `?api_key=`. Mask it."""

    def __init__(self, secret: str):
        super().__init__()
        self.secret = secret

    def filter(self, record: logging.LogRecord) -> bool:
        if self.secret:
            msg = record.getMessage()
            if self.secret in msg:
                record.msg, record.args = msg.replace(self.secret, "***"), ()
        return True


class RpcClient:
    def __init__(self, url: str, api_key: str = "", *, rps: float = 5.0, timeout: float = 20.0,
                 is_solami: bool = True):
        self.url = url
        self.api_key = api_key
        if api_key:
            for name in ("httpx", "httpcore"):
                logging.getLogger(name).addFilter(_RedactFilter(api_key))
        self.is_solami = is_solami
        self.limiter = RateLimiter(rps)
        # Solami authenticates HTTPS RPC with the key as a query parameter (`?api_key=`, alias `api-key`).
        # Header forms (Authorization: Bearer / x-api-key / x-token) answer 401 (checked live 2026-09-24).
        # The key therefore lives in the request URL: never log the URL or a raw httpx error (see _redact).
        sep = "&" if "?" in url else "?"
        self._request_url = f"{url}{sep}api_key={api_key}" if api_key else url
        headers = {"Content-Type": "application/json", "User-Agent": "propguard/0.1"}
        self._client = httpx.AsyncClient(timeout=timeout, headers=headers)
        self._id = 0
        self.stats = {"calls": 0, "errors": 0, "rate_limited": 0, "last_latency_ms": 0.0}

    async def close(self) -> None:
        await self._client.aclose()

    def _redact(self, text: str) -> str:
        """Strip the API key from anything that may reach logs or tracebacks."""
        return text.replace(self.api_key, "***") if self.api_key else text

    async def call(self, method: str, params: list[Any] | None = None, *, retries: int = 4) -> Any:
        self._id += 1
        body = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params or []}
        delay = 0.5
        for attempt in range(retries + 1):
            await self.limiter.wait()
            t0 = time.perf_counter()
            try:
                resp = await self._client.post(self._request_url, json=body)
                self.stats["calls"] += 1
                self.stats["last_latency_ms"] = (time.perf_counter() - t0) * 1000
                if resp.status_code == 429:
                    self.stats["rate_limited"] += 1
                    raise httpx.HTTPStatusError("429", request=resp.request, response=resp)
                if resp.status_code == 401:
                    raise PermissionError("Solami RPC: 401 unauthorized — check SOLAMI_API_KEY")
                if resp.status_code == 403:
                    raise PermissionError("Solami RPC: 403 — your plan does not include this feature")
                resp.raise_for_status()
                payload = resp.json()
                if "error" in payload:
                    err = payload["error"] if isinstance(payload["error"], dict) else {"message": str(payload["error"])}
                    raise RpcError(method, err.get("code"), self._redact(str(err.get("message", err))))
                return payload["result"]
            except PermissionError:
                raise
            except RpcError as exc:
                self.stats["errors"] += 1
                if exc.code in _DETERMINISTIC_RPC_ERRORS:
                    raise                              # retrying "Invalid params" only wastes 7.5 s
                if attempt >= retries:
                    raise RuntimeError(f"RPC {method} failed after {retries} retries: {exc}") from None
                log.warning("RPC %s failed (%s); retry in %.1fs", method, exc, delay)
            except (httpx.HTTPError, ValueError) as exc:   # ValueError: non-JSON body
                self.stats["errors"] += 1
                reason = self._redact(f"{type(exc).__name__}: {exc}")
                if attempt >= retries:
                    raise RuntimeError(f"RPC {method} failed after {retries} retries: {reason}") from None
                log.warning("RPC %s failed (%s); retry in %.1fs", method, reason, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 8.0)

    # ---- helpers -------------------------------------------------------------
    async def get_slot(self, commitment: str = "processed") -> int:
        return int(await self.call("getSlot", [{"commitment": commitment}]))

    async def get_multiple_accounts(self, pubkeys: list[str], commitment: str = "processed") -> tuple[int, list[AccountUpdate | None]]:
        """Returns (context_slot, updates). Solami accepts up to 1,000 keys per call (checked live
        2026-09-24 with 1,000 keys: 0.22 s); stock RPC 100."""
        chunk = 1000 if self.is_solami else 100
        out: list[AccountUpdate | None] = []
        slot = 0
        for i in range(0, len(pubkeys), chunk):
            keys = pubkeys[i:i + chunk]
            res = await self.call("getMultipleAccounts", [keys, {"encoding": "base64", "commitment": commitment}])
            slot = int(res["context"]["slot"])
            for pk, v in zip(keys, res["value"]):
                if v is None:
                    out.append(None)
                else:
                    out.append(AccountUpdate(pk, v["owner"], base64.b64decode(v["data"][0]), slot, source="rpc"))
        return slot, out

    async def get_program_accounts(self, program: str, filters: list[dict[str, Any]], commitment: str = "confirmed",
                                   data_slice: dict[str, int] | None = None, *, changed_since_slot: int | None = None,
                                   max_pages: int | None = None, time_budget_s: float | None = None) -> tuple[int, list[AccountUpdate]]:
        """Program scan. On Solami: `getProgramAccountsV2` (paginated, 1,000 per page, optional
        `changedSinceSlot`), bounded by `max_pages` / `time_budget_s` — `self.last_scan_truncated`
        tells whether the bound was hit. Elsewhere: plain `getProgramAccounts`."""
        cfg: dict[str, Any] = {"encoding": "base64", "commitment": commitment, "filters": filters}
        if data_slice:
            cfg["dataSlice"] = data_slice
        out: list[AccountUpdate] = []
        self.last_scan_truncated = False
        self.last_scan_pages = 0
        if self.is_solami:
            cfg["limit"] = 1000
            if changed_since_slot:
                cfg["changedSinceSlot"] = int(changed_since_slot)
            cursor = None
            t0 = time.monotonic()
            while True:
                if cursor:
                    cfg["paginationKey"] = cursor
                res = await self.call("getProgramAccountsV2", [program, cfg])
                self.last_scan_pages += 1
                slot = int(res["context"]["slot"])
                for item in res["value"]["accounts"]:
                    a = item["account"]
                    out.append(AccountUpdate(item["pubkey"], a["owner"], base64.b64decode(a["data"][0]), slot, source="rpc"))
                cursor = res["value"].get("paginationKey")
                if not cursor:
                    return slot, out
                if (max_pages and self.last_scan_pages >= max_pages) or \
                        (time_budget_s and time.monotonic() - t0 >= time_budget_s):
                    self.last_scan_truncated = True
                    log.info("program scan stopped after %d pages (%d accounts) — bound reached", self.last_scan_pages, len(out))
                    return slot, out
        res = await self.call("getProgramAccounts", [program, {**cfg, "withContext": True}])
        slot = int(res["context"]["slot"])
        for item in res["value"]:
            a = item["account"]
            out.append(AccountUpdate(item["pubkey"], a["owner"], base64.b64decode(a["data"][0]), slot, source="rpc"))
        return slot, out


def position_filters(wallet: str | None) -> list[dict[str, Any]]:
    f: list[dict[str, Any]] = [{"memcmp": {"offset": 0, "bytes": b58encode(POSITION_DISCRIMINATOR)}}]
    if wallet:
        f.append({"memcmp": {"offset": POSITION_OWNER_OFFSET, "bytes": wallet}})
    return f


async def fetch_wallet_positions(rpc: RpcClient, wallet: str, commitment: str = "confirmed") -> tuple[int, list[AccountUpdate]]:
    return await rpc.get_program_accounts(JUPITER_PERPS_PROGRAM, position_filters(wallet), commitment)


async def fetch_recent_positions(rpc: RpcClient, *, since_slots: int = 216_000, max_pages: int = 30,
                                 time_budget_s: float = 15.0, commitment: str = "confirmed") -> tuple[int, list[AccountUpdate]]:
    """Position accounts touched in the last `since_slots` (default ≈ 24 h at 400 ms/slot), for the
    `demo` picker. The Perps program holds > 400,000 Position accounts (closed ones are never
    deleted), so a full scan is 400 pages; `changedSinceSlot` brings it down to a few pages
    (2026-09-24: 24 h → 2,594 accounts, 3 pages, 0.7 s on Solami). Bounded regardless."""
    head = await rpc.get_slot(commitment)
    return await rpc.get_program_accounts(
        JUPITER_PERPS_PROGRAM, position_filters(None), commitment,
        changed_since_slot=max(1, head - since_slots) if rpc.is_solami else None,
        max_pages=max_pages, time_budget_s=time_budget_s,
    )


class RpcPollingTransport(Transport):
    name = "rpc"

    def __init__(self, rpc: RpcClient, wallet: str, *, interval_ms: int = 1500, commitment: str = "processed",
                 rescan_every_s: float = 30.0, extra_accounts: list[str] | None = None, source: str = "rpc",
                 metrics: Any | None = None):
        self.rpc = rpc
        self.wallet = wallet
        self.interval = max(interval_ms, 200) / 1000
        self.commitment = commitment
        self.rescan_every = rescan_every_s
        self.extra_accounts = list(extra_accounts or [])
        self.position_keys: list[str] = []
        self.bootstrapped = False
        self.bootstrap_slot = 0
        self.source = source
        self.metrics = metrics
        self.polls = 0
        self.errors = 0
        self._stop = asyncio.Event()

    async def bootstrap(self, handler: UpdateHandler) -> int:
        """Complete snapshot over RPC: the wallet's Position accounts, then the extra accounts (custodies,
        oracles). Returns the context slot of the position scan — the streaming transports replay from it.
        Also used by the guard to (re-)bootstrap the gRPC/Mirage paths."""
        slot, positions = await fetch_wallet_positions(self.rpc, self.wallet, "confirmed")
        self.position_keys = [p.pubkey for p in positions]
        for p in positions:
            await handler(AccountUpdate(p.pubkey, p.owner, p.data, p.slot, source="bootstrap"))
        extras: list = []
        if self.extra_accounts:
            _, extras = await self.rpc.get_multiple_accounts(self.extra_accounts, "confirmed")
            for u in extras:
                if u is not None:
                    await handler(AccountUpdate(u.pubkey, u.owner, u.data, u.slot, source="bootstrap"))
        await handler(SlotUpdate(slot, "confirmed", source="bootstrap"))
        self.bootstrapped, self.bootstrap_slot = True, slot
        log.info("bootstrap: %d position accounts, %d extra accounts, slot %d", len(positions), len(extras), slot)
        return slot

    async def run(self, handler: UpdateHandler) -> None:
        """Poll until closed. RPC failures (429 bursts, outages) are logged, counted and retried with a
        backoff — the polling transport must survive them just like the streaming ones do."""
        backoff = 1.0
        last_rescan = time.monotonic()
        while not self._stop.is_set():
            t0 = time.monotonic()
            try:
                if not self.bootstrapped:
                    await self.bootstrap(handler)
                keys = self.position_keys + self.extra_accounts
                if keys:
                    slot, updates = await self.rpc.get_multiple_accounts(keys, self.commitment)
                    self.polls += 1
                    for u in updates:
                        if u is not None:
                            await handler(AccountUpdate(u.pubkey, u.owner, u.data, u.slot, source=self.source))
                    await handler(SlotUpdate(slot, self.commitment, source=self.source))
                if time.monotonic() - last_rescan >= self.rescan_every:
                    _, positions = await fetch_wallet_positions(self.rpc, self.wallet, "confirmed")
                    self.position_keys = [p.pubkey for p in positions]
                    last_rescan = time.monotonic()
                backoff = 1.0
                wait = max(0.0, self.interval - (time.monotonic() - t0))
            except PermissionError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — RuntimeError after retries, decode of a bad body, …
                self.errors += 1
                if self.metrics:
                    self.metrics.on_error(f"rpc-poll: {str(exc)[:120]}")
                log.warning("RPC poll failed (%s) — retrying in %.0fs", exc, backoff)
                wait, backoff = backoff, min(backoff * 2, 30.0)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass

    async def close(self) -> None:
        self._stop.set()


def custody_pubkeys() -> list[str]:
    return list(CUSTODIES.values())


__all__ = ["RpcClient", "RpcError", "RpcPollingTransport", "fetch_wallet_positions", "fetch_recent_positions",
           "position_filters", "custody_pubkeys", "POSITION_ACCOUNT_SIZE"]
