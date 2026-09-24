"""Guard-level resilience: a stream that goes silent triggers the RPC fallback, the fallback polls the
account set, and it is stopped as soon as the stream delivers again. No network: the stream is a fake
transport, RPC is an httpx mock."""
from __future__ import annotations

import asyncio
import base64
import json
import struct
import time

import httpx

from propguard.config import Settings
from propguard.guard import Guard
from propguard.transports.base import AccountUpdate, SlotUpdate, Transport

OWNER = "815wh1VLC8D7ZWMa3uB7GgjHFetANuML2dPrJiLV1nVF"


class FakeStream(Transport):
    """Delivers oracle updates on demand; silent otherwise. Like the real transports it reports
    every data frame to the metrics itself (the guard only sees decoded updates)."""
    name = "grpc"

    def __init__(self, metrics):
        self.metrics = metrics
        self.handler = None
        self.gate = asyncio.Event()
        self.data = None
        self.slot = 450_000_100

    async def run(self, handler):
        self.handler = handler
        while True:
            await self.gate.wait(); self.gate.clear()
            self.slot += 1
            self.metrics.on_message(len(self.data[2]), ["oracles"])
            await handler(AccountUpdate(self.data[0], self.data[1], self.data[2], self.slot, source="grpc", filter_name="oracles"))
            self.metrics.on_message(16, ["slots"])
            await handler(SlotUpdate(self.slot, "processed", source="grpc"))

    def emit(self, pubkey, owner, data):
        self.data = (pubkey, owner, data)
        self.gate.set()


def _mock_rpc(mainnet_accounts, position_bytes, seen):
    from propguard.decode.jupiter import decode_custody
    cust_sol = decode_custody(mainnet_accounts["custody_sol"]["pubkey"], mainnet_accounts["custody_sol"]["data"])
    by_key = {mainnet_accounts["custody_sol"]["pubkey"]: mainnet_accounts["custody_sol"],
              mainnet_accounts["custody_usdc"]["pubkey"]: mainnet_accounts["custody_usdc"],
              cust_sol["dovesAgOracle"]: mainnet_accounts["doves_ag_sol"]}
    pos_pk = mainnet_accounts["position_FBLz"]["pubkey"]

    def enc(fx, data=None):
        return {"owner": fx["owner"], "data": [base64.b64encode(data or fx["data"]).decode(), "base64"], "lamports": 1,
                "executable": False, "rentEpoch": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read()); m, params = body["method"], body.get("params", [])
        seen.setdefault(m, 0); seen[m] += 1
        if m == "getSlot":
            res = 450_000_000
        elif m == "getMultipleAccounts":
            res = {"context": {"slot": 450_000_001}, "value": [enc(by_key[k]) if k in by_key else None for k in params[0]]}
        elif m == "getProgramAccountsV2":
            res = {"context": {"slot": 450_000_002}, "value": {"accounts": [{"pubkey": pos_pk, "account": enc(mainnet_accounts["position_FBLz"], position_bytes)}], "paginationKey": None}}
        else:
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32601, "message": "nope"}})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": res})

    return handler, cust_sol


async def test_fallback_kicks_in_on_silence_and_hands_back(mainnet_accounts, tmp_path):
    from propguard.decode.jupiter import decode_custody
    cust = decode_custody(mainnet_accounts["custody_sol"]["pubkey"], mainnet_accounts["custody_sol"]["data"])
    b = bytearray(mainnet_accounts["position_FBLz"]["data"])
    b[152] = 1; struct.pack_into("<Q", b, 153, 100_000_000); struct.pack_into("<Q", b, 161, 1_000_000_000); struct.pack_into("<Q", b, 169, 200_000_000)
    b[185:201] = cust["fundingRateState"]["cumulativeInterestRate"].to_bytes(16, "little")
    seen: dict = {}
    handler, _cust_sol = _mock_rpc(mainnet_accounts, bytes(b), seen)

    s = Settings(solami_api_key="k", wallet=OWNER, transport="grpc", rpc_rps=10_000, fallback_silence_sec=1,
                 fallback_poll_interval_ms=200, stream_stale_sec=1, alerts_dry_run=True, alert_cooldown_min=0,
                 state_file=str(tmp_path / "state.json"), health_log_file=str(tmp_path / "health.jsonl"), health_log_every_sec=1)
    g = Guard(s)
    g.rpc._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    stream = FakeStream(g.metrics)

    async def build():   # same steps as Guard.build_transport, but the stream is the fake
        _, cu = await g.rpc.get_multiple_accounts([mainnet_accounts["custody_sol"]["pubkey"], mainnet_accounts["custody_usdc"]["pubkey"]], "confirmed")
        for u in cu:
            if u:
                await g.on_update(AccountUpdate(u.pubkey, u.owner, u.data, u.slot, source="bootstrap"))
        await g.bootstrap_accounts()
        return stream

    g.build_transport = build  # type: ignore[method-assign]
    task = asyncio.create_task(g.run(with_panel=False))
    try:
        await asyncio.sleep(0.3)
        assert g.ready and len(g.state.snapshot().positions) == 1
        ag = mainnet_accounts["doves_ag_sol"]
        stream.emit(ag["pubkey"], ag["owner"], ag["data"])              # one data frame, then silence
        await asyncio.sleep(0.3)
        assert g.metrics.messages_total == 2 and not g.metrics.fallback_active
        # ... silence > 1 s -> fallback
        for _ in range(40):
            await asyncio.sleep(0.1)
            if g.metrics.fallback_active:
                break
        assert g.metrics.fallback_active and g.metrics.active_path == "rpc-fallback" and g.metrics.fallback_count == 1
        await asyncio.sleep(0.7)
        assert g.fallback is not None and g.fallback.polls >= 2, "fallback must poll the account set"
        assert seen["getMultipleAccounts"] >= 3
        codes = [a.code for a in g.router.history]
        assert "fallback_on" in codes and "stream_stale" in codes
        # stream resumes -> fallback stops
        stream.emit(ag["pubkey"], ag["owner"], ag["data"])
        for _ in range(40):
            await asyncio.sleep(0.1)
            if not g.metrics.fallback_active:
                break
        assert not g.metrics.fallback_active and g.fallback is None and g.metrics.active_path == "grpc"
        assert "fallback_off" in [a.code for a in g.router.history]
        # health journal wrote records, and the payload carries anchor + alerting info
        await asyncio.sleep(1.1)
        lines = (tmp_path / "health.jsonl").read_text(encoding="utf-8").splitlines()
        rec = json.loads(lines[-1])
        # (the fake stream is silent again by now, so a second fallback may already be running — that is correct)
        assert rec["transport"] == "grpc" and rec["fallback_count"] >= 1 and rec["rpc_calls"] > 0
        assert (tmp_path / "health.jsonl").read_text(encoding="utf-8").count("api_key") == 0
        p = g.payload()
        assert p["rules"]["anchor_source"] == "first_observation" and p["alerting"]["telegram"]["mode"] == "dry-run"
        assert p["health"]["update_age_sec"] < 5
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    _ = time
