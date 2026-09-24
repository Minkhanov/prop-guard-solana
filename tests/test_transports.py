"""Transport tests: the integration layer that broke on 2026-09-24 (install, RPC auth, gRPC stubs,
subscribe request) plus reconnect / replay / re-bootstrap on a mock Yellowstone server, the RPC
polling transport on an httpx mock, Mirage on a local WebSocket server, and the demo picker."""
from __future__ import annotations

import asyncio
import base64
import json
import struct
import time

import httpx
import pytest

from propguard.constants import CUSTODIES
from propguard.transports.base import AccountUpdate, SlotUpdate
from propguard.transports.rpc import RpcClient, RpcError, RpcPollingTransport

WALLET = "8Tpcd7K3b1zkgCD4EJTK8pcijVEx6NJ7C9wV6oyQRpNP"
FIXTURE_OWNER = "815wh1VLC8D7ZWMa3uB7GgjHFetANuML2dPrJiLV1nVF"


def test_yellowstone_stubs_import():
    from propguard.transports.grpc_yellowstone import _load_stubs
    geyser_pb2, geyser_pb2_grpc = _load_stubs()
    assert hasattr(geyser_pb2, "SubscribeRequest") and hasattr(geyser_pb2_grpc, "GeyserStub")


def test_subscribe_request_has_four_filters_and_no_ping():
    from propguard.transports.grpc_yellowstone import build_subscribe_request
    req = build_subscribe_request(WALLET, ["7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz"],
                                  ["FYq2BWQ1V5P1WFBqr3qB2Kb5yHVvSv7upzKodgQE5zXh"], "processed", from_slot=123)
    assert set(req.accounts) == {"positions", "custodies", "oracles"} and set(req.slots) == {"slots"}
    assert len(req.accounts["positions"].filters) == 2 and req.accounts["positions"].filters[1].memcmp.offset == 8
    assert not req.HasField("ping"), "a SubscribeRequest carrying `ping` is answered with pong only; filters are not installed"
    assert req.from_slot == 123


# ---- RPC client ---------------------------------------------------------------------------------

async def test_rpc_key_goes_to_query_param_and_is_redacted():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url); seen["auth"] = request.headers.get("authorization")
        return httpx.Response(500, text="boom")

    rpc = RpcClient("https://rpc.solami.dev/sol", "sk_secret123", rps=1000)
    rpc._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(RuntimeError) as ei:
        await rpc.call("getSlot", retries=0)
    assert seen["url"] == "https://rpc.solami.dev/sol?api_key=sk_secret123" and seen["auth"] is None
    assert "sk_secret123" not in str(ei.value) and "***" in str(ei.value)
    await rpc.close()


async def test_rpc_does_not_retry_deterministic_errors():
    n = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        n["calls"] += 1
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "Invalid params: bad"}})

    rpc = RpcClient("https://rpc.solami.dev/sol", "k", rps=1000)
    rpc._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    t0 = time.monotonic()
    with pytest.raises(RpcError) as ei:
        await rpc.call("getProgramAccountsV2", retries=4)
    assert n["calls"] == 1 and ei.value.code == -32602 and time.monotonic() - t0 < 0.5
    await rpc.close()


def _open_position_bytes(closed_fixture: bytes, custody: dict, *, size_usd: float, coll_usd: float, entry: float) -> bytes:
    """Turn the closed mainnet Position fixture into an open SOL long with a fresh interest snapshot."""
    b = bytearray(closed_fixture)
    b[152] = 1                                                     # side long
    struct.pack_into("<Q", b, 153, int(entry * 1e6))               # price
    struct.pack_into("<Q", b, 161, int(size_usd * 1e6))            # sizeUsd
    struct.pack_into("<Q", b, 169, int(coll_usd * 1e6))            # collateralUsd
    snap = custody["fundingRateState"]["cumulativeInterestRate"]
    b[185:201] = snap.to_bytes(16, "little")                       # cumulativeInterestSnapshot (u128)
    struct.pack_into("<q", b, 136, int(time.time()) - 3600)        # openTime
    return bytes(b)


def _rpc_mock(mainnet_accounts, position_bytes: bytes, *, seen: dict, changed_since_required: bool = False):
    """A JSON-RPC mock that serves the fixture accounts (custodies, oracle) and one Position."""
    from propguard.decode.jupiter import decode_custody
    cust_sol = decode_custody(mainnet_accounts["custody_sol"]["pubkey"], mainnet_accounts["custody_sol"]["data"])
    ag_key = cust_sol["dovesAgOracle"]
    by_key = {
        mainnet_accounts["custody_sol"]["pubkey"]: mainnet_accounts["custody_sol"],
        mainnet_accounts["custody_usdc"]["pubkey"]: mainnet_accounts["custody_usdc"],
        ag_key: mainnet_accounts["doves_ag_sol"],
    }
    pos_pk = mainnet_accounts["position_FBLz"]["pubkey"]

    def enc(fx, data=None):
        return {"owner": fx["owner"], "data": [base64.b64encode(data or fx["data"]).decode(), "base64"], "lamports": 1,
                "executable": False, "rentEpoch": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        m, params = body["method"], body.get("params", [])
        seen.setdefault(m, []).append(params)
        if m == "getSlot":
            res = 450_000_000
        elif m == "getMultipleAccounts":
            res = {"context": {"slot": 450_000_001}, "value": [enc(by_key[k]) if k in by_key else None for k in params[0]]}
        elif m == "getProgramAccountsV2":
            if changed_since_required:
                assert "changedSinceSlot" in params[1] and params[1]["limit"] == 1000
            res = {"context": {"slot": 450_000_002},
                   "value": {"accounts": [{"pubkey": pos_pk, "account": enc(mainnet_accounts["position_FBLz"], position_bytes)}],
                             "paginationKey": None}}
        else:
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32601, "message": "nope"}})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": res})

    return handler, pos_pk


async def test_rpc_polling_transport_bootstraps_and_polls(mainnet_accounts):
    from propguard.decode.jupiter import decode_custody
    cust = decode_custody(mainnet_accounts["custody_sol"]["pubkey"], mainnet_accounts["custody_sol"]["data"])
    pos = _open_position_bytes(mainnet_accounts["position_FBLz"]["data"], cust, size_usd=1000, coll_usd=200, entry=100)
    seen: dict = {}
    handler, pos_pk = _rpc_mock(mainnet_accounts, pos, seen=seen)
    rpc = RpcClient("https://rpc.solami.dev/sol", "k", rps=10_000)
    rpc._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    got: list = []

    async def on_update(u):
        got.append(u)

    t = RpcPollingTransport(rpc, FIXTURE_OWNER, interval_ms=200, extra_accounts=[CUSTODIES["SOL"], cust["dovesAgOracle"]])
    task = asyncio.create_task(t.run(on_update))
    await asyncio.sleep(0.7)
    await t.close(); await task
    boot = [u for u in got if u.source == "bootstrap"]
    polls = [u for u in got if isinstance(u, SlotUpdate) and u.source == "rpc"]
    assert any(isinstance(u, AccountUpdate) and u.pubkey == pos_pk for u in boot)
    assert len(polls) >= 2 and t.polls >= 2, "poll loop must keep going until closed"
    assert seen["getMultipleAccounts"][-1][0] == [pos_pk, CUSTODIES["SOL"], cust["dovesAgOracle"]]
    await rpc.close()


async def test_demo_picker_uses_changed_since_slot_and_skips_dead_positions(mainnet_accounts, monkeypatch):
    from propguard.cli import pick_demo_wallet
    from propguard.config import Settings
    from propguard.decode.jupiter import decode_custody, decode_doves_price_feed
    cust = decode_custody(mainnet_accounts["custody_sol"]["pubkey"], mainnet_accounts["custody_sol"]["data"])
    oracle = decode_doves_price_feed(mainnet_accounts["doves_ag_sol"]["pubkey"], mainnet_accounts["doves_ag_sol"]["data"])
    now = oracle.timestamp + 2                                      # freeze "now" so the fixture oracle counts as fresh
    mark = oracle.price_usd
    s = Settings(solami_api_key="k", rpc_rps=10_000, demo_scan_hours=24)
    current = {}
    orig = httpx.AsyncClient

    class Patched(orig):
        def __init__(self, *a, **kw):
            kw["transport"] = httpx.MockTransport(current["handler"]); super().__init__(*a, **kw)

    monkeypatch.setattr(httpx, "AsyncClient", Patched)
    # 1) a healthy 5x long -> picked
    seen: dict = {}
    current["handler"], _ = _rpc_mock(mainnet_accounts, _open_position_bytes(mainnet_accounts["position_FBLz"]["data"], cust,
                                                                             size_usd=1000, coll_usd=200, entry=mark),
                                      seen=seen, changed_since_required=True)
    r = await pick_demo_wallet(s, now=now)
    assert r["wallet"] == FIXTURE_OWNER and r["market"] == "SOL" and r["liq_distance_pct"] > 3
    assert seen["getProgramAccountsV2"][0][1]["changedSinceSlot"] == 450_000_000 - int(24 * 3600 / 0.4)
    # 2) the same position already past liquidation (entry far above mark) -> no live candidate
    current["handler"], _ = _rpc_mock(mainnet_accounts, _open_position_bytes(mainnet_accounts["position_FBLz"]["data"], cust,
                                                                             size_usd=1000, coll_usd=200, entry=mark * 1.5), seen={})
    with pytest.raises(SystemExit):
        await pick_demo_wallet(s, now=now)


# ---- gRPC on a mock Yellowstone server ---------------------------------------------------------

async def test_grpc_transport_reconnects_with_replay_and_answers_pings():
    grpc = pytest.importorskip("grpc")
    from propguard.engine.metrics import StreamMetrics
    from propguard.transports.grpc_yellowstone import GrpcTransport
    from propguard.transports.yellowstone import geyser_pb2, geyser_pb2_grpc

    requests_seen: list = []
    pongs: list = []

    class Servicer(geyser_pb2_grpc.GeyserServicer):
        async def Subscribe(self, request_iterator, context):
            first = await request_iterator.__anext__()
            requests_seen.append(first)
            n = len(requests_seen)
            # server-side ping first: the client must answer with a ping request
            yield geyser_pb2.SubscribeUpdate(ping=geyser_pb2.SubscribeUpdatePing())
            reply = await asyncio.wait_for(request_iterator.__anext__(), 2)
            pongs.append(reply.HasField("ping"))
            base = 1_000 * n
            acc = geyser_pb2.SubscribeUpdateAccount(slot=base + 1, is_startup=False)
            acc.account.pubkey = b"\x01" * 32; acc.account.owner = b"\x02" * 32; acc.account.data = b"\xaa" * 16
            acc.account.write_version = 7
            yield geyser_pb2.SubscribeUpdate(filters=["oracles"], account=acc)
            yield geyser_pb2.SubscribeUpdate(filters=["slots"], slot=geyser_pb2.SubscribeUpdateSlot(slot=base + 5, status=geyser_pb2.SLOT_PROCESSED))
            if n == 1:
                return                          # simulate a server-side drop -> client reconnects
            await asyncio.sleep(5)              # second connection stays open until the test cancels

    server = grpc.aio.server()
    geyser_pb2_grpc.add_GeyserServicer_to_server(Servicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    got: list = []

    async def on_update(u):
        got.append(u)

    m = StreamMetrics(transport="grpc")
    t = GrpcTransport(f"127.0.0.1:{port}", "tok", WALLET, [CUSTODIES["SOL"]], ["FYq2BWQ1V5P1WFBqr3qB2Kb5yHVvSv7upzKodgQE5zXh"],
                      metrics=m, insecure=True, backoff_initial_s=0.05, head_slot=None)
    task = asyncio.create_task(t.run(on_update))
    try:
        for _ in range(100):
            await asyncio.sleep(0.05)
            if len(requests_seen) >= 2 and len(got) >= 4:
                break
        assert len(requests_seen) >= 2, "client must reconnect after the server dropped the stream"
        assert requests_seen[0].from_slot == 0
        assert requests_seen[1].from_slot == 1_005 - 1, "replay must resume one slot before the last seen slot"
        assert pongs == [True, True], "server pings must be answered with a ping request"
        accs = [u for u in got if isinstance(u, AccountUpdate)]
        assert accs[0].source == "grpc" and accs[0].filter_name == "oracles" and accs[0].write_version == 7 and accs[0].data == b"\xaa" * 16
        assert m.reconnects == 1 and m.replays == 1 and m.pings_total >= 2 and m.messages_total >= 4
        assert m.by_filter == {"oracles": 2, "slots": 2}
    finally:
        await t.close(); task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        await server.stop(None)


async def test_grpc_gap_beyond_replay_window_rebootstraps():
    from propguard.engine.metrics import StreamMetrics
    from propguard.transports.grpc_yellowstone import GrpcTransport
    calls = {"boot": 0}

    async def head():
        return 100_000

    async def reboot():
        calls["boot"] += 1

    m = StreamMetrics(transport="grpc")
    t = GrpcTransport("x:1", "tok", WALLET, [], [], metrics=m, head_slot=head, rebootstrap=reboot)
    t.last_slot = 99_000                       # 1,000 slots behind: inside the 3,500-slot window
    assert await t._plan_reconnect() == 98_999 and calls["boot"] == 0
    t.last_slot = 90_000                       # 10,000 behind: outside -> re-bootstrap, subscribe from head
    assert await t._plan_reconnect() is None and calls["boot"] == 1 and t.last_slot is None and m.rebootstraps == 1
    t.last_slot = 99_990; t._force_rebootstrap = True    # server said the from_slot is not replayable
    assert await t._plan_reconnect() is None and calls["boot"] == 2


# ---- Mirage on a local WebSocket server ------------------------------------------------------------

async def test_mirage_transport_decodes_frames_and_redacts_key():
    websockets = pytest.importorskip("websockets")
    from propguard.engine.metrics import StreamMetrics
    from propguard.transports.mirage import MirageTransport
    from propguard.transports.yellowstone import geyser_pb2
    paths: list = []

    async def serve(ws):
        paths.append(ws.request.path)
        acc = geyser_pb2.SubscribeUpdateAccount(slot=42)
        acc.account.pubkey = b"\x03" * 32; acc.account.owner = b"\x04" * 32; acc.account.data = b"\xbb" * 8
        await ws.send(geyser_pb2.SubscribeUpdate(filters=["custodies"], account=acc).SerializeToString())
        await ws.send(geyser_pb2.SubscribeUpdate(filters=["slots"], slot=geyser_pb2.SubscribeUpdateSlot(slot=43)).SerializeToString())
        await ws.send(geyser_pb2.SubscribeUpdate(ping=geyser_pb2.SubscribeUpdatePing()).SerializeToString())
        await asyncio.sleep(0.3)

    async with websockets.serve(serve, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        got: list = []

        async def on_update(u):
            got.append(u)

        m = StreamMetrics(transport="mirage")
        t = MirageTransport(f"ws://127.0.0.1:{port}/mirage/stream/sub1", "sk_mirage_secret", metrics=m, backoff_max_s=0.05)
        task = asyncio.create_task(t.run(on_update))
        for _ in range(40):
            await asyncio.sleep(0.05)
            if len(got) >= 2:
                break
        await t.close(); task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    assert paths and paths[0] == "/mirage/stream/sub1?api_key=sk_mirage_secret"
    assert isinstance(got[0], AccountUpdate) and got[0].source == "mirage" and got[0].filter_name == "custodies" and got[0].slot == 42
    assert isinstance(got[1], SlotUpdate) and got[1].slot == 43
    assert m.messages_total == 2 and m.pings_total == 1
    assert "sk_mirage_secret" not in t._redact(f"error for {t.url}")
