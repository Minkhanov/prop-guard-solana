"""Solami Yellowstone gRPC transport.

Endpoint: grpc.solami.dev:443 (or {ams|fra|nyc}.grpc.solami.dev), auth via the
`x-token` metadata header, replay with `from_slot` after a reconnect (Solami keeps
up to 3,500 slots ≈ 23 min). The protobuf stubs are generated from the vendored
Yellowstone protos and committed; regenerate with:

    python -m propguard.transports.yellowstone.gen

Filters (all scoped, so they fit a plan-included stream, not PAYG):
  positions : accounts owned by the Perps program, memcmp(discriminator) + memcmp(owner=wallet)
  custodies : the JLP custody accounts (fee / borrow parameters)
  oracles   : Doves price feeds referenced by the custodies (mark prices)
  slots     : slot heartbeat for lag / health metrics

Reconnect policy:
  * exponential backoff 1 → 30 s with jitter;
  * if the last seen slot is still inside the replay window, subscribe with
    `from_slot = last_slot - 1` (one slot of overlap; updates are idempotent);
  * if the gap is larger than the window (or the server rejects `from_slot`),
    the transport asks the guard to re-bootstrap the account set over RPC and
    subscribes from the head. Nothing is silently skipped either way.
"""
from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Awaitable, Callable

from ..constants import GRPC_REPLAY_MAX_SLOTS, JUPITER_PERPS_PROGRAM, POSITION_OWNER_OFFSET
from ..decode.borsh import b58decode, b58encode
from ..decode.jupiter import POSITION_DISCRIMINATOR
from .base import AccountUpdate, SlotUpdate, Transport, UpdateHandler

log = logging.getLogger("propguard.grpc")

REPLAY_SAFETY_MARGIN = 300          # slots we leave unused at the far end of the replay window
BACKOFF_MAX_S = 30.0


def _load_stubs():
    try:
        from .yellowstone import geyser_pb2, geyser_pb2_grpc  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Yellowstone protobuf stubs are missing. Generate them with:\n"
            "  pip install grpcio-tools && python -m propguard.transports.yellowstone.gen"
        ) from exc
    return geyser_pb2, geyser_pb2_grpc


def build_subscribe_request(wallet: str, custodies: list[str], oracles: list[str], commitment: str,
                            from_slot: int | None = None) -> Any:
    geyser_pb2, _ = _load_stubs()
    level = {"processed": geyser_pb2.PROCESSED, "confirmed": geyser_pb2.CONFIRMED,
             "finalized": geyser_pb2.FINALIZED}[commitment]
    req = geyser_pb2.SubscribeRequest(commitment=level)
    pos = req.accounts["positions"]
    pos.owner.append(JUPITER_PERPS_PROGRAM)
    f1 = pos.filters.add(); f1.memcmp.offset = 0; f1.memcmp.bytes = bytes(POSITION_DISCRIMINATOR)
    f2 = pos.filters.add(); f2.memcmp.offset = POSITION_OWNER_OFFSET; f2.memcmp.bytes = b58decode(wallet)
    req.accounts["custodies"].account.extend(custodies)
    req.accounts["oracles"].account.extend(oracles)
    req.slots["slots"].filter_by_commitment = True
    # No `ping` here: Yellowstone treats a SubscribeRequest that carries `ping` as a keep-alive only
    # (replies with pong and skips filter installation). Pings are sent as separate requests in run().
    if from_slot:
        req.from_slot = from_slot
    return req


def _looks_like_from_slot_rejection(details: str) -> bool:
    d = (details or "").lower()
    return "from_slot" in d or "from slot" in d or "replay" in d or "too old" in d


class GrpcTransport(Transport):
    name = "grpc"

    def __init__(self, endpoint: str, token: str, wallet: str, custodies: list[str], oracles: list[str],
                 *, commitment: str = "processed", metrics: Any | None = None,
                 head_slot: Callable[[], Awaitable[int]] | None = None,
                 rebootstrap: Callable[[], Awaitable[None]] | None = None,
                 insecure: bool = False, tls_server_name: str = "", backoff_initial_s: float = 1.0,
                 backoff_max_s: float = BACKOFF_MAX_S):
        self.endpoint = endpoint
        self.token = token
        self.wallet = wallet
        self.custodies = custodies
        self.oracles = oracles
        self.commitment = commitment
        self.metrics = metrics
        self.head_slot = head_slot          # async () -> current RPC head slot (for the replay-window check)
        self.rebootstrap = rebootstrap      # async () -> None: re-fetch positions/oracles over RPC
        self.insecure = insecure            # plaintext channel (tests / local mocks only)
        self.tls_server_name = tls_server_name   # SNI / cert-name override when going through a passthrough proxy
        self.backoff_initial_s = backoff_initial_s
        self.backoff_max_s = backoff_max_s
        self.last_slot: int | None = None
        self.last_from_slot: int | None = None
        self._force_rebootstrap = False
        self._stop = asyncio.Event()
        self.connected = asyncio.Event()

    # ---- replay decision -------------------------------------------------------------
    async def _plan_reconnect(self) -> int | None:
        """Decide `from_slot` for the next subscription. Returns None when starting from the head."""
        if not self.last_slot:
            return None
        if self._force_rebootstrap:
            reason = "server rejected from_slot"
        else:
            head = None
            if self.head_slot:
                try:
                    head = await self.head_slot()
                except Exception as exc:  # noqa: BLE001 — RPC hiccup: replay optimistically
                    log.warning("cannot read RPC head for the replay check (%s); replaying anyway", exc)
            gap = (head - self.last_slot) if head else 0
            if gap <= GRPC_REPLAY_MAX_SLOTS - REPLAY_SAFETY_MARGIN:
                if gap > 0:
                    log.info("replaying %d slots via from_slot", gap)
                return max(self.last_slot - 1, 1)
            reason = f"gap {gap} slots exceeds the replay window ({GRPC_REPLAY_MAX_SLOTS})"
        log.warning("%s — re-bootstrapping over RPC and subscribing from the head", reason)
        self._force_rebootstrap = False
        self.last_slot = None
        if self.rebootstrap:
            await self.rebootstrap()
        if self.metrics:
            self.metrics.rebootstraps += 1
        return None

    def _channel(self, grpc):
        options = [("grpc.max_receive_message_length", 64 * 1024 * 1024),
                   ("grpc.keepalive_time_ms", 15000), ("grpc.keepalive_timeout_ms", 5000)]
        if self.insecure:
            return grpc.aio.insecure_channel(self.endpoint, options=options)
        if self.tls_server_name:
            options.append(("grpc.ssl_target_name_override", self.tls_server_name))
        return grpc.aio.secure_channel(self.endpoint, grpc.ssl_channel_credentials(), options=options)

    # ---- main loop -------------------------------------------------------------------------
    async def run(self, handler: UpdateHandler) -> None:
        import grpc  # local import: optional dependency at runtime for rpc-only users
        geyser_pb2, geyser_pb2_grpc = _load_stubs()
        backoff = self.backoff_initial_s
        first = True
        while not self._stop.is_set():
            if not first:
                if self.metrics:
                    self.metrics.on_reconnect(replayed=bool(self.last_slot) and not self._force_rebootstrap)
                await asyncio.sleep(backoff + random.uniform(0, backoff / 2))
                backoff = min(backoff * 2, self.backoff_max_s)
                if self._stop.is_set():
                    break
            first = False
            from_slot = await self._plan_reconnect()
            self.last_from_slot = from_slot
            request = build_subscribe_request(self.wallet, self.custodies, self.oracles, self.commitment, from_slot)
            outbound: asyncio.Queue = asyncio.Queue()
            await outbound.put(request)

            async def requests():
                while True:
                    item = await outbound.get()
                    if item is None:
                        return
                    yield item

            try:
                async with self._channel(grpc) as channel:
                    stub = geyser_pb2_grpc.GeyserStub(channel)
                    log.info("gRPC subscribe %s (commitment=%s, from_slot=%s)", self.endpoint, self.commitment, from_slot)
                    stream = stub.Subscribe(requests(), metadata=(("x-token", self.token),))
                    async for update in stream:
                        kind = update.WhichOneof("update_oneof")
                        if kind in ("ping", "pong"):
                            if self.metrics:
                                self.metrics.on_ping(update.ByteSize())
                            if kind == "ping":
                                await outbound.put(geyser_pb2.SubscribeRequest(ping=geyser_pb2.SubscribeRequestPing(id=1)))
                            continue
                        if not self.connected.is_set():
                            self.connected.set()
                            if self.metrics:
                                self.metrics.on_connected()
                        backoff = self.backoff_initial_s
                        if self.metrics:
                            self.metrics.on_message(update.ByteSize(), list(update.filters))
                        if kind == "account":
                            acc = update.account.account
                            self.last_slot = max(self.last_slot or 0, update.account.slot)
                            await handler(AccountUpdate(
                                b58encode(acc.pubkey), b58encode(acc.owner), bytes(acc.data), update.account.slot,
                                acc.write_version, source="grpc", filter_name=(update.filters[0] if update.filters else "")))
                        elif kind == "slot":
                            self.last_slot = max(self.last_slot or 0, update.slot.slot)
                            await handler(SlotUpdate(update.slot.slot, geyser_pb2.SlotStatus.Name(update.slot.status).lower(), source="grpc"))
                    # server closed the stream cleanly: treat as a disconnect
                    log.warning("gRPC stream ended by the server — reconnecting")
            except grpc.aio.AioRpcError as exc:
                code, details = exc.code(), exc.details() or ""
                if self.metrics:
                    self.metrics.on_error(f"grpc {code.name}: {details[:120]}")
                if code == grpc.StatusCode.UNAUTHENTICATED:
                    raise PermissionError("Solami gRPC: unauthenticated — check SOLAMI_GRPC_KEY (Pro plan+)") from exc
                if code == grpc.StatusCode.PERMISSION_DENIED:   # plan has no gRPC (e.g. trial ended)
                    raise PermissionError(f"Solami gRPC: permission denied — {details}") from exc
                if code == grpc.StatusCode.INVALID_ARGUMENT and from_slot and _looks_like_from_slot_rejection(details):
                    self._force_rebootstrap = True
                log.warning("gRPC stream error %s: %s — reconnecting in ~%.0fs", code.name, details[:200], backoff)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — anything else is a transport failure, keep the guard alive
                if self.metrics:
                    self.metrics.on_error(f"grpc {type(exc).__name__}: {str(exc)[:120]}")
                log.warning("gRPC transport failure %s: %s — reconnecting in ~%.0fs", type(exc).__name__, exc, backoff)
            finally:
                self.connected.clear()
                await outbound.put(None)

    async def close(self) -> None:
        self._stop.set()


def describe_filters(wallet: str, custodies: list[str], oracles: list[str]) -> dict[str, Any]:
    """Human-readable filter summary for the panel / README."""
    return {
        "positions": {"owner": JUPITER_PERPS_PROGRAM, "memcmp": [{"offset": 0, "bytes": POSITION_DISCRIMINATOR.hex()},
                                                                  {"offset": POSITION_OWNER_OFFSET, "bytes": wallet}]},
        "custodies": {"account": custodies},
        "oracles": {"account": oracles},
        "slots": {"filter_by_commitment": True},
    }


__all__ = ["GrpcTransport", "build_subscribe_request", "describe_filters", "REPLAY_SAFETY_MARGIN"]
