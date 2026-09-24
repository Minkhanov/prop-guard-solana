"""Data-path transports. All three deliver the same `AccountUpdate` / `SlotUpdate`
objects to the engine, so the risk logic never knows which wire it came from:

- rpc     : Solami JSON-RPC polling (works on the Free plan, 5 rps)
- grpc    : Solami Yellowstone gRPC (Pro+), lowest latency, server-side filters
- mirage  : the same Yellowstone `SubscribeUpdate` frames over a plain WebSocket
"""
from .base import AccountUpdate, SlotUpdate, Transport, UpdateHandler

__all__ = ["AccountUpdate", "SlotUpdate", "Transport", "UpdateHandler"]
