from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable


@dataclass(frozen=True)
class AccountUpdate:
    pubkey: str
    owner: str
    data: bytes
    slot: int
    write_version: int = 0
    received_at: float = field(default_factory=time.time)
    source: str = ""          # "rpc" | "grpc" | "mirage" | "bootstrap"
    filter_name: str = ""     # gRPC/Mirage filter label that matched


@dataclass(frozen=True)
class SlotUpdate:
    slot: int
    status: str = "processed"
    received_at: float = field(default_factory=time.time)
    source: str = ""


UpdateHandler = Callable[[AccountUpdate | SlotUpdate], Awaitable[None]]


class Transport(abc.ABC):
    """A transport pushes updates to `handler` until cancelled."""

    name: str = "base"

    @abc.abstractmethod
    async def run(self, handler: UpdateHandler) -> None: ...

    async def close(self) -> None:  # optional
        return None
