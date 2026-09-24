"""Jupiter Perpetuals account decoders (Position, Custody) and Doves oracle feed.

Position is decoded by fixed offsets (fast path for the hot stream); Custody is
decoded through the vendored IDL because its layout is long and nested.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..constants import POSITION_ACCOUNT_SIZE, USD_DECIMALS
from .borsh import IdlDecoder, Reader, anchor_discriminator

IDL_PATH = Path(__file__).with_name("idl") / "jupiter-perpetuals-idl.json"

POSITION_DISCRIMINATOR = anchor_discriminator("account", "Position")
CUSTODY_DISCRIMINATOR = anchor_discriminator("account", "Custody")
DOVES_PRICE_FEED_DISCRIMINATOR = anchor_discriminator("account", "PriceFeed")
DOVES_AG_PRICE_FEED_DISCRIMINATOR = anchor_discriminator("account", "AgPriceFeed")

SIDE_NAMES = {0: "none", 1: "long", 2: "short"}


@dataclass(frozen=True)
class Position:
    pubkey: str
    owner: str
    pool: str
    custody: str
    collateral_custody: str
    open_time: int
    update_time: int
    side: str                 # "long" | "short" | "none"
    price: int                # entry price, 1e-6 USD
    size_usd: int             # 1e-6 USD, 0 == closed
    collateral_usd: int       # 1e-6 USD
    realised_pnl_usd: int     # 1e-6 USD, signed
    cumulative_interest_snapshot: int
    locked_amount: int
    bump: int
    data_len: int

    @property
    def is_open(self) -> bool:
        return self.size_usd > 0

    @property
    def leverage(self) -> float:
        return self.size_usd / self.collateral_usd if self.collateral_usd else 0.0

    def usd(self, field: str) -> float:
        return getattr(self, field) / 10**USD_DECIMALS


def decode_position(pubkey: str, data: bytes) -> Position:
    if data[:8] != POSITION_DISCRIMINATOR:
        raise ValueError(f"not a Position account: disc {data[:8].hex()}")
    if len(data) < 210:
        raise ValueError(f"Position account too short: {len(data)} bytes")
    r = Reader(data, 8)
    owner = r.pubkey(); pool = r.pubkey(); custody = r.pubkey(); coll = r.pubkey()
    open_time = r.i64(); update_time = r.i64()
    side = SIDE_NAMES.get(r.u8(), "none")
    price = r.u64(); size_usd = r.u64(); collateral_usd = r.u64()
    realised = r.i64(); cum = r.u128(); locked = r.u64(); bump = r.u8()
    return Position(pubkey, owner, pool, custody, coll, open_time, update_time, side, price,
                    size_usd, collateral_usd, realised, cum, locked, bump, len(data))


@dataclass(frozen=True)
class DovesPrice:
    pubkey: str
    pair: str
    price: int      # raw integer
    expo: int       # e.g. -8
    timestamp: int  # unix seconds

    @property
    def price_usd(self) -> float:
        return self.price * (10.0 ** self.expo)

    def price_1e6(self) -> int:
        """Price scaled to the program's 1e-6 USD fixed point (integer math)."""
        shift = self.expo + USD_DECIMALS
        return self.price * 10**shift if shift >= 0 else self.price // 10**(-shift)


def decode_doves_price_feed(pubkey: str, data: bytes) -> DovesPrice:
    """Doves oracle accounts (two layouts, both used by Jupiter custodies):

    PriceFeed   : pair[32] signer[33] price:u64 expo:i8 timestamp:i64 bump:u8 (+reserved)
    AgPriceFeed : mint[32] edgeFeed[32] clFeed[32] pythFeed[32] pythFeedId[32] price:u64 expo:i8 timestamp:i64 ...
    """
    disc = data[:8]
    r = Reader(data, 8)
    if disc == DOVES_PRICE_FEED_DISCRIMINATOR:
        pair = r.bytes(32).rstrip(b"\0").decode("ascii", "replace")
        r.bytes(33)
    elif disc == DOVES_AG_PRICE_FEED_DISCRIMINATOR:
        pair = "AG:" + r.pubkey()[:6]
        r.bytes(32 * 4)
    else:
        raise ValueError(f"not a Doves PriceFeed/AgPriceFeed account: disc {disc.hex()}")
    price = r.u64(); expo = r.i8(); ts = r.i64()
    return DovesPrice(pubkey, pair, price, expo, ts)


@lru_cache(maxsize=1)
def load_idl(path: Path = IDL_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def idl_decoder() -> IdlDecoder:
    return IdlDecoder(load_idl())


def decode_custody(pubkey: str, data: bytes) -> dict[str, Any]:
    """Full Custody account as nested dict (IDL-driven). Adds `pubkey`."""
    out = idl_decoder().decode_account("Custody", data)
    out["pubkey"] = pubkey
    return out


def is_position_account(data: bytes) -> bool:
    return len(data) >= 210 and data[:8] == POSITION_DISCRIMINATOR


def position_size_hint() -> int:
    return POSITION_ACCOUNT_SIZE
