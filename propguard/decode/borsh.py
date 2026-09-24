"""Minimal Borsh reader and an IDL-driven struct decoder.

Covers what Anchor (legacy IDL, `publicKey` type names) needs for reading
Jupiter Perps accounts: integers, bool, publicKey, fixed arrays, vec, option,
string, defined structs and unit-variant enums. Anything else raises.
"""
from __future__ import annotations

import hashlib
import struct
from typing import Any

from ..constants import B58_ALPHABET


def b58encode(raw: bytes) -> str:
    n = int.from_bytes(raw, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = B58_ALPHABET[r] + out
    pad = len(raw) - len(raw.lstrip(b"\0"))
    return "1" * pad + out


def b58decode(text: str) -> bytes:
    n = 0
    for ch in text:
        n = n * 58 + B58_ALPHABET.index(ch)
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(text) - len(text.lstrip("1"))
    return b"\0" * pad + raw


def anchor_discriminator(kind: str, name: str) -> bytes:
    """sha256(f"{kind}:{name}")[:8]  e.g. ("account", "Position")."""
    return hashlib.sha256(f"{kind}:{name}".encode()).digest()[:8]


class Reader:
    __slots__ = ("data", "pos")

    def __init__(self, data: bytes, pos: int = 0):
        self.data = data
        self.pos = pos

    def _take(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise ValueError(f"borsh: need {n} bytes at {self.pos}, have {len(self.data) - self.pos}")
        chunk = self.data[self.pos:self.pos + n]
        self.pos += n
        return chunk

    def u8(self) -> int: return self._take(1)[0]
    def i8(self) -> int: return struct.unpack("<b", self._take(1))[0]
    def u16(self) -> int: return struct.unpack("<H", self._take(2))[0]
    def u32(self) -> int: return struct.unpack("<I", self._take(4))[0]
    def i32(self) -> int: return struct.unpack("<i", self._take(4))[0]
    def u64(self) -> int: return struct.unpack("<Q", self._take(8))[0]
    def i64(self) -> int: return struct.unpack("<q", self._take(8))[0]
    def u128(self) -> int: return int.from_bytes(self._take(16), "little")
    def i128(self) -> int: return int.from_bytes(self._take(16), "little", signed=True)
    def f32(self) -> float: return struct.unpack("<f", self._take(4))[0]
    def f64(self) -> float: return struct.unpack("<d", self._take(8))[0]
    def bool(self) -> bool: return self.u8() != 0
    def pubkey(self) -> str: return b58encode(self._take(32))
    def bytes(self, n: int) -> bytes: return self._take(n)
    def string(self) -> str: return self._take(self.u32()).decode("utf-8", "replace")
    def remaining(self) -> int: return len(self.data) - self.pos


_PRIMITIVES = {
    "u8": Reader.u8, "i8": Reader.i8, "u16": Reader.u16, "u32": Reader.u32, "i32": Reader.i32,
    "u64": Reader.u64, "i64": Reader.i64, "u128": Reader.u128, "i128": Reader.i128,
    "f32": Reader.f32, "f64": Reader.f64,
    "bool": Reader.bool, "publicKey": Reader.pubkey, "pubkey": Reader.pubkey, "string": Reader.string,
}


class IdlDecoder:
    """Decode Anchor accounts/types by walking the IDL JSON."""

    def __init__(self, idl: dict[str, Any]):
        self.idl = idl
        self.types = {t["name"]: t["type"] for t in idl.get("types", [])}
        self.accounts = {a["name"]: a["type"] for a in idl.get("accounts", [])}

    def decode_account(self, name: str, data: bytes, *, check_discriminator: bool = True) -> dict[str, Any]:
        disc = anchor_discriminator("account", name)
        if check_discriminator and data[:8] != disc:
            raise ValueError(f"discriminator mismatch for {name}: {data[:8].hex()} != {disc.hex()}")
        r = Reader(data, 8)
        return self._struct(self.accounts[name], r)

    def _read(self, ty: Any, r: Reader) -> Any:
        if isinstance(ty, str):
            if ty in _PRIMITIVES:
                return _PRIMITIVES[ty](r)
            raise ValueError(f"unsupported primitive {ty}")
        if "defined" in ty:
            name = ty["defined"] if isinstance(ty["defined"], str) else ty["defined"]["name"]
            return self._typed(name, r)
        if "array" in ty:
            inner, n = ty["array"]
            if inner == "u8":
                return r.bytes(n)
            return [self._read(inner, r) for _ in range(n)]
        if "vec" in ty:
            n = r.u32()
            return [self._read(ty["vec"], r) for _ in range(n)]
        if "option" in ty:
            return self._read(ty["option"], r) if r.u8() else None
        raise ValueError(f"unsupported type {ty}")

    def _typed(self, name: str, r: Reader) -> Any:
        t = self.types.get(name)
        if t is None:
            raise ValueError(f"unknown defined type {name}")
        if t["kind"] == "struct":
            return self._struct(t, r)
        if t["kind"] == "enum":
            idx = r.u8()
            variants = t["variants"]
            if idx >= len(variants):
                raise ValueError(f"enum {name}: variant index {idx} out of range")
            v = variants[idx]
            if v.get("fields"):
                raise ValueError(f"enum {name}: variant {v['name']} with fields is not supported")
            return v["name"]
        raise ValueError(f"unsupported kind {t['kind']}")

    def _struct(self, t: dict[str, Any], r: Reader) -> dict[str, Any]:
        return {f["name"]: self._read(f["type"], r) for f in t["fields"]}
