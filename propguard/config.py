"""Configuration: tiny .env loader + validated Settings.

No python-dotenv on purpose: the loader is 25 lines and has no surprises.
Precedence: process environment > .env file > defaults.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

from .constants import B58_ALPHABET

TRANSPORTS = ("rpc", "grpc", "mirage")
REGIONS = ("", "ams", "fra", "nyc")
COMMITMENTS = ("processed", "confirmed", "finalized")


def load_dotenv(path: str | os.PathLike | None = None) -> dict[str, str]:
    """Read KEY=VALUE lines from a .env file into a dict (does not touch os.environ)."""
    candidates = [Path(path)] if path else [Path(os.environ.get("PROPGUARD_ENV", ".env"))]
    out: dict[str, str] = {}
    for p in candidates:
        if not p.is_file():
            continue
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            out[key.strip()] = value
    return out


def is_base58_pubkey(value: str) -> bool:
    if not (32 <= len(value) <= 44):
        return False
    if any(ch not in B58_ALPHABET for ch in value):
        return False
    n = 0
    for ch in value:
        n = n * 58 + B58_ALPHABET.index(ch)
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big")
    pad = len(value) - len(value.lstrip("1"))
    return len(raw) + pad == 32


@dataclass
class Settings:
    # Solami
    solami_api_key: str = ""
    solami_grpc_key: str = ""
    solami_region: str = ""
    solami_rpc_url: str = ""
    solami_grpc_endpoint: str = ""
    solami_grpc_tls_server_name: str = ""   # SNI/cert name override when SOLAMI_GRPC_ENDPOINT is a local TLS-passthrough proxy
    mirage_subscription_id: str = ""
    # target
    transport: str = "rpc"
    wallet: str = ""
    commitment: str = "processed"
    poll_interval_ms: int = 1500
    rpc_rps: float = 5.0
    # rules
    account_size_usd: float = 0.0
    daily_loss_limit_pct: float = 5.0
    max_exposure_x: float = 10.0
    liq_distance_warn_pct: float = 5.0
    liq_distance_crit_pct: float = 2.0
    oracle_stale_sec: int = 30
    stream_stale_sec: int = 20
    # alerts
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_min_level: str = "info"     # info | warn | crit — lowest level delivered to Telegram
    alerts_dry_run: bool = False         # log the Telegram message instead of sending it
    alert_cooldown_min: int = 15
    # stream resilience
    fallback_silence_sec: int = 20       # stream silent this long -> RPC polling takes over (0 = off)
    fallback_poll_interval_ms: int = 2000
    # health journal (JSON lines, one record every HEALTH_LOG_EVERY_SEC; empty = off)
    health_log_file: str = ""
    health_log_every_sec: int = 10
    # demo picker: look at positions touched within the last N hours (getProgramAccountsV2 changedSinceSlot)
    demo_scan_hours: float = 24.0
    # panel
    panel_host: str = "127.0.0.1"
    panel_port: int = 8787
    # persistence
    state_file: str = "state/propguard-state.json"
    log_level: str = "INFO"
    # bookkeeping
    source: dict[str, str] = field(default_factory=dict, repr=False)

    # ---- derived -------------------------------------------------------------
    @property
    def grpc_key(self) -> str:
        return self.solami_grpc_key or self.solami_api_key

    def host(self, service: str) -> str:
        """Regional hostname for a Solami service: rpc | grpc | ws | api."""
        base = f"{service}.solami.dev"
        return f"{self.solami_region}.{base}" if self.solami_region else base

    @property
    def rpc_url(self) -> str:
        if self.solami_rpc_url:
            return self.solami_rpc_url
        return f"https://{self.host('rpc')}/sol"

    @property
    def rpc_is_solami(self) -> bool:
        return "solami" in self.rpc_url

    @property
    def grpc_endpoint(self) -> str:
        return self.solami_grpc_endpoint or f"{self.host('grpc')}:443"

    @property
    def ws_url(self) -> str:
        return f"wss://{self.host('ws')}/ws/sol"

    @property
    def mirage_url(self) -> str:
        return f"wss://{self.host('ws')}/mirage/stream/{self.mirage_subscription_id}"

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @property
    def telegram_dry_run(self) -> bool:
        """Dry-run when asked explicitly, or when there is no token to send with."""
        return self.alerts_dry_run or not self.telegram_enabled

    # ---- validation ----------------------------------------------------------
    def problems(self, *, need_wallet: bool = True) -> list[str]:
        p: list[str] = []
        if self.transport not in TRANSPORTS:
            p.append(f"TRANSPORT must be one of {TRANSPORTS}, got {self.transport!r}")
        if self.solami_region not in REGIONS:
            p.append(f"SOLAMI_REGION must be one of {REGIONS[1:]} or empty, got {self.solami_region!r}")
        if self.commitment not in COMMITMENTS:
            p.append(f"COMMITMENT must be one of {COMMITMENTS}, got {self.commitment!r}")
        if self.rpc_is_solami and not self.solami_api_key:
            p.append("SOLAMI_API_KEY is empty (get one at https://solami.dev -> Dashboard -> API keys)")
        if self.transport == "grpc" and not self.grpc_key:
            p.append("TRANSPORT=grpc needs SOLAMI_GRPC_KEY or SOLAMI_API_KEY (Pro plan or higher)")
        if self.transport == "mirage":
            if not self.mirage_subscription_id:
                p.append("TRANSPORT=mirage needs MIRAGE_SUBSCRIPTION_ID (create one in the Solami dashboard)")
            if not self.solami_api_key:
                p.append("TRANSPORT=mirage needs SOLAMI_API_KEY with the MirageStream permission")
        if need_wallet:
            if not self.wallet:
                p.append("WALLET is empty (or use `propguard demo` to auto-pick one)")
            elif not is_base58_pubkey(self.wallet):
                p.append(f"WALLET is not a valid base58 Solana public key: {self.wallet!r}")
        if not (0 < self.daily_loss_limit_pct <= 100):
            p.append("DAILY_LOSS_LIMIT_PCT must be in (0, 100]")
        if self.liq_distance_crit_pct >= self.liq_distance_warn_pct:
            p.append("LIQ_DISTANCE_CRIT_PCT must be smaller than LIQ_DISTANCE_WARN_PCT")
        if self.max_exposure_x <= 0:
            p.append("MAX_EXPOSURE_X must be positive")
        if self.poll_interval_ms < 200:
            p.append("POLL_INTERVAL_MS must be >= 200")
        if self.rpc_rps <= 0:
            p.append("RPC_RPS must be positive")
        if bool(self.telegram_bot_token) != bool(self.telegram_chat_id):
            p.append("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set together")
        if self.telegram_min_level not in ("info", "warn", "crit"):
            p.append("TELEGRAM_MIN_LEVEL must be info | warn | crit")
        if self.fallback_silence_sec < 0 or self.health_log_every_sec < 1:
            p.append("FALLBACK_SILENCE_SEC must be >= 0 and HEALTH_LOG_EVERY_SEC >= 1")
        return p

    # ---- presentation --------------------------------------------------------
    def masked(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for f in fields(self):
            if f.name == "source":
                continue
            v = getattr(self, f.name)
            if any(s in f.name for s in ("key", "token")) and v:
                v = v[:4] + "…" + v[-2:] if len(v) > 8 else "***"
            out[f.name] = v
        return out


def _bool(raw: str) -> bool:
    v = raw.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off", ""):
        return False
    raise ValueError(raw)


_CASTS: dict[str, Any] = {
    "poll_interval_ms": int,
    "fallback_silence_sec": int,
    "fallback_poll_interval_ms": int,
    "health_log_every_sec": int,
    "alerts_dry_run": _bool,
    "demo_scan_hours": float,
    "oracle_stale_sec": int,
    "stream_stale_sec": int,
    "alert_cooldown_min": int,
    "panel_port": int,
    "rpc_rps": float,
    "account_size_usd": float,
    "daily_loss_limit_pct": float,
    "max_exposure_x": float,
    "liq_distance_warn_pct": float,
    "liq_distance_crit_pct": float,
}


def load_settings(env_path: str | None = None, overrides: dict[str, Any] | None = None) -> Settings:
    file_vals = load_dotenv(env_path)
    s = Settings()
    for f in fields(s):
        if f.name == "source":
            continue
        env_name = f.name.upper()
        if env_name in os.environ:
            raw, src = os.environ[env_name], "env"
        elif env_name in file_vals:
            raw, src = file_vals[env_name], ".env"
        else:
            continue
        cast = _CASTS.get(f.name, str)
        try:
            setattr(s, f.name, cast(raw))
        except ValueError:
            raise ValueError(f"{env_name}={raw!r} is not a valid {cast.__name__}") from None
        s.source[f.name] = src
    for k, v in (overrides or {}).items():
        if v is not None:
            setattr(s, k, v)
            s.source[k] = "cli"
    s.transport = s.transport.lower()
    s.solami_region = s.solami_region.lower()
    s.commitment = s.commitment.lower()
    s.telegram_min_level = s.telegram_min_level.lower()
    return s
