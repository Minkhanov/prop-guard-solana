"""On-chain constants for Jupiter Perpetuals (mainnet-beta) and Solami endpoints.

Sources (checked 2026-09-24):
- developers.jup.ag/docs/perps/*  (position / custody / pool accounts)
- github.com/julianfssen/jupiter-perps-anchor-idl-parsing (constants.ts, examples)
- solami.dev/llms.txt, api.solami.dev/pricing, Solami docs (Mirage, gRPC, RPC)
"""
from __future__ import annotations

B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# --- Jupiter Perpetuals ------------------------------------------------------
JUPITER_PERPS_PROGRAM = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"
JUPITER_PERPS_EVENT_AUTHORITY = "37hJBDnntwqhGbK7L6M1bLyvccj4u55CCUiLPdYkiqBN"
JLP_POOL = "5BUwFW4nRbftYTDMbgxykoFWqWHPzahFSNAaaaJtVKsq"
DOVES_PROGRAM = "DoVEsk76QybCEHQGzkvYPWLQu9gzNoZZZt3TPiL597e"

# Custody accounts (one per token in the JLP pool). JupUSD added in 2026.
CUSTODIES: dict[str, str] = {
    "SOL": "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz",
    "ETH": "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn",
    "BTC": "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm",
    "USDC": "G18jKKXQwBbrHeiK3C9MRXhkHsLHf7XgCSisykV46EZa",
    "USDT": "4vkNeXiYEUizLdrpdPS1eC2mccyM4NUPRtERrk6ZETkk",
    "JUPUSD": "DdwY1ELc9rRK7xNL3hTXabSFBmVrTPpfsUZSv2Y3LL1U",
}
CUSTODY_SYMBOL: dict[str, str] = {v: k for k, v in CUSTODIES.items()}

# Doves aggregated price feeds (`custody.dovesAgOracle`, account type AgPriceFeed) — the feeds
# the program prices against today. The legacy `custody.dovesOracle` PriceFeed accounts stopped
# updating around June 2026 (111 days stale on 2026-09-24). Fallback list only: the live value
# is read from the custody accounts at bootstrap so a rotated feed does not break us.
DOVES_ORACLES_FALLBACK: dict[str, str] = {
    "SOL": "FYq2BWQ1V5P1WFBqr3qB2Kb5yHVvSv7upzKodgQE5zXh",
    "ETH": "AFZnHPzy4mvVCffrVwhewHbFc93uTHvDSFrVH7GtfXF1",
    "BTC": "hUqAT1KQ7eW1i6Csp9CXYtpPfSAvi835V7wKi5fRfmC",
    "USDC": "6Jp2xZUTWdDD2ZyUPRzeMdc6AFQ5K3pFgZxk2EijfjnM",
    "USDT": "Fgc93D641F8N2d1xLjQ4jmShuD3GE3BsCXA56KBQbF5u",
    "JUPUSD": "9DRrwc4hSMQouw3ptpHXtQXVNHoqwyqLaK6NWztWTDin",
}
DOVES_LEGACY_FEEDS: dict[str, str] = {  # kept for the fixture tests only
    "SOL": "39cWjvHrpHNz2SbXv6ME4NPhqBDBd4KsjUYv5JkHEAJU",
}

# Position account layout facts (verified on mainnet 2026-09-24: 216 bytes,
# discriminator aabc8fe47a40f7d0 = sha256("account:Position")[:8]).
POSITION_ACCOUNT_SIZE = 216
POSITION_OWNER_OFFSET = 8

# Fixed-point precisions used by the program
USD_DECIMALS = 6              # sizeUsd / collateralUsd / price are 1e-6 USD
BPS_POWER = 10_000
DBPS_POWER = 100_000
RATE_POWER = 1_000_000_000
DEBT_POWER = RATE_POWER
HOURS_IN_YEAR = 24 * 365

# --- Solami -----------------------------------------------------------------
SOLAMI_HOSTS = {"rpc": "rpc.solami.dev", "grpc": "grpc.solami.dev", "ws": "ws.solami.dev", "api": "api.solami.dev"}
SOLAMI_PLAN_LIMITS = {  # from GET https://api.solami.dev/pricing (2026-09-24)
    "Free": {"rpc_rps": 5, "ws": 0, "grpc": 0},
    "Dev": {"rpc_rps": 50, "ws": 2, "grpc": 0},
    "Pro": {"rpc_rps": 200, "ws": 5, "grpc": 2},
    "Ultra": {"rpc_rps": 500, "ws": 10, "grpc": 5},
}
GRPC_REPLAY_MAX_SLOTS = 3500  # Solami replays up to 3,500 slots via from_slot
