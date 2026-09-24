from propguard.constants import CUSTODIES, DOVES_PROGRAM, JUPITER_PERPS_PROGRAM
from propguard.decode.borsh import anchor_discriminator, b58decode, b58encode
from propguard.decode.jupiter import (POSITION_DISCRIMINATOR, decode_custody, decode_doves_price_feed,
                                      decode_position, is_position_account)


def test_base58_roundtrip():
    pk = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"
    raw = b58decode(pk)
    assert len(raw) == 32 and b58encode(raw) == pk
    assert b58encode(b"\0" * 32) == "1" * 32


def test_position_discriminator_matches_mainnet(mainnet_accounts):
    data = mainnet_accounts["position_FBLz"]["data"]
    assert data[:8] == POSITION_DISCRIMINATOR == bytes.fromhex("aabc8fe47a40f7d0")
    assert anchor_discriminator("account", "Position") == POSITION_DISCRIMINATOR


def test_decode_position_fixture(mainnet_accounts):
    fx = mainnet_accounts["position_FBLz"]
    assert fx["owner"] == JUPITER_PERPS_PROGRAM
    assert is_position_account(fx["data"])
    p = decode_position(fx["pubkey"], fx["data"])
    assert p.owner == "815wh1VLC8D7ZWMa3uB7GgjHFetANuML2dPrJiLV1nVF"
    assert p.pool == "5BUwFW4nRbftYTDMbgxykoFWqWHPzahFSNAaaaJtVKsq"
    assert p.custody == CUSTODIES["SOL"] and p.collateral_custody == CUSTODIES["SOL"]
    assert p.side == "long"
    assert p.usd("price") == 151.433252
    assert p.size_usd == 0 and not p.is_open          # closed position keeps its account
    assert p.data_len == 216


def test_decode_doves_feed(mainnet_accounts):
    fx = mainnet_accounts["doves_sol"]
    assert fx["owner"] == DOVES_PROGRAM
    o = decode_doves_price_feed(fx["pubkey"], fx["data"])
    assert o.pair == "SOLUSD"
    assert o.expo < 0
    assert 1 < o.price_usd < 100_000                  # sane SOL price
    assert o.timestamp > 1_700_000_000
    assert abs(o.price_1e6() / 1e6 - o.price_usd) < 1e-5


def test_decode_doves_ag_feed_is_the_live_one(mainnet_accounts):
    """Legacy PriceFeed is stale (months); AgPriceFeed is what the program prices against."""
    legacy = decode_doves_price_feed(mainnet_accounts["doves_sol"]["pubkey"], mainnet_accounts["doves_sol"]["data"])
    ag_fx = mainnet_accounts["doves_ag_sol"]
    ag = decode_doves_price_feed(ag_fx["pubkey"], ag_fx["data"])
    assert ag.expo == -8 and 1 < ag.price_usd < 100_000
    assert ag_fx["captured_at"] - ag.timestamp < 120            # fresh at capture time
    assert ag_fx["captured_at"] - legacy.timestamp > 30 * 86400   # legacy feed months old
    assert ag.pair.startswith("AG:So1111")                        # mint = wrapped SOL


def test_decode_custody_fixture(mainnet_accounts):
    fx = mainnet_accounts["custody_sol"]
    c = decode_custody(fx["pubkey"], fx["data"])
    assert c["mint"] == "So11111111111111111111111111111111111111112"
    assert c["decimals"] == 9 and c["isStable"] is False
    assert c["pricing"]["maxLeverage"] > 0 and c["pricing"]["tradeImpactFeeScalar"] > 0
    assert c["fundingRateState"]["cumulativeInterestRate"] > 0
    assert c["dovesOracle"] == mainnet_accounts["doves_sol"]["pubkey"]
    assert c["dovesAgOracle"] == mainnet_accounts["doves_ag_sol"]["pubkey"]
    u = decode_custody(mainnet_accounts["custody_usdc"]["pubkey"], mainnet_accounts["custody_usdc"]["data"])
    assert u["isStable"] is True and u["decimals"] == 6
