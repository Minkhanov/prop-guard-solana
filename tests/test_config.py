from pathlib import Path

from propguard.config import is_base58_pubkey, load_dotenv, load_settings


def test_dotenv_parsing(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text('SOLAMI_API_KEY="sk_test_123"\n# comment\nTRANSPORT=grpc\nRPC_RPS=200\n\nWALLET=815wh1VLC8D7ZWMa3uB7GgjHFetANuML2dPrJiLV1nVF\n', encoding="utf-8")
    vals = load_dotenv(env)
    assert vals["SOLAMI_API_KEY"] == "sk_test_123"
    s = load_settings(str(env))
    assert s.transport == "grpc" and s.rpc_rps == 200 and s.grpc_key == "sk_test_123"
    assert s.problems() == []
    assert s.rpc_url == "https://rpc.solami.dev/sol"
    assert s.grpc_endpoint == "grpc.solami.dev:443"


def test_region_prefix(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("SOLAMI_REGION", raising=False)
    s = load_settings(str(tmp_path / "missing.env"), {"solami_region": "fra", "solami_api_key": "k"})
    assert s.rpc_url == "https://fra.rpc.solami.dev/sol"
    assert s.ws_url == "wss://fra.ws.solami.dev/ws/sol"


def test_problems_reported(tmp_path: Path):
    s = load_settings(str(tmp_path / "missing.env"), {"transport": "mirage", "wallet": "not-a-key"})
    probs = s.problems()
    assert any("SOLAMI_API_KEY" in p for p in probs)
    assert any("MIRAGE_SUBSCRIPTION_ID" in p for p in probs)
    assert any("WALLET" in p for p in probs)


def test_public_rpc_needs_no_key(tmp_path: Path):
    s = load_settings(str(tmp_path / "missing.env"), {"solami_rpc_url": "https://api.mainnet-beta.solana.com",
                                                      "wallet": "815wh1VLC8D7ZWMa3uB7GgjHFetANuML2dPrJiLV1nVF"})
    assert not s.rpc_is_solami
    assert s.problems() == []


def test_base58_check():
    assert is_base58_pubkey("PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu")
    assert not is_base58_pubkey("0OIl")
    assert not is_base58_pubkey("PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJ")  # 31 bytes


def test_masked_secrets(tmp_path: Path):
    s = load_settings(str(tmp_path / "missing.env"), {"solami_api_key": "sk_live_abcdefghijk", "telegram_bot_token": "123:abc"})
    m = s.masked()
    assert "abcdefghijk" not in m["solami_api_key"] and m["solami_api_key"].startswith("sk_l")
    assert m["telegram_bot_token"] == "***"
