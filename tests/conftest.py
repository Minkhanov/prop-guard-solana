import base64
import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).with_name("fixtures")


@pytest.fixture(scope="session")
def mainnet_accounts() -> dict:
    raw = json.loads((FIXTURES / "mainnet_accounts_2026-09-24.json").read_text(encoding="utf-8"))
    return {k: {**v, "data": base64.b64decode(v["data_b64"])} for k, v in raw.items()}
