"""Alert router + Telegram sink: dry-run, min level, cooldown, live path on a mock, token redaction."""
import logging

import httpx
import pytest

from propguard.alerts import AlertContext, AlertRouter, TelegramSink, format_alert
from propguard.engine.rules import Alert


def test_format_alert_has_level_code_text_and_footer():
    a = Alert("crit", "daily_loss", "daily_loss:crit", "DAILY LOSS LIMIT HIT: -1,234.00 USD [anchor: 00:00 UTC]", ts=1_790_000_000)
    txt = format_alert(a, AlertContext(wallet="8Tpcd7K3b1zkgCD4EJTK8pcijVEx6NJ7C9wV6oyQRpNP", transport="grpc"))
    assert "<b>Prop Guard !! CRIT · daily_loss</b>" in txt and "-1,234.00 USD" in txt
    assert "wallet 8Tpc…RpNP · grpc · 14:13:20 UTC" in txt
    a2 = Alert("info", "x", "x", "<script>")
    assert "&lt;script&gt;" in format_alert(a2)


async def test_dry_run_logs_instead_of_sending(caplog):
    sink = TelegramSink("", "", dry_run=True)
    with caplog.at_level(logging.INFO, logger="propguard.alerts"):
        await sink.send(Alert("warn", "liq_distance", "k", "Liquidation 4.10% away"))
    assert sink.sent == 1 and sink.failed == 0
    assert any("telegram[dry-run]" in r.message and "Liquidation 4.10% away" in r.message for r in caplog.records)


async def test_min_level_filters_remote_only():
    sink = TelegramSink("t", "c", min_level="warn", dry_run=True)
    await sink.send(Alert("info", "position_open", "k1", "OPEN"))
    await sink.send(Alert("crit", "daily_loss", "k2", "HIT"))
    assert sink.skipped == 1 and sink.sent == 1


async def test_live_send_uses_bot_api_and_redacts_token():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((str(request.url), request.read()))
        return httpx.Response(200, json={"ok": True}) if len(calls) == 1 else httpx.Response(401, text="Unauthorized bot123:SECRET")

    sink = TelegramSink("123:SECRET", "42", dry_run=False)
    sink._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await sink.send(Alert("warn", "x", "k", "hello"))
    await sink.send(Alert("warn", "x", "k2", "again"))
    assert calls[0][0] == "https://api.telegram.org/bot123:SECRET/sendMessage" and b'"chat_id":"42"' in calls[0][1]
    assert sink.sent == 1 and sink.failed == 1
    assert "SECRET" not in sink.last_error and "401" in sink.last_error
    await sink.close()


async def test_router_cooldown_and_history():
    sink = TelegramSink("", "", dry_run=True)
    r = AlertRouter([sink], cooldown_min=15)
    a = Alert("warn", "liq_distance", "liq:p1:warn", "4% away")
    assert await r.dispatch([a, a]) == 1                 # same key+level within cooldown -> once
    assert await r.dispatch([Alert("crit", "liq_distance", "liq:p1:crit", "1% away")]) == 1
    assert r.suppressed == 1 and len(r.history) == 2 and r.stats()["telegram"]["sent"] == 2
    await r.close()
