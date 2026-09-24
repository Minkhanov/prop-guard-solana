"""Alert delivery: console, Telegram (Bot API, HTML), dry-run, with dedup + cooldown.

Message shape (Telegram and dry-run share `format_alert`):

    Prop Guard !! CRIT · daily_loss
    DAILY LOSS LIMIT HIT: -1,234.00 USD (5.10% of $24,000) — stop trading today [anchor: 00:00 UTC]
    wallet 8Tpc…RpNP · grpc · 13:47:02 UTC

The router keeps a history for the panel and suppresses a repeated key+level for
`cooldown_min` minutes. Event alerts (open/close/change) carry a timestamp in their
key, so every event is delivered once.
"""
from __future__ import annotations

import asyncio
import html
import logging
import time
from datetime import datetime, timezone
from typing import Any, Protocol

import httpx

from ..engine.rules import Alert

log = logging.getLogger("propguard.alerts")

ICON = {"info": "i", "warn": "!", "crit": "!!"}
LEVEL_RANK = {"info": 0, "warn": 1, "crit": 2}


class Sink(Protocol):
    name: str
    async def send(self, alert: Alert) -> None: ...


class AlertContext:
    """What every remote message is stamped with (set once by the guard)."""

    def __init__(self, wallet: str = "", transport: str = "", title: str = "Prop Guard"):
        self.wallet = wallet
        self.transport = transport
        self.title = title

    @property
    def wallet_short(self) -> str:
        return f"{self.wallet[:4]}…{self.wallet[-4:]}" if len(self.wallet) > 12 else self.wallet


def format_alert(alert: Alert, ctx: AlertContext | None = None, *, html_mode: bool = True) -> str:
    ctx = ctx or AlertContext()
    when = datetime.fromtimestamp(alert.ts, tz=timezone.utc).strftime("%H:%M:%S UTC")
    esc = html.escape if html_mode else (lambda s: s)
    head = f"{esc(ctx.title)} {ICON[alert.level]} {alert.level.upper()} · {esc(alert.code)}"
    foot = " · ".join(x for x in (f"wallet {esc(ctx.wallet_short)}" if ctx.wallet else "", esc(ctx.transport), when) if x)
    if html_mode:
        return f"<b>{head}</b>\n{esc(alert.text)}\n<i>{foot}</i>"
    return f"{head}\n{alert.text}\n{foot}"


class ConsoleSink:
    name = "console"

    async def send(self, alert: Alert) -> None:
        log.log({"info": logging.INFO, "warn": logging.WARNING, "crit": logging.ERROR}[alert.level],
                "[%s] %s", alert.code, alert.text)


class NullSink:
    name = "null"

    async def send(self, alert: Alert) -> None:
        return None


class TelegramSink:
    """Plain Bot API (no framework): POST /bot{token}/sendMessage, ≤ 1 msg/s, HTML.

    `dry_run=True` logs the exact message instead of sending it (no token needed) —
    the delivery path, formatting, min-level and rate-limit are exercised anyway.
    """
    name = "telegram"

    def __init__(self, token: str, chat_id: str, *, ctx: AlertContext | None = None, min_level: str = "info",
                 dry_run: bool = False):
        self.token = token
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.ctx = ctx or AlertContext()
        self.min_level = min_level
        self.dry_run = dry_run or not (token and chat_id)
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()
        self._last = 0.0
        self.sent = 0
        self.failed = 0
        self.skipped = 0
        self.last_error = ""

    def _redact(self, text: str) -> str:
        return text.replace(self.token, "***") if self.token else text

    async def send(self, alert: Alert) -> None:
        if LEVEL_RANK[alert.level] < LEVEL_RANK.get(self.min_level, 0):
            self.skipped += 1
            return
        text = format_alert(alert, self.ctx)
        async with self._lock:
            wait = 1.0 - (time.monotonic() - self._last)
            if wait > 0 and not self.dry_run:
                await asyncio.sleep(wait)
            try:
                if self.dry_run:
                    chat = f"…{self.chat_id[-2:]}" if self.chat_id else "(none)"   # never log the full chat id
                    log.info("telegram[dry-run] -> chat %s: %s", chat, text.replace("\n", " | "))
                    self.sent += 1
                    return
                await self.post_message(text)
                self.sent += 1
            except (httpx.HTTPError, RuntimeError) as exc:
                self.failed += 1
                self.last_error = self._redact(str(exc))
                log.warning("telegram error: %s", self.last_error)
            finally:
                self._last = time.monotonic()

    async def post_message(self, text: str) -> dict[str, Any]:
        """Raw sendMessage; raises RuntimeError on a non-200 answer (token redacted)."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=15)
        r = await self._client.post(self.url, json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML",
                                                    "disable_web_page_preview": True})
        if r.status_code != 200:
            raise RuntimeError(f"telegram sendMessage {r.status_code}: {self._redact(r.text[:200])}")
        return r.json()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


class AlertRouter:
    """Fan-out with dedup: the same key+level is re-sent only after `cooldown_min`."""

    def __init__(self, sinks: list[Sink], *, cooldown_min: int = 15):
        self.sinks = sinks
        self.cooldown = cooldown_min * 60
        self._seen: dict[str, float] = {}
        self.history: list[Alert] = []
        self.suppressed = 0

    def _fresh(self, alert: Alert) -> bool:
        k = f"{alert.key}|{alert.level}"
        last = self._seen.get(k)
        now = time.time()
        if last is not None and now - last < self.cooldown:
            return False
        self._seen[k] = now
        return True

    async def dispatch(self, alerts: list[Alert]) -> int:
        n = 0
        for a in alerts:
            if not self._fresh(a):
                self.suppressed += 1
                continue
            self.history.append(a)
            del self.history[:-200]
            n += 1
            for s in self.sinks:
                try:
                    await s.send(a)
                except Exception as exc:  # a broken sink must not stop the guard
                    log.warning("sink %s failed: %s", s.name, exc)
        return n

    async def close(self) -> None:
        for s in self.sinks:
            close = getattr(s, "close", None)
            if close:
                try:
                    await close()
                except Exception as exc:  # noqa: BLE001
                    log.debug("sink %s close: %s", s.name, exc)

    def stats(self) -> dict[str, Any]:
        out: dict[str, Any] = {"suppressed": self.suppressed, "delivered": len(self.history)}
        for s in self.sinks:
            if isinstance(s, TelegramSink):
                out["telegram"] = {"mode": "dry-run" if s.dry_run else "live", "sent": s.sent, "failed": s.failed,
                                   "skipped_below_min_level": s.skipped, "min_level": s.min_level, "last_error": s.last_error}
        return out
