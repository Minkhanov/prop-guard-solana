"""Alert sinks: console, Telegram (live or dry-run). Dedup + cooldown live in `AlertRouter`."""
from .router import AlertContext, AlertRouter, ConsoleSink, NullSink, TelegramSink, format_alert

__all__ = ["AlertContext", "AlertRouter", "ConsoleSink", "NullSink", "TelegramSink", "format_alert"]
