"""
Telegram channel/group message collector.

Uses python-telegram-bot's Bot API to read recent messages from
channels/groups where the bot has been added as an admin.

Env vars:
  TELEGRAM_BOT_TOKEN        – Bot token from @BotFather
  TELEGRAM_CHAT_IDS         – Comma-separated list of chat IDs to monitor
"""

import asyncio
import os
from datetime import datetime, timezone
from typing import Optional


def get_telegram_messages(
    chat_ids: Optional[list[str]] = None,
    limit: int = 50,
) -> list[dict]:
    """
    Fetch recent messages from Telegram chats.

    NOTE: The Telegram Bot API does not support fetching *historical* messages
    via getUpdates for chats the bot hasn't seen yet. In production, run the
    bot persistently (see src/live/scheduler.py) and store incoming messages.
    This function returns buffered messages collected by the running bot.

    Returns [] immediately if TELEGRAM_BOT_TOKEN is not configured.

    Args:
        chat_ids:  Optional override list of chat IDs; defaults to env var
        limit:     Max messages to return per chat

    Returns:
        List of dicts with keys: chat_id, message_id, text, date, sender_username.
    """
    if not os.environ.get("TELEGRAM_BOT_TOKEN"):
        return []

    # In a persistent bot setup, messages are stored in _message_buffer
    # (populated by start_telegram_listener). Return from buffer here.
    results: list[dict] = []
    for msg in list(_message_buffer)[-limit:]:
        if chat_ids is None or str(msg.get("chat_id")) in chat_ids:
            results.append(msg)
    return results


# In-memory buffer populated by the async listener
_message_buffer: list[dict] = []
_MAX_BUFFER = 1000


def _append_to_buffer(msg: dict) -> None:
    """Add a message to the circular buffer."""
    _message_buffer.append(msg)
    if len(_message_buffer) > _MAX_BUFFER:
        _message_buffer.pop(0)


async def _telegram_listener() -> None:
    """
    Start a long-polling Telegram bot that appends messages to _message_buffer.
    Run this in a background thread/task during live trading.
    """
    try:
        from telegram import Update
        from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
    except ImportError:
        raise ImportError("python-telegram-bot is required. Run: poetry add 'python-telegram-bot[job-queue]'")

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is not set.")

    async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        msg = update.message or update.channel_post
        if not msg or not msg.text:
            return
        _append_to_buffer(
            {
                "chat_id": msg.chat_id,
                "message_id": msg.message_id,
                "text": msg.text[:500],
                "date": msg.date.astimezone(timezone.utc).isoformat(),
                "sender_username": msg.from_user.username if msg.from_user else None,
            }
        )

    app = ApplicationBuilder().token(token).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    await app.run_polling(drop_pending_updates=True)


def start_telegram_listener_thread() -> None:
    """
    Spawn a daemon thread running the async Telegram listener.
    Call this once at startup in live trading mode.
    """
    import threading

    def _run():
        asyncio.run(_telegram_listener())

    t = threading.Thread(target=_run, daemon=True, name="telegram-listener")
    t.start()
