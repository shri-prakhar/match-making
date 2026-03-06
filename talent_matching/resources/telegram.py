"""Telegram Bot API resource for sending alerts.

Uses httpx to POST to the Telegram Bot API. No additional dependencies.
Alerting failures are logged, not raised — alerting should never crash pipelines.
"""

import logging
from datetime import UTC, datetime

import httpx
from dagster import ConfigurableResource
from pydantic import Field

logger = logging.getLogger(__name__)


def _escape_markdown_v2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    # Backslash must be escaped first
    text = text.replace("\\", "\\\\")
    for char in "_*[]()~`>#+-=|{}.!":
        text = text.replace(char, f"\\{char}")
    return text


class TelegramResource(ConfigurableResource):
    """Resource for sending messages via the Telegram Bot API.

    Configuration:
        bot_token: Telegram Bot API token (from BotFather)
        chat_id: Target chat ID for messages
        enabled: Kill switch; when False, send_message/send_alert no-op
    """

    bot_token: str = Field(
        default="",
        description="Telegram Bot API token (from TELEGRAM_BOT_TOKEN)",
    )
    chat_id: str = Field(
        default="",
        description="Target chat ID for messages (from TELEGRAM_CHAT_ID)",
    )
    enabled: bool = Field(
        default=True,
        description="When False, send_message and send_alert are no-ops",
    )

    def send_message(
        self,
        text: str,
        parse_mode: str = "MarkdownV2",
    ) -> bool:
        """Send a raw message. Returns True on success, False on failure. Logs errors."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }

        with httpx.Client(timeout=10.0) as client:
            resp = client.post(url, json=payload)

        if resp.status_code != 200:
            # Log but do not raise — alerting should never crash pipelines
            logger.warning(
                "Telegram send_message failed: %s %s",
                resp.status_code,
                resp.text[:200],
            )
            return False
        return True

    def send_alert(self, title: str, body: str) -> bool:
        """Send an alert with standard format: bold title, timestamp, body."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            return False

        now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        # MarkdownV2 requires escaping
        title_esc = _escape_markdown_v2(title)
        body_esc = _escape_markdown_v2(body)
        now_esc = _escape_markdown_v2(now)

        text = f"*{title_esc}*\n_{now_esc}_\n\n{body_esc}"
        return self.send_message(text, parse_mode="MarkdownV2")
