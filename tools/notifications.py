from __future__ import annotations

import os
from typing import Iterable
import requests
import logging

# Discord Webhook URLs (loaded from environment variables for security)
# Set these environment variables:
#   DISCORD_WEBHOOK_FINANCIAL - for financial_crawling channel
#   DISCORD_WEBHOOK_SELECTION - for stock_selection channel  
#   DISCORD_WEBHOOK_TRADE - for trade_execution channel
WEBHOOK_URLS = {
    "financial_crawling": os.environ.get("DISCORD_WEBHOOK_FINANCIAL", ""),
    "stock_selection": os.environ.get("DISCORD_WEBHOOK_SELECTION", ""),
    "trade_execution": os.environ.get("DISCORD_WEBHOOK_TRADE", ""),
}

logger = logging.getLogger(__name__)

def send_notification(
    channel: str,
    message: str,
    *,
    title: str | None = None,
    priority: str | None = None,
    tags: Iterable[str] | None = None,
    markdown: bool = False,
    base_url: str | None = None,
    token: str | None = None,
    timeout: float | None = None,
) -> None:
    """
    Sends a notification via Discord Webhook.
    
    The 'channel' argument determines which Webhook URL to use.
    Supported channels: 'financial_crawling', 'stock_selection', 'trade_execution'.
    Defaults to 'trade_execution' if channel is unknown.
    """
    try:
        # Select webhook URL based on channel
        webhook_url = WEBHOOK_URLS.get(channel)
        if not webhook_url:
            logger.warning(f"Unknown channel '{channel}'. Defaulting to 'trade_execution'.")
            webhook_url = WEBHOOK_URLS["trade_execution"]

        content = ""
        
        # Map tags to emojis
        if tags:
            # tags often match discord emoji names (e.g. warning, rocket)
            emoji_str = " ".join(f":{tag}:" for tag in tags)
            content += f"{emoji_str} "
            
        if title:
            content += f"**{title}**\n"
            
        content += message
        
        # Discord payload
        payload = {
            "content": content
        }
        
        response = requests.post(webhook_url, json=payload, timeout=timeout or 10)
        response.raise_for_status()
        
    except Exception as e:
        logger.error(f"Failed to send Discord notification: {e}")