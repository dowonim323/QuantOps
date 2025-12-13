from __future__ import annotations

import os
from typing import Iterable
import requests
import logging

# Discord Webhook URLs
WEBHOOK_URLS = {
    "financial_crawling": "https://discordapp.com/api/webhooks/1447250119933296752/rdyWhx0s5xXFxFqyvjE6jWcRemD7MbGzK6jOdfn0r_AH41S_bvqhyanTjdwcBIh9vhnw",
    "stock_selection": "https://discordapp.com/api/webhooks/1447272865870385225/uJ8KSekjSe-JDZVSiGJKrA1lTep_ZjPPULeHbubSSivxCgMAovbBFtMECjCXs3c0Oaqc",
    "trade_execution": "https://discordapp.com/api/webhooks/1447273280988905615/1dUG2fB3Q_rQCynCj78zUF2BDEa_OVg8h_vitXG3-7n6iWg9mOWvHJPtjTzQ8cnyzQnj",
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