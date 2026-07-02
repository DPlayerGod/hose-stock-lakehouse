"""
Slack Notifier — Gửi cảnh báo CRITICAL qua Slack Incoming Webhook.
"""

import os
import json
import logging
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError

logger = logging.getLogger('alerts.slack')

SEVERITY_COLOR = {
    'CRITICAL': '#ff0000',
    'WARNING': '#ffa800',
    'INFO': '#36a64f',
}

ALERT_TYPE_EMOJI = {
    'COMBINED_PUMP_RISK': '🔴',
    'COMBINED_PANIC_SELL': '🔴',
    'COMBINED_OVERBOUGHT_BREAKOUT': '📈',
    'COMBINED_OVERSOLD_BREAKDOWN': '📉',
    'COMBINED_UNUSUAL_VOLUME': '🟠',
    'COMBINED_VOLUME_BREAKOUT': '📈',
    'COMBINED_VOLUME_BREAKDOWN': '📉',
    'VWAP_BREAKOUT_UP': '📈',
    'VWAP_BREAKDOWN': '📉',
    'RSI_OVERBOUGHT': '🔴',
    'RSI_OVERSOLD': '🟢',
    'VOLUME_SPIKE': '🔶',
}


class SlackNotifier:
    """Gửi cảnh báo qua Slack Incoming Webhook."""

    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url or os.getenv('SLACK_DNSE_WEBHOOK', '')
        self.enabled = bool(self.webhook_url)
        if self.enabled:
            logger.info("Slack notifier enabled")
        else:
            logger.info("Slack notifier disabled (SLACK_DNSE_WEBHOOK not set)")

    def send_alert(self, alert) -> bool:
        """
        Gửi 1 alert lên Slack. Chỉ gửi nếu severity == CRITICAL.
        Returns True nếu gửi thành công.
        """
        if not self.enabled:
            return False

        if alert.severity != 'CRITICAL':
            return False

        emoji = ALERT_TYPE_EMOJI.get(alert.alert_type, '🚨')
        color = SEVERITY_COLOR.get(alert.severity, '#ff0000')

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} CRITICAL ALERT — {alert.symbol}",
                        "emoji": True
                    }
                },
            ],
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "section",
                            "fields": [
                                {"type": "mrkdwn", "text": f"*Loại:*\n`{alert.alert_type}`"},
                                {"type": "mrkdwn", "text": f"*Rule:*\n`{alert.rule_name}`"},
                                {"type": "mrkdwn", "text": f"*Giá:*\n`{alert.price:,.2f}`"},
                                {"type": "mrkdwn", "text": f"*Chỉ báo:*\n`{alert.indicator_value:.2f}`"},
                                {"type": "mrkdwn", "text": f"*Ngưỡng:*\n`{alert.threshold:.2f}`"},
                                {"type": "mrkdwn", "text": f"*Thời gian:*\n`{alert.alert_time.strftime('%H:%M:%S')}`"},
                            ]
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"💬 *{alert.message}*"
                            }
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"📡 Stock Lakehouse Alert Detector • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        return self._post(payload)

    def _post(self, payload: dict) -> bool:
        """POST JSON payload tới Slack webhook."""
        try:
            data = json.dumps(payload).encode('utf-8')
            req = Request(
                self.webhook_url,
                data=data,
                headers={'Content-Type': 'application/json'},
                method='POST',
            )
            with urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    logger.info("Slack notification sent ✓")
                    return True
                else:
                    logger.warning(f"Slack returned status {resp.status}")
                    return False
        except URLError as e:
            logger.error(f"Slack send failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Slack unexpected error: {e}")
            return False
