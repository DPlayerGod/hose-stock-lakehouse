"""
Integration test — gửi alert thật đến Slack.
Chạy: python -m pytest tests/test_slack_integration.py -v -s
"""

import pytest
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, 'src')

from stock_lakehouse.streaming.alerts.models import Alert
from stock_lakehouse.streaming.alerts.slack_notifier import SlackNotifier


@pytest.fixture
def slack_notifier():
    """Khởi tạo SlackNotifier với webhook thật."""
    return SlackNotifier()


@pytest.fixture
def sample_alert():
    """Tạo alert mẫu để test."""
    return Alert(
        symbol='FPT',
        alert_type='VWAP_BREAKOUT_UP',
        severity='CRITICAL',
        rule_name='VWAP Breakout Rule',
        price=125000.0,
        indicator_value=124500.0,
        threshold=2.0,
        deviation_pct=0.4,
        message='FPT vừa breakout khỏi VWAP, tăng 0.4%',
        alert_time=datetime.now(),
    )


def test_send_alert_to_slack(slack_notifier, sample_alert):
    """
    Integration test: gửi alert thật đến Slack channel.
    """
    if not slack_notifier.enabled:
        pytest.skip("SLACK_DNSE_WEBHOOK not configured")

    print(f"\n>>> Sending alert to Slack...")
    print(f">>> Webhook URL: {slack_notifier.webhook_url[:50]}...")

    result = slack_notifier.send_alert(sample_alert)

    print(f">>> Result: {result}")
    assert result is True, "Slack notification failed!"


def test_send_multiple_alerts(slack_notifier):
    """
    Test gửi nhiều alerts liên tiếp.
    """
    if not slack_notifier.enabled:
        pytest.skip("SLACK_DNSE_WEBHOOK not configured")

    alerts = [
        Alert(
            symbol=symbol,
            alert_type='RSI_OVERBOUGHT',
            severity='CRITICAL',
            rule_name='RSI Overbought Rule',
            price=125000.0 + i * 1000,
            indicator_value=75.0,
            threshold=70.0,
            deviation_pct=5.0,
            message=f'{symbol} RSI đang ở vùng overbought: 75',
            alert_time=datetime.now(),
        )
        for i, symbol in enumerate(['FPT', 'VCB', 'HPG'])
    ]

    print(f"\n>>> Sending {len(alerts)} alerts to Slack...")

    for alert in alerts:
        result = slack_notifier.send_alert(alert)
        print(f">>> {alert.symbol}: {'✓' if result else '✗'}")
        assert result is True


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
