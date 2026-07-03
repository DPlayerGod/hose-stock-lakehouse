"""Alert Detector configuration — reads from lakehouse .env."""

import os
from dotenv import load_dotenv

# Load .env from project root (two levels up from this file: alerts/ → streaming/ → stock_lakehouse/)
_dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '.env')
load_dotenv(dotenv_path=_dotenv_path)


class Config:
    """Config for Python Alert Detector, adapted for the lakehouse DB."""

    # --- ClickHouse ---
    CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_HTTP_PORT: int = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    CLICKHOUSE_USER: str = os.getenv('CLICKHOUSE_USER', 'admin')
    CLICKHOUSE_PASSWORD: str = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')
    CLICKHOUSE_DB: str = os.getenv('CLICKHOUSE_DB', 'lakehouse')

    # --- Slack ---
    SLACK_DNSE_WEBHOOK: str = os.getenv('SLACK_DNSE_WEBHOOK', '')

    # --- VWAP Bands ---
    # 'pct': threshold = ALERT_THRESHOLD_PCT % deviation from VWAP
    # 'sigma': threshold = k * sigma around VWAP
    ALERT_BAND_MODE: str = os.getenv('ALERT_BAND_MODE', 'sigma')
    ALERT_THRESHOLD_PCT: float = float(os.getenv('ALERT_THRESHOLD_PCT', 1.5))
    BAND_SIGMA_MULTIPLIER: float = float(os.getenv('BAND_SIGMA_MULTIPLIER', 2.0))

    # --- RSI ---
    RSI_PERIOD: int = int(os.getenv('RSI_PERIOD', 14))
    RSI_OVERBOUGHT: float = float(os.getenv('RSI_OVERBOUGHT', 70))
    RSI_OVERSOLD: float = float(os.getenv('RSI_OVERSOLD', 30))

    # --- Volume Spike ---
    VOLUME_LOOKBACK: int = int(os.getenv('VOLUME_LOOKBACK', 20))
    VOLUME_SPIKE_RATIO: float = float(os.getenv('VOLUME_SPIKE_RATIO', 3.0))

    # --- Runtime ---
    POLL_INTERVAL_SEC: int = int(os.getenv('POLL_INTERVAL_SEC', 10))
    ALERT_COOLDOWN_SEC: int = int(os.getenv('ALERT_COOLDOWN_SEC', 300))
    CANDLE_BUFFER_SIZE: int = int(os.getenv('CANDLE_BUFFER_SIZE', 500))

    # --- Symbols ---
    _DEFAULT_SYMBOLS = 'FPT,VCB,HPG,VNM,MWG'
    SYMBOLS: list = [
        s.strip()
        for s in os.getenv('ALERT_SYMBOLS', _DEFAULT_SYMBOLS).split(',')
        if s.strip()
    ]
