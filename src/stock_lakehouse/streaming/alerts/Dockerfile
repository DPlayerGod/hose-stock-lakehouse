FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Cài runtime deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY src/stock_lakehouse/streaming/alerts/requirements.txt ./alerts_requirements.txt
RUN pip install --no-cache-dir --break-system-packages -r alerts_requirements.txt

# Copy source code
COPY src/ /app/src/

# Health check: liveness probe trên /health
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

# Expose health server port
EXPOSE 8080

# Chạy supervisor (điều phối detector + health server trong 1 container)
CMD ["python", "-m", "stock_lakehouse.streaming.alerts.supervisor"]
