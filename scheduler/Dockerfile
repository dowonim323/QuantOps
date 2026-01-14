FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for Chrome + cron + timezone
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    tzdata \
    wget \
    gnupg \
    unzip \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome using modern keyring method (apt-key is deprecated)
RUN wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# Set timezone to Asia/Seoul (KST)
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install Python dependencies
COPY scheduler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY tools/ /app/tools/
COPY pykis/ /app/pykis/
COPY financial_crawler.py /app/
COPY stock_selection.py /app/
COPY autorebalance.py /app/

# Set python path
ENV PYTHONPATH=/app

# Copy cron configuration and entrypoint
COPY scheduler/crontab /etc/cron.d/quantops-cron
COPY scheduler/entrypoint.sh /entrypoint.sh

# Set permissions
RUN chmod 0644 /etc/cron.d/quantops-cron \
    && chmod +x /entrypoint.sh \
    && crontab /etc/cron.d/quantops-cron

# Create log file for cron output
RUN touch /var/log/cron.log

ENTRYPOINT ["/entrypoint.sh"]
