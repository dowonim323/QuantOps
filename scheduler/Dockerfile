FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    supervisor \
    tzdata \
    ca-certificates \
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
COPY strategies/ /app/strategies/
COPY pipelines/ /app/pipelines/

# Set python path
ENV PYTHONPATH=/app

COPY scheduler/entrypoint.sh /entrypoint.sh
COPY scheduler/supervisord.conf /etc/supervisor/quantops-supervisord.conf

# Set permissions
RUN chmod +x /entrypoint.sh \
    && chmod 0644 /etc/supervisor/quantops-supervisord.conf

ENTRYPOINT ["/entrypoint.sh"]
