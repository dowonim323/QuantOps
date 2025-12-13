FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY web/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code (as fallback, will be overridden by volumes)
COPY web/ /app/web/
COPY tools/ /app/tools/
COPY pykis/ /app/pykis/

# Set python path to include root
ENV PYTHONPATH=/app

# Run gunicorn
CMD ["gunicorn", "-c", "web/gunicorn_config.py", "web.app:app"]
