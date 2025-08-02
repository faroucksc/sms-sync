FROM python:3.9-slim

WORKDIR /app

# Install cron and dependencies
RUN apt-get update && apt-get -y install cron && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY sync.py .
COPY config.py .
COPY db.py .
COPY cloudflare.py .
COPY utils.py .
COPY health.py .

# Setup cron job
COPY crontab /etc/cron.d/sync-cron
RUN chmod 0644 /etc/cron.d/sync-cron
RUN crontab /etc/cron.d/sync-cron

# Create log directory
RUN mkdir -p /app/logs

# Expose port for health checks (optional)
EXPOSE 8080

# Copy and setup start script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Run the start script
CMD ["/app/start.sh"]