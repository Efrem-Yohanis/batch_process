FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for asyncpg
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Create log directory
RUN mkdir -p /app/logs/layer2

# Run as non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose ports (main app and health)
EXPOSE 8002 8003

# Run the application
CMD ["python", "-m", "app.main"]