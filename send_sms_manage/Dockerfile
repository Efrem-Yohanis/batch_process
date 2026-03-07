# sms-sender-manager Dockerfile (for Layer 1 + Layer 2)
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (for asyncpg, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create log directories
RUN mkdir -p /app/logs/layer1 /app/logs/layer2

# Copy all Python source files
COPY *.py .

# Make scripts executable
RUN chmod +x *.py

# Expose Layer 1 API port
EXPOSE 8001

# Default command is Layer 1 (FastAPI)
# Layer 2 will override this in docker-compose with a different CMD
CMD ["uvicorn", "layer1_ingestion:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]