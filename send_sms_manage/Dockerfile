# sms-sender-manager Dockerfile (for Layer 1 + Layer 2)
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Python source files
COPY *.py .

# Expose Layer 1 API port
EXPOSE 8001

# Default command is Layer 1 (FastAPI)
# Layer 2 will override this in docker-compose with a different CMD
CMD ["uvicorn", "layer1_ingestion:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]