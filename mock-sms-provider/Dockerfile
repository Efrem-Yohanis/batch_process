FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir fastapi==0.104.1 uvicorn[standard]==0.24.0 pydantic==2.4.2

# Copy application
COPY mock_sms_provider.py .

# Expose port
EXPOSE 8003

# Run the application
CMD ["python", "mock_sms_provider.py"]