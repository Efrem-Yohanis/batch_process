#!/bin/bash
set -e

echo "========================================="
echo "SMS Layer 1 - Command Ingestion Service"
echo "========================================="

# Run the application
exec uvicorn app.main:app --host 0.0.0.0 --port 8001
