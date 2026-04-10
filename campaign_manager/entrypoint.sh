#!/bin/bash
set -e

echo "Starting SMS Layer 1 - Command Ingestion Service..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8001
