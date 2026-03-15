#!/bin/sh
set -e

echo "========================================"
echo "Layer 4: Status Updater Service"
echo "Version: $(python -c "import app; print(app.__version__)" 2>/dev/null || echo '1.0.0')"
echo "========================================"

# Run database migrations check (optional - if you have migrations)
# python -m app.migrations check

# Run in standalone mode if STANDALONE=true, otherwise run with web server
if [ "$STANDALONE" = "true" ]; then
    echo "Starting in standalone consumer mode..."
    echo "Mode: Consumer only (no web server)"
    echo "========================================"
    exec python -m app.main --standalone
else
    echo "Starting with web server..."
    echo "Mode: API + Consumer"
    echo "HTTP Port: ${HTTP_PORT:-8003}"
    echo "========================================"
    exec uvicorn app.main:app \
        --host 0.0.0.0 \
        --port ${HTTP_PORT:-8003} \
        --workers 1 \
        --loop asyncio \
        --log-level ${LOG_LEVEL,,}
fi