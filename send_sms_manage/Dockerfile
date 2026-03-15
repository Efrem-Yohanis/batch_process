# Multi-stage build for all SMS layers (Layer 1, 2, 3, and 4)
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies and useful tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    curl \
    postgresql-client \
    kafkacat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder to system site-packages
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Create a non-root user for security
RUN useradd -m -u 1000 appuser

# Create log directories for all layers and set proper ownership
RUN mkdir -p /app/logs/layer1 /app/logs/layer2 /app/logs/layer3 /app/logs/layer4 && \
    chown -R appuser:appuser /app/logs && \
    chmod 755 /app/logs/layer1 /app/logs/layer2 /app/logs/layer3 /app/logs/layer4

# Copy all Python source files
COPY *.py .

# Make scripts executable and ensure proper ownership
RUN chmod +x *.py && \
    chown -R appuser:appuser /app

# Create healthcheck scripts for each layer
RUN echo '#!/bin/bash\n\
# Healthcheck for Layer 1 - Command Ingestion\n\
curl -f http://localhost:8001/health || exit 1' > /app/healthcheck-layer1.sh && chmod +x /app/healthcheck-layer1.sh

RUN echo '#!/bin/bash\n\
# Healthcheck for Layer 2 - Execution Engine\n\
curl -f http://localhost:8002/health || exit 1' > /app/healthcheck-layer2.sh && chmod +x /app/healthcheck-layer2.sh

RUN echo '#!/bin/bash\n\
# Healthcheck for Layer 3 - SMS Sender\n\
curl -f http://localhost:8003/health || exit 1' > /app/healthcheck-layer3.sh && chmod +x /app/healthcheck-layer3.sh

RUN echo '#!/bin/bash\n\
# Healthcheck for Layer 4 - Status Updater\n\
curl -f http://localhost:8004/health || exit 1' > /app/healthcheck-layer4.sh && chmod +x /app/healthcheck-layer4.sh

# Create diagnostic script
RUN echo '#!/bin/bash\n\
echo "=== Layer Diagnostics ===="\n\
echo "Hostname: $(hostname)"\n\
echo "Date: $(date)"\n\
echo "User: $(whoami)"\n\
echo "Python: $(python --version)"\n\
echo "\n--- Environment Variables ---"\n\
env | sort\n\
echo "\n--- Kafka Connectivity ---"\n\
if [ -n "$KAFKA_BOOTSTRAP_SERVERS" ]; then\n\
    echo "Testing Kafka: $KAFKA_BOOTSTRAP_SERVERS"\n\
    kafkacat -L -b $KAFKA_BOOTSTRAP_SERVERS -t _kafkacat_test 2>&1 | head -20\n\
else\n\
    echo "KAFKA_BOOTSTRAP_SERVERS not set"\n\
fi\n\
echo "\n--- Redis Connectivity ---"\n\
if [ -n "$REDIS_URL" ]; then\n\
    echo "Testing Redis: $REDIS_URL"\n\
    python -c "import redis; r=redis.from_url(\"$REDIS_URL\"); print(r.ping())" 2>&1\n\
else\n\
    echo "REDIS_URL not set"\n\
fi\n\
echo "\n--- PostgreSQL Connectivity ---"\n\
if [ -n "$POSTGRES_HOST" ]; then\n\
    echo "Testing PostgreSQL: $POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"\n\
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 1" 2>&1\n\
else\n\
    echo "POSTGRES_HOST not set"\n\
fi\n\
echo "\n--- Log Directory ---"\n\
ls -la /app/logs/\n\
echo "==========================="' > /app/diagnostic.sh && chmod +x /app/diagnostic.sh

# Create entrypoint script with layer selection
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Function to check required environment variables\n\
check_required_vars() {\n\
    local layer=$1\n\
    local missing=0\n\
    \n\
    case $layer in\n\
        layer1)\n\
            for var in KAFKA_BOOTSTRAP_SERVERS DJANGO_URL; do\n\
                if [ -z "${!var}" ]; then\n\
                    echo "ERROR: $var is required for Layer 1"\n\
                    missing=1\n\
                fi\n\
            done\n\
            ;;\n\
        layer2)\n\
            for var in KAFKA_BOOTSTRAP_SERVERS REDIS_URL DJANGO_URL; do\n\
                if [ -z "${!var}" ]; then\n\
                    echo "ERROR: $var is required for Layer 2"\n\
                    missing=1\n\
                fi\n\
            done\n\
            ;;\n\
        layer3)\n\
            for var in KAFKA_BOOTSTRAP_SERVERS REDIS_URL API_URL API_KEY; do\n\
                if [ -z "${!var}" ]; then\n\
                    echo "ERROR: $var is required for Layer 3"\n\
                    missing=1\n\
                fi\n\
            done\n\
            ;;\n\
        layer4)\n\
            for var in KAFKA_BOOTSTRAP_SERVERS POSTGRES_HOST POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB; do\n\
                if [ -z "${!var}" ]; then\n\
                    echo "ERROR: $var is required for Layer 4"\n\
                    missing=1\n\
                fi\n\
            done\n\
            ;;\n\
    esac\n\
    \n\
    if [ $missing -eq 1 ]; then\n\
        echo "Exiting due to missing required environment variables"\n\
        exit 1\n\
    fi\n\
}\n\
\n\
# Function to wait for dependencies\n\
wait_for_dependencies() {\n\
    local layer=$1\n\
    local retries=30\n\
    local wait=2\n\
    \n\
    case $layer in\n\
        layer2|layer3)\n\
            # Wait for Redis\n\
            if [ -n "$REDIS_URL" ]; then\n\
                echo "Waiting for Redis..."\n\
                until redis-cli -u "$REDIS_URL" ping > /dev/null 2>&1; do\n\
                    retries=$((retries - 1))\n\
                    if [ $retries -eq 0 ]; then\n\
                        echo "ERROR: Redis not available after 60 seconds"\n\
                        exit 1\n\
                    fi\n\
                    echo "Redis not ready, retrying in $wait seconds..."\n\
                    sleep $wait\n\
                done\n\
                echo "✓ Redis is ready"\n\
            fi\n\
            ;;&\n\
        layer1|layer2|layer3|layer4)\n\
            # Wait for Kafka\n\
            if [ -n "$KAFKA_BOOTSTRAP_SERVERS" ]; then\n\
                echo "Waiting for Kafka..."\n\
                retries=30\n\
                until kafkacat -L -b "$KAFKA_BOOTSTRAP_SERVERS" -t _kafkacat_test > /dev/null 2>&1; do\n\
                    retries=$((retries - 1))\n\
                    if [ $retries -eq 0 ]; then\n\
                        echo "ERROR: Kafka not available after 60 seconds"\n\
                        exit 1\n\
                    fi\n\
                    echo "Kafka not ready, retrying in $wait seconds..."\n\
                    sleep $wait\n\
                done\n\
                echo "✓ Kafka is ready"\n\
            fi\n\
            ;;&\n\
        layer4)\n\
            # Wait for PostgreSQL\n\
            if [ -n "$POSTGRES_HOST" ]; then\n\
                echo "Waiting for PostgreSQL..."\n\
                retries=30\n\
                until PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1" > /dev/null 2>&1; do\n\
                    retries=$((retries - 1))\n\
                    if [ $retries -eq 0 ]; then\n\
                        echo "ERROR: PostgreSQL not available after 60 seconds"\n\
                        exit 1\n\
                    fi\n\
                    echo "PostgreSQL not ready, retrying in $wait seconds..."\n\
                    sleep $wait\n\
                done\n\
                echo "✓ PostgreSQL is ready"\n\
            fi\n\
            ;;\n\
    esac\n\
}\n\
\n\
# Parse layer from command or environment\n\
LAYER=${LAYER:-${1:-}}\n\
if [ -z "$LAYER" ]; then\n\
    echo "ERROR: No layer specified"\n\
    echo "Usage: docker run ... [layer1|layer2|layer3|layer4|diagnostic]"   \n\
    echo ""\n\
    echo "Available layers:"\n\
    echo "  layer1 - Command Ingestion (Airflow)"\n\
    echo "  layer2 - Execution Engine"\n\
    echo "  layer3 - SMS Sender (4000 TPS)"\n\
    echo "  layer4 - Status Updater"\n\
    echo "  diagnostic - Run diagnostics"\n\
    exit 1\n\
fi\n\
\n\
# Remove layer from arguments if present\n\
if [ "$1" = "$LAYER" ]; then\n\
    shift\n\
fi\n\
\n\
# Handle diagnostic mode\n\
if [ "$LAYER" = "diagnostic" ]; then\n\
    /app/diagnostic.sh\n\
    exit 0\n\
fi\n\
\n\
# Check required environment variables\n\
check_required_vars "$LAYER"\n\
\n\
# Wait for dependencies\n\
wait_for_dependencies "$LAYER"\n\
\n\
# Check if log directory is writable\n\
if [ -w "/app/logs" ]; then\n\
    echo "✓ Log directory is writable"\n\
else\n\
    echo "Warning: Log directory may not be writable, but continuing..."\n\
fi\n\
\n\
# Execute the appropriate layer\n\
echo "Starting Layer: $LAYER"\n\
case $LAYER in\n\
    layer1)\n\
        echo "Running Layer 1 - Command Ingestion"\n\
        exec python layer1_ingestion.py "$@"\n\
        ;;\n\
    layer2)\n\
        echo "Running Layer 2 - Execution Engine"\n\
        exec python layer2_execution.py "$@"\n\
        ;;\n\
    layer3)\n\
        echo "Running Layer 3 - SMS Sender (4000 TPS)"\n\
        exec python layer3_sender.py "$@"\n\
        ;;\n\
    layer4)\n\
        echo "Running Layer 4 - Status Updater"\n\
        # Layer 4 can run in two modes\n\
        if [ "$1" = "--standalone" ]; then\n\
            echo "  Mode: Standalone Consumer (no API)"\n\
            exec python layer4_updater.py --standalone\n\
        else\n\
            echo "  Mode: API Server + Consumer"\n\
            exec python layer4_updater.py\n\
        fi\n\
        ;;\n\
    *)\n\
        echo "ERROR: Unknown layer: $LAYER"\n\
        echo "Valid layers: layer1, layer2, layer3, layer4, diagnostic"\n\
        exit 1\n\
        ;;\nesac' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh && chown appuser:appuser /app/entrypoint.sh

# Switch to non-root user
USER appuser

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (shows help)
CMD ["help"]

# Expose ports for all layers
EXPOSE 8001 8002 8003 8004

# Healthcheck is now layer-specific and set in docker-compose