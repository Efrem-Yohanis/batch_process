# Multi-stage build for all SMS layers
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

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder to system site-packages
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Create a non-root user for security
RUN useradd -m -u 1000 appuser

# Create log directories and set proper ownership
RUN mkdir -p /app/logs/layer1 /app/logs/layer2 /app/logs/layer3 && \
    chown -R appuser:appuser /app/logs && \
    chmod 755 /app/logs/layer1 /app/logs/layer2 /app/logs/layer3

# Copy all Python source files
COPY *.py .

# Make scripts executable and ensure proper ownership
RUN chmod +x *.py && \
    chown -R appuser:appuser /app

# Create entrypoint script
RUN echo '#!/bin/bash\n\
# Check if log directory is writable\n\
if [ -w "/app/logs" ]; then\n\
    echo "Log directory is writable"\n\
else\n\
    echo "Warning: Log directory may not be writable, but continuing..."\n\
fi\n\
\n\
# Execute the command passed to the entrypoint\n\
exec "$@"' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh && chown appuser:appuser /app/entrypoint.sh

# Switch to non-root user
USER appuser

# Set entrypoint to handle permission checks
ENTRYPOINT ["/app/entrypoint.sh"]

# Expose ports
EXPOSE 8001 8002

# Healthcheck (optional - can be overridden)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# Default command
CMD ["python", "-c", "print('Specify layer in docker-compose: layer1, layer2, or layer3')"]