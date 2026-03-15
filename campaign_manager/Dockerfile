# django_project/Dockerfile
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file from django_project directory
COPY django_project/requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir django==4.2.7  # Explicitly install Django as backup

# Copy the entire django_project
COPY django_project /app/

# Create a simple check script
RUN echo '#!/bin/bash\n\
echo "Checking Django installation..."\n\
python -c "import django; print(f\"Django version: {django.get_version()}\")"\n\
echo "Waiting for database at $DB_HOST:$DB_PORT..."\n\
while ! nc -z $DB_HOST $DB_PORT; do\n\
  sleep 1\n\
done\n\
echo "Database ready!"\n\
python manage.py migrate --noinput\n\
echo "✅ Migrations complete"\n\
echo "Starting Django server..."\n\
exec python manage.py runserver 0.0.0.0:8000' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# Run the application
CMD ["/app/entrypoint.sh"]