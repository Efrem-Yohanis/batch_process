#!/bin/bash
echo "🔧 Fixing Django and database issues..."

cd /home/efrem/my-scheduler-app

# Fix Django command in docker-compose (already fixed above)

# Ensure campaign database exists with correct name
echo "Creating campaign database if needed..."
docker exec -it campaign-postgres psql -U postgres -c "CREATE DATABASE campaign_db OWNER campaign_user;" 2>/dev/null || true
docker exec -it campaign-postgres psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE campaign_db TO campaign_user;"

# Run Django migrations manually
echo "Running Django migrations..."
docker exec -it django-campaign-manager python manage.py migrate

# Test Django API
echo "Testing Django API..."
curl -f http://localhost:8000/api/scheduler/campaigns/ || echo "Django not ready yet"

echo "✅ Fixes applied!"