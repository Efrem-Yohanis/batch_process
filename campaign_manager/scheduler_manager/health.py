# scheduler_manager/health.py
from django.http import JsonResponse
from django.db import connection
from django.db.utils import OperationalError
from django.utils import timezone
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

def health_check(request):
    """
    Comprehensive health check endpoint for monitoring systems.
    
    Checks:
    - Database connectivity
    - Overall API health
    - Basic system status
    """
    health_status = {
        'status': 'healthy',
        'timestamp': timezone.now().isoformat(),
        'services': {
            'database': 'unknown',
            'api': 'operational',
        },
        'version': getattr(settings, 'API_VERSION', '1.0.0'),
        'environment': 'development' if settings.DEBUG else 'production',
    }
    
    # Check database connectivity
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        health_status['services']['database'] = 'connected'
    except OperationalError as e:
        health_status['status'] = 'degraded'
        health_status['services']['database'] = 'disconnected'
        logger.error(f"Health check database error: {e}")
    except Exception as e:
        health_status['status'] = 'degraded'
        health_status['services']['database'] = 'error'
        logger.error(f"Health check unexpected error: {e}")
    
    # Return appropriate HTTP status code
    http_status = 200 if health_status['status'] == 'healthy' else 503
    
    return JsonResponse(health_status, status=http_status)