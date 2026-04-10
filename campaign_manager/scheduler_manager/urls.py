from django.urls import path, include
from rest_framework.routers import DefaultRouter
from drf_spectacular.utils import extend_schema
from .health import health_check
from .views import (
    CampaignViewSet, 
    ScheduleViewSet, 
    MessageStatusViewSet,
    CampaignProgressViewSet, 
    BatchStatusViewSet, 
    CheckpointViewSet,
    UserViewSet,
    AudienceViewSet,
    MessageContentViewSet
)

# Create router and register viewsets
router = DefaultRouter()

# Campaign management
router.register(r'campaigns', CampaignViewSet, basename='campaign')

# Schedule management (full CRUD)
router.register(r'schedules', ScheduleViewSet, basename='schedule')

# Audience management (full CRUD)
router.register(r'audiences', AudienceViewSet, basename='audience')

# Message content management (full CRUD)
router.register(r'message-contents', MessageContentViewSet, basename='message-content')

# Message status tracking (read-only)
router.register(r'message-statuses', MessageStatusViewSet, basename='message-status')

# Progress and monitoring
router.register(r'campaign-progress', CampaignProgressViewSet, basename='campaign-progress')
router.register(r'batches', BatchStatusViewSet, basename='batch')
router.register(r'checkpoints', CheckpointViewSet, basename='checkpoint')

# User management
router.register(r'users', UserViewSet, basename='user')

# =============== URL Patterns ===============
urlpatterns = [
    # API root - includes all router URLs
    path('', include(router.urls)),
    
    # Health check endpoint (public)
    path('health/', health_check, name='health-check'),
    
    # Authentication endpoints (DRF login/logout - optional)
    path('auth/', include('rest_framework.urls')),
]

# Optional: Add API root view with links to all endpoints
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.reverse import reverse

@api_view(['GET'])
@permission_classes([AllowAny])
def api_root(request, format=None):
    """API root endpoint with links to all resources"""
    return Response({
        'campaigns': reverse('campaign-list', request=request, format=format),
        'schedules': reverse('schedule-list', request=request, format=format),
        'audiences': reverse('audience-list', request=request, format=format),
        'message_contents': reverse('message-content-list', request=request, format=format),
        'message_statuses': reverse('message-status-list', request=request, format=format),
        'campaign_progress': reverse('campaign-progress-list', request=request, format=format),
        'batches': reverse('batch-list', request=request, format=format),
        'checkpoints': reverse('checkpoint-list', request=request, format=format),
        'users': reverse('user-list', request=request, format=format),
        'health': reverse('health-check', request=request, format=format),
        'documentation': {
            'swagger': reverse('swagger-ui', request=request, format=format),
            'redoc': reverse('redoc', request=request, format=format),
        }
    })

# Add api_root to urlpatterns
urlpatterns += [
    path('', api_root, name='api-root'),
]

__all__ = [
    'urlpatterns',
    'router',
    'api_root',
]