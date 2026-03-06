from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    CampaignViewSet, ScheduleViewSet, MessageStatusViewSet,
    CampaignProgressViewSet, BatchStatusViewSet,UserViewSet
)


# Create router and register viewsets
router = DefaultRouter()
router.register(r'campaigns', CampaignViewSet, basename='campaign')
router.register(r'schedules', ScheduleViewSet, basename='schedule')
router.register(r'message-statuses', MessageStatusViewSet, basename='message-status')
router.register(r'campaign-progress', CampaignProgressViewSet, basename='campaign-progress')
router.register(r'batches', BatchStatusViewSet, basename='batch')
router.register(r'users', UserViewSet, basename='user')

urlpatterns = [
    # API root
    path('', include(router.urls)),
 
    # Additional endpoints can be added here if needed
    # path('health/', health_check, name='health-check'),
]
