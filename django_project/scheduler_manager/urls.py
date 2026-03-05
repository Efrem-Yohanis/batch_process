from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import CampaignViewSet, ScheduleViewSet

router = DefaultRouter()
router.register(r'campaigns', CampaignViewSet, basename='campaign')
router.register(r'schedules', ScheduleViewSet, basename='schedule')

urlpatterns = [
    path('', include(router.urls)),
]