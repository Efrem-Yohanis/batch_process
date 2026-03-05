from django.urls import path, include

urlpatterns = [
    path('api/', include('scheduler_manager.urls')),
]