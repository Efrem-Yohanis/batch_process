from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django.shortcuts import get_object_or_404

from .models import Campaign, Schedule, MessageContent, Audience
from .serializers import (
    CampaignSerializer, ScheduleSerializer, MessageContentSerializer, AudienceSerializer,
    CampaignScheduleSerializer, CampaignMessageContentSerializer, CampaignAudienceSerializer
)


class CampaignViewSet(viewsets.ModelViewSet):
    queryset = Campaign.objects.all().select_related('schedule', 'message_content', 'audience')
    serializer_class = CampaignSerializer

    @action(detail=True, methods=['get', 'patch'], url_path='message-content')
    def message_content(self, request, pk=None):
        """Retrieve or partially update the language map for a campaign."""
        campaign = self.get_object()
        
        # Check if message content exists
        if not hasattr(campaign, 'message_content'):
            return Response({"detail": "No message content found"}, status=404)
        
        mc = campaign.message_content
        
        if request.method == 'GET':
            # Use MessageContentSerializer directly, not CampaignSerializer
            serializer = MessageContentSerializer(mc)
            return Response(serializer.data)
        
        # PATCH/PUT
        serializer = MessageContentSerializer(mc, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data)

    @action(detail=True, methods=['get', 'post', 'put', 'patch'], url_path='schedule')
    def schedule_detail(self, request, pk=None):
        """Create, retrieve, update, or partially update schedule for a campaign"""
        campaign = self.get_object()
        
        if request.method == 'GET':
            if hasattr(campaign, 'schedule'):
                # Use ScheduleSerializer directly
                serializer = ScheduleSerializer(campaign.schedule)
                return Response(serializer.data)
            return Response({"detail": "No schedule found"}, status=404)
        
        elif request.method == 'POST':
            # Check if schedule already exists
            if hasattr(campaign, 'schedule'):
                return Response(
                    {"detail": "Schedule already exists. Use PUT or PATCH to update."}, 
                    status=400
                )
            # Create new schedule
            serializer = CampaignScheduleSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            schedule = Schedule.objects.create(campaign=campaign, **serializer.validated_data)
            # Return with ScheduleSerializer
            return Response(ScheduleSerializer(schedule).data, status=201)
        
        elif request.method == 'PUT':
            # Update existing schedule (full update)
            if not hasattr(campaign, 'schedule'):
                return Response({"detail": "No schedule found"}, status=404)
            schedule = campaign.schedule
            serializer = CampaignScheduleSerializer(schedule, data=request.data)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            # Return with ScheduleSerializer
            return Response(ScheduleSerializer(schedule).data)
        
        elif request.method == 'PATCH':
            # Partial update of existing schedule
            if not hasattr(campaign, 'schedule'):
                return Response({"detail": "No schedule found"}, status=404)
            schedule = campaign.schedule
            serializer = CampaignScheduleSerializer(schedule, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            # Return with ScheduleSerializer
            return Response(ScheduleSerializer(schedule).data)

    @action(detail=True, methods=['get', 'post', 'put', 'patch'], url_path='audience')
    def audience_detail(self, request, pk=None):
        """Create, retrieve, update, or partially update audience for a campaign"""
        campaign = self.get_object()
        
        if request.method == 'GET':
            if hasattr(campaign, 'audience'):
                # Use AudienceSerializer directly
                serializer = AudienceSerializer(campaign.audience)
                return Response(serializer.data)
            return Response({"detail": "No audience found"}, status=404)
        
        elif request.method == 'POST':
            # Check if audience already exists
            if hasattr(campaign, 'audience'):
                return Response(
                    {"detail": "Audience already exists. Use PUT or PATCH to update."}, 
                    status=400
                )
            # Create new audience
            serializer = CampaignAudienceSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            audience = Audience.objects.create(campaign=campaign, **serializer.validated_data)
            # Return with AudienceSerializer
            return Response(AudienceSerializer(audience).data, status=201)
        
        elif request.method == 'PUT':
            # Update existing audience (full update)
            if not hasattr(campaign, 'audience'):
                return Response({"detail": "No audience found"}, status=404)
            audience = campaign.audience
            serializer = CampaignAudienceSerializer(audience, data=request.data)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            # Return with AudienceSerializer
            return Response(AudienceSerializer(audience).data)
        
        elif request.method == 'PATCH':
            # Partial update of existing audience
            if not hasattr(campaign, 'audience'):
                return Response({"detail": "No audience found"}, status=404)
            audience = campaign.audience
            serializer = CampaignAudienceSerializer(audience, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            # Return with AudienceSerializer
            return Response(AudienceSerializer(audience).data)

    @action(detail=True, methods=['post'])
    def start(self, request, pk=None):
        """Manual Start: Sets status to running and enables the campaign"""
        campaign = self.get_object()
        
        # Check if schedule exists
        if not hasattr(campaign, 'schedule'):
            return Response({"error": "Cannot start campaign without a schedule."}, status=400)
        
        sched = campaign.schedule
        if sched.status == 'completed':
            return Response({"error": "Cannot start a completed campaign."}, status=400)
        
        sched.status = 'running'
        sched.is_active = True
        sched.save()
        return Response({"status": "running", "message": "Campaign is now active and running."})

    @action(detail=True, methods=['post'])
    def stop(self, request, pk=None):
        """Manual Stop: Pauses the campaign (used for daily off-hours or maintenance)"""
        campaign = self.get_object()
        
        if not hasattr(campaign, 'schedule'):
            return Response({"error": "No schedule found for this campaign."}, status=400)
        
        sched = campaign.schedule
        sched.status = 'stop'
        sched.is_active = False
        sched.save()
        return Response({"status": "stop", "message": "Campaign has been stopped/paused."})

    @action(detail=True, methods=['post'])
    def complete(self, request, pk=None):
        """Final Mark: Sets status to completed. Cannot be restarted."""
        campaign = self.get_object()
        
        if not hasattr(campaign, 'schedule'):
            return Response({"error": "No schedule found for this campaign."}, status=400)
        
        sched = campaign.schedule
        sched.status = 'completed'
        sched.is_active = False
        sched.save()
        return Response({"status": "completed", "message": "Campaign marked as finished."})


class ScheduleViewSet(viewsets.ReadOnlyModelViewSet):
    """
    A simple ViewSet for viewing all schedules.
    """
    queryset = Schedule.objects.all().select_related('campaign')
    serializer_class = ScheduleSerializer