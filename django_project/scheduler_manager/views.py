from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.db import transaction
from django.contrib.auth.models import User
from rest_framework import generics, permissions
from .serializers import UserSerializer
from django.core.exceptions import ValidationError
import logging

from .models import (
    Campaign, Schedule, MessageContent, Audience, 
    CampaignProgress, BatchStatus, MessageStatus, Checkpoint
)
from .serializers import (
    CampaignSerializer, ScheduleSerializer, MessageContentSerializer, AudienceSerializer,
    CampaignScheduleSerializer, CampaignMessageContentSerializer, CampaignAudienceSerializer,
    CampaignProgressSerializer, BatchStatusSerializer, MessageStatusSerializer,
    MessageStatusBulkCreateSerializer, CheckpointSerializer, CampaignActionSerializer
)

logger = logging.getLogger(__name__)


class CampaignViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing SMS campaigns.
    
    Provides CRUD operations plus custom actions for:
    - Managing schedule, message content, and audience
    - Starting, stopping, and completing campaigns
    - Viewing campaign progress and statistics
    """
    
    queryset = Campaign.objects.all().select_related(
        'schedule', 'message_content', 'audience', 'progress', 'checkpoint'
    )
    serializer_class = CampaignSerializer
    
    
    def get_queryset(self):
        """Filter queryset based on user and query parameters"""
        queryset = super().get_queryset()
        
        # Filter by status if provided
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        # Filter by date range if provided
        created_after = self.request.query_params.get('created_after')
        if created_after:
            queryset = queryset.filter(created_at__date__gte=created_after)
        
        created_before = self.request.query_params.get('created_before')
        if created_before:
            queryset = queryset.filter(created_at__date__lte=created_before)
        
        # Exclude deleted campaigns by default
        if not self.request.query_params.get('include_deleted'):
            queryset = queryset.filter(is_deleted=False)
        
        return queryset

    def perform_create(self, serializer):
        """Set the creator when creating a campaign"""
        serializer.save(created_by=self.request.user)
        logger.info(f"Campaign created by user {self.request.user.id}")

    @action(detail=True, methods=['get', 'patch', 'put'], url_path='message-content')
    def message_content_detail(self, request, pk=None):
        """
        Retrieve, partially update, or fully update the message content for a campaign.
        
        GET: Retrieve message content
        PATCH: Partially update message content
        PUT: Fully replace message content
        """
        campaign = self.get_object()
        
        # Check if campaign is in draft mode for modifications
        if request.method in ['PATCH', 'PUT'] and campaign.status != 'draft':
            return Response(
                {"detail": "Message content can only be modified when campaign is in 'draft' status"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Check if message content exists
        if not hasattr(campaign, 'message_content'):
            return Response(
                {"detail": "No message content found"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        message_content = campaign.message_content
        
        if request.method == 'GET':
            serializer = MessageContentSerializer(message_content)
            return Response(serializer.data)
        
        # PATCH or PUT
        partial = request.method == 'PATCH'
        serializer = MessageContentSerializer(
            message_content, 
            data=request.data, 
            partial=partial,
            context={'request': request}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        
        logger.info(f"Message content updated for campaign {campaign.id}")
        return Response(serializer.data)

    @action(detail=True, methods=['get', 'post', 'put', 'patch', 'delete'], url_path='schedule')
    def schedule_detail(self, request, pk=None):
        """
        Manage schedule for a campaign.
        
        GET: Retrieve schedule
        POST: Create new schedule
        PUT: Fully update existing schedule
        PATCH: Partially update existing schedule
        DELETE: Delete schedule (only in draft mode)
        """
        campaign = self.get_object()
        
        # Check if campaign is in draft mode for modifications
        if request.method in ['POST', 'PUT', 'PATCH', 'DELETE'] and campaign.status != 'draft':
            return Response(
                {"detail": f"Schedule can only be modified when campaign is in 'draft' status (current: {campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # GET request
        if request.method == 'GET':
            if hasattr(campaign, 'schedule'):
                serializer = ScheduleSerializer(campaign.schedule)
                return Response(serializer.data)
            return Response(
                {"detail": "No schedule found"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        # DELETE request
        if request.method == 'DELETE':
            if hasattr(campaign, 'schedule'):
                campaign.schedule.delete()
                logger.info(f"Schedule deleted for campaign {campaign.id}")
                return Response(
                    {"detail": "Schedule deleted successfully"},
                    status=status.HTTP_204_NO_CONTENT
                )
            return Response(
                {"detail": "No schedule found"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        # POST request (create)
        if request.method == 'POST':
            if hasattr(campaign, 'schedule'):
                return Response(
                    {"detail": "Schedule already exists. Use PUT or PATCH to update."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            serializer = CampaignScheduleSerializer(
                data=request.data,
                context={'request': request}
            )
            serializer.is_valid(raise_exception=True)
            
            with transaction.atomic():
                schedule = Schedule.objects.create(
                    campaign=campaign, 
                    **serializer.validated_data
                )
                logger.info(f"Schedule created for campaign {campaign.id}")
            
            # Return with full ScheduleSerializer
            output_serializer = ScheduleSerializer(schedule)
            return Response(output_serializer.data, status=status.HTTP_201_CREATED)
        
        # PUT or PATCH request (update)
        if not hasattr(campaign, 'schedule'):
            return Response(
                {"detail": "No schedule found"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        schedule = campaign.schedule
        partial = request.method == 'PATCH'
        
        serializer = CampaignScheduleSerializer(
            schedule, 
            data=request.data, 
            partial=partial,
            context={'request': request}
        )
        serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            serializer.save()
            logger.info(f"Schedule updated for campaign {campaign.id}")
        
        # Return with full ScheduleSerializer
        output_serializer = ScheduleSerializer(schedule)
        return Response(output_serializer.data)

    @action(detail=True, methods=['get', 'post', 'put', 'patch', 'delete'], url_path='audience')
    def audience_detail(self, request, pk=None):
        """
        Manage audience for a campaign.
        
        GET: Retrieve audience
        POST: Create new audience
        PUT: Fully update existing audience
        PATCH: Partially update existing audience
        DELETE: Delete audience (only in draft mode)
        """
        campaign = self.get_object()
        
        # Check if campaign is in draft mode for modifications
        if request.method in ['POST', 'PUT', 'PATCH', 'DELETE'] and campaign.status != 'draft':
            return Response(
                {"detail": f"Audience can only be modified when campaign is in 'draft' status (current: {campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # GET request
        if request.method == 'GET':
            if hasattr(campaign, 'audience'):
                serializer = AudienceSerializer(campaign.audience)
                return Response(serializer.data)
            return Response(
                {"detail": "No audience found"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        # DELETE request
        if request.method == 'DELETE':
            if hasattr(campaign, 'audience'):
                campaign.audience.delete()
                logger.info(f"Audience deleted for campaign {campaign.id}")
                return Response(
                    {"detail": "Audience deleted successfully"},
                    status=status.HTTP_204_NO_CONTENT
                )
            return Response(
                {"detail": "No audience found"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        # POST request (create)
        if request.method == 'POST':
            if hasattr(campaign, 'audience'):
                return Response(
                    {"detail": "Audience already exists. Use PUT or PATCH to update."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            serializer = CampaignAudienceSerializer(
                data=request.data,
                context={'request': request}
            )
            serializer.is_valid(raise_exception=True)
            
            with transaction.atomic():
                audience = Audience.objects.create(
                    campaign=campaign, 
                    **serializer.validated_data
                )
                # Update campaign progress total_messages
                if hasattr(campaign, 'progress'):
                    campaign.progress.total_messages = audience.total_count
                    campaign.progress.save()
                
                logger.info(f"Audience created for campaign {campaign.id} with {audience.total_count} recipients")
            
            # Return with full AudienceSerializer
            output_serializer = AudienceSerializer(audience)
            return Response(output_serializer.data, status=status.HTTP_201_CREATED)
        
        # PUT or PATCH request (update)
        if not hasattr(campaign, 'audience'):
            return Response(
                {"detail": "No audience found"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        audience = campaign.audience
        partial = request.method == 'PATCH'
        
        serializer = CampaignAudienceSerializer(
            audience, 
            data=request.data, 
            partial=partial,
            context={'request': request}
        )
        serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            serializer.save()
            # Update campaign progress total_messages
            if hasattr(campaign, 'progress'):
                campaign.progress.total_messages = audience.total_count
                campaign.progress.save()
            
            logger.info(f"Audience updated for campaign {campaign.id}")
        
        # Return with full AudienceSerializer
        output_serializer = AudienceSerializer(audience)
        return Response(output_serializer.data)

    @action(detail=True, methods=['post'])
    def start(self, request, pk=None):
        """
        Start a campaign.
        
        Validates that all required components are present and campaign is in draft state.
        """
        campaign = self.get_object()
        
        # Comprehensive validation
        errors = []
        
        if campaign.status != 'draft':
            errors.append(f"Campaign must be in 'draft' status to start (current: {campaign.status})")
        
        if not hasattr(campaign, 'schedule'):
            errors.append("Schedule is required")
        elif not campaign.schedule.is_active:
            errors.append("Schedule is not active")
            
        if not hasattr(campaign, 'message_content'):
            errors.append("Message content is required")
        else:
            # Check if content has actual text for default language
            default_lang = campaign.message_content.default_language
            if not campaign.message_content.content.get(default_lang):
                errors.append(f"Message content for default language '{default_lang}' cannot be empty")
        
        if not hasattr(campaign, 'audience'):
            errors.append("Audience is required")
        elif not campaign.audience.recipients:
            errors.append("Recipients list cannot be empty")
        elif campaign.audience.valid_count == 0:
            errors.append("No valid recipients found in audience")
        
        if errors:
            return Response(
                {"detail": "Cannot start campaign", "errors": errors},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Update campaign status
        campaign.status = 'active'
        campaign.save()
        
        # Update schedule
        schedule = campaign.schedule
        schedule.status = 'running'
        schedule.is_active = True
        schedule.save()
        
        # Update progress status
        if hasattr(campaign, 'progress'):
            campaign.progress.status = 'ACTIVE'
            campaign.progress.save()
        
        # Create initial checkpoint if needed
        if hasattr(campaign, 'checkpoint'):
            campaign.checkpoint.status = 'RUNNING'
            campaign.checkpoint.total_to_process = campaign.audience.total_count
            campaign.checkpoint.save()
        
        logger.info(f"Campaign {campaign.id} started by user {request.user.id}")
        
        return Response({
            "status": "active",
            "message": "Campaign is now active and running",
            "campaign_id": campaign.id,
            "total_recipients": campaign.audience.total_count,
            "valid_recipients": campaign.audience.valid_count
        })

    @action(detail=True, methods=['post'])
    def stop(self, request, pk=None):
        """
        Stop/pause a running campaign.
        """
        campaign = self.get_object()
        
        if campaign.status != 'active':
            return Response(
                {"detail": f"Can only stop active campaigns (current: {campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Update campaign status
        campaign.status = 'paused'
        campaign.save()
        
        # Update schedule
        if hasattr(campaign, 'schedule'):
            schedule = campaign.schedule
            schedule.status = 'stop'
            schedule.is_active = False
            schedule.save()
        
        # Update progress status
        if hasattr(campaign, 'progress'):
            campaign.progress.status = 'STOPPED'
            campaign.progress.save()
        
        # Update checkpoint
        if hasattr(campaign, 'checkpoint'):
            campaign.checkpoint.status = 'PAUSED'
            campaign.checkpoint.save()
        
        logger.info(f"Campaign {campaign.id} stopped by user {request.user.id}")
        
        return Response({
            "status": "paused",
            "message": "Campaign has been stopped/paused",
            "campaign_id": campaign.id
        })

    @action(detail=True, methods=['post'])
    def complete(self, request, pk=None):
        """
        Mark a campaign as completed.
        """
        campaign = self.get_object()
        
        if campaign.status not in ['active', 'paused']:
            return Response(
                {"detail": f"Can only complete active or paused campaigns (current: {campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Update campaign status
        campaign.status = 'completed'
        campaign.save()
        
        # Update schedule
        if hasattr(campaign, 'schedule'):
            schedule = campaign.schedule
            schedule.status = 'completed'
            schedule.is_active = False
            schedule.save()
        
        # Update progress
        if hasattr(campaign, 'progress'):
            campaign.progress.status = 'COMPLETED'
            campaign.progress.completed_at = timezone.now()
            campaign.progress.save()
        
        # Update checkpoint
        if hasattr(campaign, 'checkpoint'):
            campaign.checkpoint.status = 'COMPLETED'
            campaign.checkpoint.save()
        
        logger.info(f"Campaign {campaign.id} completed by user {request.user.id}")
        
        return Response({
            "status": "completed",
            "message": "Campaign marked as finished",
            "campaign_id": campaign.id
        })

    @action(detail=True, methods=['post'])
    def archive(self, request, pk=None):
        """
        Archive a completed campaign.
        """
        campaign = self.get_object()
        
        if campaign.status != 'completed':
            return Response(
                {"detail": "Can only archive completed campaigns"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        campaign.status = 'archived'
        campaign.save()
        
        logger.info(f"Campaign {campaign.id} archived by user {request.user.id}")
        
        return Response({
            "status": "archived",
            "message": "Campaign archived successfully",
            "campaign_id": campaign.id
        })

    @action(detail=True, methods=['post'])
    def soft_delete(self, request, pk=None):
        """
        Soft delete a campaign (only if not active).
        """
        campaign = self.get_object()
        
        if campaign.status == 'active':
            return Response(
                {"detail": "Cannot delete an active campaign. Stop it first."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        campaign.soft_delete()
        
        logger.info(f"Campaign {campaign.id} soft deleted by user {request.user.id}")
        
        return Response({
            "message": "Campaign deleted successfully",
            "campaign_id": campaign.id
        })

    @action(detail=True, methods=['get'])
    def progress(self, request, pk=None):
        """
        Get detailed progress information for a campaign.
        """
        campaign = self.get_object()
        
        if not hasattr(campaign, 'progress'):
            return Response(
                {"detail": "No progress data found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        progress = campaign.progress
        
        # Get batch summaries
        batches = BatchStatus.objects.filter(campaign=progress)
        batch_summary = {
            'total_batches': batches.count(),
            'completed_batches': batches.filter(status='COMPLETED').count(),
            'failed_batches': batches.filter(status='FAILED').count(),
            'in_progress_batches': batches.filter(status='IN_PROGRESS').count(),
        }
        
        data = {
            'campaign_id': campaign.id,
            'campaign_name': campaign.name,
            'progress': CampaignProgressSerializer(progress).data,
            'batches': batch_summary,
        }
        
        return Response(data)

    @action(detail=True, methods=['get'])
    def batches(self, request, pk=None):
        """
        Get all batches for a campaign.
        """
        campaign = self.get_object()
        
        if not hasattr(campaign, 'progress'):
            return Response(
                {"detail": "No progress data found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        batches = BatchStatus.objects.filter(campaign=campaign.progress)
        
        # Apply filters
        status_filter = request.query_params.get('status')
        if status_filter:
            batches = batches.filter(status=status_filter)
        
        page = self.paginate_queryset(batches)
        if page is not None:
            serializer = BatchStatusSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = BatchStatusSerializer(batches, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def checkpoint(self, request, pk=None):
        """
        Get checkpoint information for a campaign.
        """
        campaign = self.get_object()
        
        if not hasattr(campaign, 'checkpoint'):
            return Response(
                {"detail": "No checkpoint found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        serializer = CheckpointSerializer(campaign.checkpoint)
        return Response(serializer.data)

    @action(detail=False, methods=['get'])
    def summary(self, request):
        """
        Get summary statistics for all campaigns.
        """
        campaigns = self.get_queryset()
        
        summary = {
            'total': campaigns.count(),
            'by_status': {},
            'total_recipients': 0,
            'total_sent': 0,
        }
        
        for status_choice, _ in Campaign.STATUS_CHOICES:
            count = campaigns.filter(status=status_choice).count()
            if count > 0:
                summary['by_status'][status_choice] = count
        
        # Aggregate progress data
        from django.db.models import Sum
        progress_data = CampaignProgress.objects.filter(
            campaign_id__in=campaigns.values_list('id', flat=True)
        ).aggregate(
            total_recipients=Sum('total_messages'),
            total_sent=Sum('sent_count'),
            total_failed=Sum('failed_count')
        )
        
        summary['total_recipients'] = progress_data.get('total_recipients') or 0
        summary['total_sent'] = progress_data.get('total_sent') or 0
        summary['total_failed'] = progress_data.get('total_failed') or 0
        
        return Response(summary)


class ScheduleViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing all schedules.
    Provides read-only access to schedule data.
    """
    
    queryset = Schedule.objects.all().select_related('campaign')
    serializer_class = ScheduleSerializer
    
    
    def get_queryset(self):
        """Filter queryset based on query parameters"""
        queryset = super().get_queryset()
        
        # Filter by status
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        # Filter by active
        is_active = self.request.query_params.get('is_active')
        if is_active is not None:
            is_active = is_active.lower() == 'true'
            queryset = queryset.filter(is_active=is_active)
        
        # Filter by date range
        start_after = self.request.query_params.get('start_after')
        if start_after:
            queryset = queryset.filter(start_date__gte=start_after)
        
        start_before = self.request.query_params.get('start_before')
        if start_before:
            queryset = queryset.filter(start_date__lte=start_before)
        
        return queryset


class MessageStatusViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing message statuses.
    Provides read-only access with filtering capabilities.
    """
    
    queryset = MessageStatus.objects.all()
    serializer_class = MessageStatusSerializer
   
    
    def get_queryset(self):
        """Filter queryset based on query parameters"""
        queryset = super().get_queryset()
        
        # Filter by campaign
        campaign_id = self.request.query_params.get('campaign_id')
        if campaign_id:
            queryset = queryset.filter(campaign_id=campaign_id)
        
        # Filter by batch
        batch_id = self.request.query_params.get('batch_id')
        if batch_id:
            queryset = queryset.filter(batch_id=batch_id)
        
        # Filter by status
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        # Filter by phone number
        phone_number = self.request.query_params.get('phone_number')
        if phone_number:
            queryset = queryset.filter(phone_number=phone_number)
        
        # Filter by date range
        created_after = self.request.query_params.get('created_after')
        if created_after:
            queryset = queryset.filter(created_at__date__gte=created_after)
        
        created_before = self.request.query_params.get('created_before')
        if created_before:
            queryset = queryset.filter(created_at__date__lte=created_before)
        
        return queryset

    @action(detail=False, methods=['post'])
    def bulk_create(self, request):
        """
        Bulk create message statuses.
        This endpoint is intended for internal use by the messaging system.
        """
        serializer = MessageStatusBulkCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        messages = serializer.validated_data['messages']
        
        # Bulk create
        with transaction.atomic():
            created = []
            for message_data in messages:
                message_status = MessageStatus.objects.create(**message_data)
                created.append(message_status)
        
        logger.info(f"Bulk created {len(created)} message statuses")
        
        output_serializer = MessageStatusSerializer(created, many=True)
        return Response(output_serializer.data, status=status.HTTP_201_CREATED)


class CampaignProgressViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing campaign progress.
    """
    
    queryset = CampaignProgress.objects.all().select_related('campaign')
    serializer_class = CampaignProgressSerializer
   
    
    def get_queryset(self):
        """Filter queryset based on query parameters"""
        queryset = super().get_queryset()
        
        # Filter by status
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        # Filter by progress range
        progress_min = self.request.query_params.get('progress_min')
        if progress_min:
            queryset = queryset.filter(progress_percent__gte=progress_min)
        
        progress_max = self.request.query_params.get('progress_max')
        if progress_max:
            queryset = queryset.filter(progress_percent__lte=progress_max)
        
        return queryset


class BatchStatusViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing batch statuses.
    """
    
    queryset = BatchStatus.objects.all()
    serializer_class = BatchStatusSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter queryset based on query parameters"""
        queryset = super().get_queryset()
        
        # Filter by campaign
        campaign_id = self.request.query_params.get('campaign_id')
        if campaign_id:
            queryset = queryset.filter(campaign_id_direct=campaign_id)
        
        # Filter by status
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        return queryset
from rest_framework import viewsets
from django.contrib.auth.models import User
from .serializers import UserSerializer

class UserViewSet(viewsets.ModelViewSet):
    """
    ViewSet for viewing and editing users.
    """
    queryset = User.objects.all()
    serializer_class = UserSerializer
    
    def get_permissions(self):
        """Set permissions based on action"""
        if self.action == 'create':
            # Allow anyone to register
            permission_classes = [permissions.AllowAny]
        else:
            # Require authentication for other actions
            permission_classes = [permissions.IsAuthenticated]
        return [permission() for permission in permission_classes]
    
    @action(detail=False, methods=['get'])
    def me(self, request):
        """Get current user details"""
        serializer = self.get_serializer(request.user)
        return Response(serializer.data)
__all__ = [
    'CampaignViewSet',
    'ScheduleViewSet',
    'MessageStatusViewSet',
    'CampaignProgressViewSet',
    'BatchStatusViewSet',
    'UserViewSet'
]