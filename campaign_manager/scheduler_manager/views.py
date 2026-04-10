from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, AllowAny
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.db import transaction
from django.db.models import Sum, Count, Q, F
from django.contrib.auth.models import User
from rest_framework import generics, permissions
from .serializers import (
    UserSerializer, ChannelChoiceSerializer, CampaignActionSerializer
)
from django.core.exceptions import ValidationError
import logging
from datetime import datetime, timedelta

# DRF Spectacular imports for Swagger
from drf_spectacular.utils import extend_schema, extend_schema_view, OpenApiParameter, OpenApiExample, OpenApiResponse
from drf_spectacular.types import OpenApiTypes

from .models import (
    Campaign, Schedule, MessageContent, Audience, 
    CampaignProgress, BatchStatus, MessageStatus, Checkpoint,
    SUPPORTED_LANGUAGES
)
from .serializers import (
    CampaignSerializer, ScheduleSerializer, MessageContentSerializer, AudienceSerializer,
    CampaignScheduleSerializer, CampaignMessageContentSerializer, CampaignAudienceSerializer,
    CampaignProgressSerializer, BatchStatusSerializer, MessageStatusSerializer,
    MessageStatusBulkCreateSerializer, CheckpointSerializer
)

logger = logging.getLogger(__name__)


# =============== CAMPAIGN VIEWSET WITH SWAGGER DECORATORS ===============

@extend_schema_view(
    list=extend_schema(
        tags=['📱 Campaigns'],
        summary='List all campaigns',
        description='Retrieve a paginated list of all campaigns with optional filtering by status, execution status, channel, sender_id, and date ranges.',
        parameters=[
            OpenApiParameter(name='status', type=str, location='query', 
                           description='Filter by campaign status',
                           enum=['draft', 'active', 'paused', 'completed', 'archived']),
            OpenApiParameter(name='execution_status', type=str, location='query',
                           description='Filter by execution status',
                           enum=['PENDING', 'PROCESSING', 'PAUSED', 'STOPPED', 'COMPLETED', 'FAILED']),
            OpenApiParameter(name='channel', type=str, location='query',
                           description='Filter by channel',
                           enum=['sms', 'app_notification', 'flash_sms']),
            OpenApiParameter(name='sender_id', type=str, location='query',
                           description='Filter by sender ID'),
            OpenApiParameter(name='created_after', type=str, location='query',
                           description='Filter campaigns created after this date (YYYY-MM-DD)'),
            OpenApiParameter(name='created_before', type=str, location='query',
                           description='Filter campaigns created before this date (YYYY-MM-DD)'),
        ],
        responses={200: CampaignSerializer(many=True)},
    ),
    retrieve=extend_schema(
        tags=['📱 Campaigns'],
        summary='Get campaign details',
        description='Retrieve detailed information about a specific campaign including its schedule, message content, audience, and progress.',
        responses={200: CampaignSerializer(), 404: OpenApiResponse(description='Campaign not found')},
    ),
    create=extend_schema(
        tags=['📱 Campaigns'],
        summary='Create a new campaign',
        description='Create a new campaign in draft status. Required fields: name, channels. Optional: sender_id.',
        examples=[
            OpenApiExample(
                'Basic Campaign Example',
                value={
                    'name': 'Summer Sale Campaign',
                    'sender_id': 'SMSINFO',
                    'channels': ['sms', 'app_notification']
                },
                request_only=True,
            ),
            OpenApiExample(
                'Full Campaign Example',
                value={
                    'name': 'Holiday Promotion',
                    'sender_id': 'HOLIDAY',
                    'channels': ['sms', 'flash_sms']
                },
                request_only=True,
            ),
        ],
        responses={201: CampaignSerializer(), 400: OpenApiResponse(description='Validation error')},
    ),
    update=extend_schema(
        tags=['📱 Campaigns'],
        summary='Update a campaign',
        description='Update campaign details. Only allowed when campaign is in draft status.',
    ),
    partial_update=extend_schema(
        tags=['📱 Campaigns'],
        summary='Partially update a campaign',
        description='Partially update campaign details. Only allowed when campaign is in draft status.',
    ),
    destroy=extend_schema(
        tags=['📱 Campaigns'],
        summary='Delete a campaign',
        description='Soft delete a campaign. Only allowed for non-active campaigns.',
    ),
)
class CampaignViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing SMS campaigns.
    
    Provides CRUD operations plus custom actions for:
    - Managing schedule, message content, and audience
    - Starting, stopping, pausing, resuming, and completing campaigns
    - Viewing campaign progress and statistics
    - Channel management
    - Execution status tracking
    """
    
    queryset = Campaign.objects.all().select_related(
        'schedule', 'message_content', 'audience', 'progress', 'checkpoint'
    )
    serializer_class = CampaignSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter queryset based on user and query parameters"""
        queryset = super().get_queryset()
        
        # Filter by status if provided
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        # Filter by execution_status if provided
        execution_status = self.request.query_params.get('execution_status')
        if execution_status:
            queryset = queryset.filter(execution_status=execution_status)
        
        # Filter by channel if provided
        channel = self.request.query_params.get('channel')
        if channel:
            queryset = queryset.filter(channels__contains=[channel])
        
        # Filter by sender_id if provided
        sender_id = self.request.query_params.get('sender_id')
        if sender_id:
            queryset = queryset.filter(sender_id=sender_id)
        
        # Filter by date range if provided
        created_after = self.request.query_params.get('created_after')
        if created_after:
            queryset = queryset.filter(created_at__date__gte=created_after)
        
        created_before = self.request.query_params.get('created_before')
        if created_before:
            queryset = queryset.filter(created_at__date__lte=created_before)
        
        # Filter by execution date range
        executed_after = self.request.query_params.get('executed_after')
        if executed_after:
            queryset = queryset.filter(execution_started_at__date__gte=executed_after)
        
        executed_before = self.request.query_params.get('executed_before')
        if executed_before:
            queryset = queryset.filter(execution_started_at__date__lte=executed_before)
        
        # Exclude deleted campaigns by default
        if not self.request.query_params.get('include_deleted'):
            queryset = queryset.filter(is_deleted=False)
        
        return queryset

    def perform_create(self, serializer):
        """Set the creator when creating a campaign"""
        serializer.save(created_by=self.request.user)
        logger.info(f"Campaign created by user {self.request.user.id}")

    @extend_schema(
        tags=['📱 Campaigns'],
        summary='Get channel choices',
        description='Get available channel choices for campaigns. Useful for frontend dropdowns.',
        responses={200: ChannelChoiceSerializer(many=True)},
    )
    @action(detail=False, methods=['get'], url_path='channel-choices')
    def channel_choices(self, request):
        """
        Get available channel choices for campaigns.
        Useful for frontend dropdowns.
        """
        return Response(ChannelChoiceSerializer.get_choices())
    
    @extend_schema(
        tags=['📱 Campaigns'],
        summary='Get execution status choices',
        description='Get available execution status choices for campaigns.',
        responses={200: OpenApiTypes.OBJECT},
    )
    @action(detail=False, methods=['get'], url_path='execution-status-choices')
    def execution_status_choices(self, request):
        """
        Get available execution status choices.
        """
        return Response([
            {'value': status[0], 'display': status[1]} 
            for status in Campaign.EXECUTION_STATUS_CHOICES
        ])

    @extend_schema(
        tags=['📝 Message Contents'],
        summary='Manage campaign message content',
        description='Retrieve, partially update, or fully update the message content for a campaign.',
        methods=['GET'],
        responses={200: MessageContentSerializer()},
    )
    @extend_schema(
        methods=['PATCH', 'PUT'],
        request=MessageContentSerializer,
        responses={200: MessageContentSerializer(), 400: OpenApiResponse(description='Validation error')},
    )
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

    @extend_schema(
        tags=['⏰ Schedules'],
        summary='Manage campaign schedule',
        description='Retrieve, create, update, or delete the schedule for a campaign.',
        methods=['GET'],
        responses={200: ScheduleSerializer()},
    )
    @extend_schema(
        methods=['POST'],
        request=CampaignScheduleSerializer,
        responses={201: ScheduleSerializer(), 400: OpenApiResponse(description='Validation error')},
    )
    @extend_schema(
        methods=['PUT', 'PATCH'],
        request=CampaignScheduleSerializer,
        responses={200: ScheduleSerializer(), 400: OpenApiResponse(description='Validation error')},
    )
    @extend_schema(
        methods=['DELETE'],
        responses={204: OpenApiResponse(description='Schedule deleted successfully')},
    )
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
                # Initialize the schedule
                schedule.initialize_schedule()
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
            # Recalculate next run
            schedule._calculate_next_run()
            logger.info(f"Schedule updated for campaign {campaign.id}")
        
        # Return with full ScheduleSerializer
        output_serializer = ScheduleSerializer(schedule)
        return Response(output_serializer.data)

    @extend_schema(
        tags=['👥 Audiences'],
        summary='Manage campaign audience',
        description='Retrieve, create, update, or delete the audience for a campaign.',
        methods=['GET'],
        responses={200: AudienceSerializer()},
    )
    @extend_schema(
        methods=['POST'],
        request=CampaignAudienceSerializer,
        responses={201: AudienceSerializer(), 400: OpenApiResponse(description='Validation error')},
        examples=[
            OpenApiExample(
                'Audience Example',
                value={
                    'recipients': [
                        {'msisdn': '+251912345678', 'lang': 'en'},
                        {'msisdn': '+251911223344', 'lang': 'am'},
                    ]
                },
                request_only=True,
            ),
        ],
    )
    @extend_schema(
        methods=['PUT', 'PATCH'],
        request=CampaignAudienceSerializer,
        responses={200: AudienceSerializer(), 400: OpenApiResponse(description='Validation error')},
    )
    @extend_schema(
        methods=['DELETE'],
        responses={204: OpenApiResponse(description='Audience deleted successfully')},
    )
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
                    campaign.progress.total_messages = audience.valid_count
                    campaign.progress.save()
                
                # Update checkpoint total_to_process
                if hasattr(campaign, 'checkpoint'):
                    campaign.checkpoint.total_to_process = audience.valid_count
                    campaign.checkpoint.save()
                
                # Update campaign totals
                campaign.total_messages = audience.valid_count
                campaign.save()
                
                logger.info(f"Audience created for campaign {campaign.id} with {audience.valid_count} valid recipients")
            
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
                campaign.progress.total_messages = audience.valid_count
                campaign.progress.save()
            
            # Update checkpoint total_to_process
            if hasattr(campaign, 'checkpoint'):
                campaign.checkpoint.total_to_process = audience.valid_count
                campaign.checkpoint.save()
            
            # Update campaign totals
            campaign.total_messages = audience.valid_count
            campaign.save()
            
            logger.info(f"Audience updated for campaign {campaign.id}")
        
        # Return with full AudienceSerializer
        output_serializer = AudienceSerializer(audience)
        return Response(output_serializer.data)

    @extend_schema(
        tags=['▶️ Campaigns - Actions'],
        summary='Start a campaign',
        description='Start a campaign. Validates that all required components (schedule, message content, audience) are present and valid.',
        request=None,
        responses={
            200: OpenApiResponse(description='Campaign started successfully'),
            400: OpenApiResponse(description='Validation error - campaign cannot be started'),
        },
        examples=[
            OpenApiExample(
                'Success Response',
                value={
                    'status': 'active',
                    'execution_status': 'PROCESSING',
                    'message': 'Campaign is now active and will run according to schedule',
                    'campaign_id': 1,
                    'total_recipients': 1000,
                    'sender_id': 'SMSINFO',
                    'channels': ['sms']
                },
                response_only=True,
            ),
        ],
    )
    @action(detail=True, methods=['post'])
    def start(self, request, pk=None):
        """
        Start a campaign.
        
        Validates that all required components are present and campaign is in draft state.
        Includes validation for sender_id, channels, and schedule type.
        """
        campaign = self.get_object()
        
        # Use action serializer for validation
        action_serializer = CampaignActionSerializer(
            data={'action': 'start'},
            context={'campaign': campaign}
        )
        action_serializer.is_valid(raise_exception=True)
        
        # Comprehensive validation
        errors = []
        
        if campaign.status != 'draft':
            errors.append(f"Campaign must be in 'draft' status to start (current: {campaign.status})")
        
        # Validate sender_id
        if not campaign.sender_id:
            errors.append("Sender ID is required")
        
        # Validate channels
        if not campaign.channels:
            errors.append("At least one channel must be selected")
        
        if not hasattr(campaign, 'schedule'):
            errors.append("Schedule is required")
        else:
            schedule = campaign.schedule
            if not schedule.is_active:
                errors.append("Schedule is not active")
            
            # Check if schedule has upcoming windows
            next_date, _ = schedule.get_next_window()
            if not next_date:
                errors.append("Schedule has no upcoming windows to execute")
        
        if not hasattr(campaign, 'message_content'):
            errors.append("Message content is required")
        else:
            # Check if content has actual text for default language
            default_lang = campaign.message_content.default_language
            if not campaign.message_content.content.get(default_lang):
                errors.append(f"Message content for default language '{default_lang}' cannot be empty")
        
        if not hasattr(campaign, 'audience'):
            errors.append("Audience is required")
        elif campaign.audience.valid_count == 0:
            errors.append("No valid recipients found in audience")
        
        if errors:
            return Response(
                {"detail": "Cannot start campaign", "errors": errors},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        with transaction.atomic():
            # Update campaign status
            campaign.status = 'active'
            campaign.execution_status = 'PROCESSING'
            campaign.execution_started_at = timezone.now()
            campaign.save()
            
            # Update schedule - find and start current window
            schedule = campaign.schedule
            next_date, next_window = schedule.get_next_window()
            if next_date:
                schedule.start_window(next_date, next_window)
            
            # Update progress status
            if hasattr(campaign, 'progress'):
                campaign.progress.status = 'ACTIVE'
                campaign.progress.save()
            
            # Update checkpoint
            if hasattr(campaign, 'checkpoint'):
                campaign.checkpoint.status = 'RUNNING'
                campaign.checkpoint.save()
        
        logger.info(f"Campaign {campaign.id} started by user {request.user.id}")
        
        return Response({
            "status": "active",
            "execution_status": "PROCESSING",
            "message": "Campaign is now active and will run according to schedule",
            "campaign_id": campaign.id,
            "total_recipients": campaign.audience.valid_count,
            "valid_recipients": campaign.audience.valid_count,
            "sender_id": campaign.sender_id,
            "channels": campaign.channels,
            "next_run": campaign.schedule.get_schedule_summary() if hasattr(campaign, 'schedule') else None
        })

    @extend_schema(
        tags=['▶️ Campaigns - Actions'],
        summary='Pause a campaign',
        description='Pause a running campaign. Can be resumed later.',
        request=None,
        responses={200: OpenApiResponse(description='Campaign paused successfully'), 400: OpenApiResponse(description='Cannot pause campaign')},
    )
    @action(detail=True, methods=['post'])
    def pause(self, request, pk=None):
        """
        Pause a running campaign.
        """
        campaign = self.get_object()
        
        # Use action serializer for validation
        action_serializer = CampaignActionSerializer(
            data={'action': 'pause'},
            context={'campaign': campaign}
        )
        action_serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            # Update campaign status
            campaign.execution_status = 'PAUSED'
            campaign.execution_paused_at = timezone.now()
            campaign.save()
            
            # Update schedule
            if hasattr(campaign, 'schedule'):
                campaign.schedule.pause()
            
            # Update progress status
            if hasattr(campaign, 'progress'):
                campaign.progress.status = 'STOPPED'
                campaign.progress.save()
            
            # Update checkpoint
            if hasattr(campaign, 'checkpoint'):
                campaign.checkpoint.status = 'PAUSED'
                campaign.checkpoint.save()
        
        logger.info(f"Campaign {campaign.id} paused by user {request.user.id}")
        
        return Response({
            "status": campaign.status,
            "execution_status": "PAUSED",
            "message": "Campaign has been paused",
            "campaign_id": campaign.id,
            "paused_at": campaign.execution_paused_at
        })

    @extend_schema(
        tags=['▶️ Campaigns - Actions'],
        summary='Resume a campaign',
        description='Resume a paused campaign.',
        request=None,
        responses={200: OpenApiResponse(description='Campaign resumed successfully'), 400: OpenApiResponse(description='Cannot resume campaign')},
    )
    @action(detail=True, methods=['post'])
    def resume(self, request, pk=None):
        """
        Resume a paused campaign.
        """
        campaign = self.get_object()
        
        # Use action serializer for validation
        action_serializer = CampaignActionSerializer(
            data={'action': 'resume'},
            context={'campaign': campaign}
        )
        action_serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            # Update campaign status
            campaign.execution_status = 'PROCESSING'
            campaign.execution_paused_at = None
            campaign.save()
            
            # Update schedule
            if hasattr(campaign, 'schedule'):
                campaign.schedule.resume()
            
            # Update progress status
            if hasattr(campaign, 'progress'):
                campaign.progress.status = 'ACTIVE'
                campaign.progress.save()
            
            # Update checkpoint
            if hasattr(campaign, 'checkpoint'):
                campaign.checkpoint.status = 'RUNNING'
                campaign.checkpoint.save()
        
        logger.info(f"Campaign {campaign.id} resumed by user {request.user.id}")
        
        return Response({
            "status": campaign.status,
            "execution_status": "PROCESSING",
            "message": "Campaign has been resumed",
            "campaign_id": campaign.id
        })

    @extend_schema(
        tags=['▶️ Campaigns - Actions'],
        summary='Stop a campaign',
        description='Stop a running campaign completely. Cannot be resumed after stop.',
        request=None,
        responses={200: OpenApiResponse(description='Campaign stopped successfully'), 400: OpenApiResponse(description='Cannot stop campaign')},
    )
    @action(detail=True, methods=['post'])
    def stop(self, request, pk=None):
        """
        Stop a running campaign completely.
        """
        campaign = self.get_object()
        
        # Use action serializer for validation
        action_serializer = CampaignActionSerializer(
            data={'action': 'stop'},
            context={'campaign': campaign}
        )
        action_serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            # Update campaign status
            campaign.status = 'paused'
            campaign.execution_status = 'STOPPED'
            campaign.save()
            
            # Update schedule
            if hasattr(campaign, 'schedule'):
                campaign.schedule.stop()
            
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
            "execution_status": "STOPPED",
            "message": "Campaign has been stopped",
            "campaign_id": campaign.id
        })

    @extend_schema(
        tags=['▶️ Campaigns - Actions'],
        summary='Complete a campaign',
        description='Mark a campaign as completed.',
        request=None,
        responses={200: OpenApiResponse(description='Campaign completed successfully'), 400: OpenApiResponse(description='Cannot complete campaign')},
    )
    @action(detail=True, methods=['post'])
    def complete(self, request, pk=None):
        """
        Mark a campaign as completed.
        """
        campaign = self.get_object()
        
        # Use action serializer for validation
        action_serializer = CampaignActionSerializer(
            data={'action': 'complete'},
            context={'campaign': campaign}
        )
        action_serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            # Update campaign status
            campaign.status = 'completed'
            campaign.execution_status = 'COMPLETED'
            campaign.execution_completed_at = timezone.now()
            campaign.save()
            
            # Update schedule - complete current window if active
            if hasattr(campaign, 'schedule'):
                if campaign.schedule.current_window_status == 'active':
                    campaign.schedule.complete_window(
                        status='completed',
                        stats={'messages_sent': campaign.total_processed}
                    )
                else:
                    campaign.schedule.campaign_status = 'completed'
                    campaign.schedule.is_active = False
                    campaign.schedule.save()
            
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
            "execution_status": "COMPLETED",
            "message": "Campaign marked as completed",
            "campaign_id": campaign.id,
            "total_processed": campaign.total_processed,
            "completed_at": campaign.execution_completed_at
        })

    @extend_schema(
        tags=['▶️ Campaigns - Actions'],
        summary='Archive a campaign',
        description='Archive a completed campaign.',
        request=None,
        responses={200: OpenApiResponse(description='Campaign archived successfully'), 400: OpenApiResponse(description='Cannot archive campaign')},
    )
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

    @extend_schema(
        tags=['📱 Campaigns'],
        summary='Soft delete a campaign',
        description='Soft delete a campaign (only if not active).',
        request=None,
        responses={200: OpenApiResponse(description='Campaign deleted successfully'), 400: OpenApiResponse(description='Cannot delete active campaign')},
    )
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

    @extend_schema(
        tags=['📈 Campaigns - Progress'],
        summary='Get campaign progress',
        description='Get detailed progress information for a campaign including sent, delivered, failed counts.',
        responses={200: OpenApiTypes.OBJECT, 404: OpenApiResponse(description='No progress data found')},
    )
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
        
        # Get recent batches
        recent_batches = batches.order_by('-created_at')[:10]
        recent_serializer = BatchStatusSerializer(recent_batches, many=True)
        
        data = {
            'campaign_id': campaign.id,
            'campaign_name': campaign.name,
            'status': campaign.status,
            'execution_status': campaign.execution_status,
            'progress': CampaignProgressSerializer(progress).data,
            'batches': batch_summary,
            'recent_batches': recent_serializer.data,
            'channels': campaign.channels,
            'sender_id': campaign.sender_id,
            'total_processed': campaign.total_processed,
            'last_processed_id': campaign.last_processed_id
        }
        
        return Response(data)

    @extend_schema(
        tags=['📦 Batches'],
        summary='Get campaign batches',
        description='Get all batches for a campaign with optional filtering.',
        parameters=[
            OpenApiParameter(name='status', type=str, location='query',
                           description='Filter by batch status',
                           enum=['PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED']),
            OpenApiParameter(name='order_by', type=str, location='query',
                           description='Order by field (e.g., -created_at, created_at)'),
        ],
    )
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
        
        # Order by
        order_by = request.query_params.get('order_by', '-created_at')
        batches = batches.order_by(order_by)
        
        page = self.paginate_queryset(batches)
        if page is not None:
            serializer = BatchStatusSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = BatchStatusSerializer(batches, many=True)
        return Response(serializer.data)

    @extend_schema(
        tags=['🔍 Checkpoints'],
        summary='Get campaign checkpoint',
        description='Get checkpoint information for a campaign (used for resumable processing).',
        responses={200: CheckpointSerializer(), 404: OpenApiResponse(description='No checkpoint found')},
    )
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

    @extend_schema(
        tags=['🔍 Checkpoints'],
        summary='Update checkpoint',
        description='Update checkpoint information (used by Layer 2 for resumable processing).',
        request=OpenApiTypes.OBJECT,
        responses={200: CheckpointSerializer(), 400: OpenApiResponse(description='Invalid data')},
    )
    @action(detail=True, methods=['post'])
    def update_checkpoint(self, request, pk=None):
        """
        Update checkpoint information (used by Layer 2).
        """
        campaign = self.get_object()
        
        if not hasattr(campaign, 'checkpoint'):
            return Response(
                {"detail": "No checkpoint found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        checkpoint = campaign.checkpoint
        last_processed = request.data.get('last_processed')
        total_processed = request.data.get('total_processed')
        status_val = request.data.get('status')
        
        if last_processed is not None:
            try:
                checkpoint.last_processed_index = int(last_processed)
                campaign.last_processed_id = int(last_processed)
            except (ValueError, TypeError):
                return Response(
                    {"detail": "last_processed must be an integer"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        if total_processed is not None:
            try:
                campaign.total_processed = int(total_processed)
            except (ValueError, TypeError):
                return Response(
                    {"detail": "total_processed must be an integer"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        if status_val is not None:
            # Validate status
            valid_statuses = ['RUNNING', 'PAUSED', 'COMPLETED', 'FAILED']
            if status_val not in valid_statuses:
                return Response(
                    {"detail": f"Invalid status. Must be one of: {valid_statuses}"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            checkpoint.status = status_val
            campaign.execution_status = status_val
        
        checkpoint.save()
        campaign.save()
        
        logger.info(f"Checkpoint updated for campaign {campaign.id}")
        
        return Response(CheckpointSerializer(checkpoint).data)

    @extend_schema(
        tags=['📱 Campaigns'],
        summary='Get campaign summary',
        description='Get summary statistics for all campaigns.',
        responses={200: OpenApiTypes.OBJECT},
    )
    @action(detail=False, methods=['get'])
    def summary(self, request):
        """
        Get summary statistics for all campaigns.
        """
        campaigns = self.get_queryset()
        
        summary = {
            'total': campaigns.count(),
            'by_status': {},
            'by_execution_status': {},
            'by_channel': {},
            'total_recipients': 0,
            'total_processed': 0,
            'total_sent': 0,
            'total_delivered': 0,
            'total_failed': 0,
        }
        
        # Count by status
        for status_choice, _ in Campaign.STATUS_CHOICES:
            count = campaigns.filter(status=status_choice).count()
            if count > 0:
                summary['by_status'][status_choice] = count
        
        # Count by execution status
        for exec_status, _ in Campaign.EXECUTION_STATUS_CHOICES:
            count = campaigns.filter(execution_status=exec_status).count()
            if count > 0:
                summary['by_execution_status'][exec_status] = count
        
        # Count by channel (campaigns can have multiple channels)
        channel_counts = {'sms': 0, 'app_notification': 0, 'flash_sms': 0}
        for campaign in campaigns:
            for channel in campaign.channels:
                if channel in channel_counts:
                    channel_counts[channel] += 1
        summary['by_channel'] = channel_counts
        
        # Aggregate progress data
        summary['total_recipients'] = campaigns.aggregate(Sum('total_messages'))['total_messages__sum'] or 0
        summary['total_processed'] = campaigns.aggregate(Sum('total_processed'))['total_processed__sum'] or 0
        summary['total_sent'] = campaigns.aggregate(Sum('sent_count'))['sent_count__sum'] or 0
        summary['total_delivered'] = campaigns.aggregate(Sum('delivered_count'))['delivered_count__sum'] or 0
        summary['total_failed'] = campaigns.aggregate(Sum('failed_count'))['failed_count__sum'] or 0
        
        return Response(summary)


# =============== SCHEDULE VIEWSET WITH SWAGGER DECORATORS ===============

@extend_schema_view(
    list=extend_schema(
        tags=['Schedules'],
        summary='List all schedules',
        description='Retrieve a list of all campaign schedules with filtering options.',
        parameters=[
            OpenApiParameter(name='schedule_type', type=str, location='query',
                           description='Filter by schedule type',
                           enum=['once', 'daily', 'weekly', 'monthly']),
            OpenApiParameter(name='campaign_id', type=int, location='query',
                           description='Filter by campaign ID'),
            OpenApiParameter(name='campaign_status', type=str, location='query',
                           description='Filter by campaign status',
                           enum=['active', 'paused', 'stopped', 'completed', 'expired']),
            OpenApiParameter(name='is_active', type=bool, location='query',
                           description='Filter by active status'),
        ],
    ),
    create=extend_schema(
        tags=['Schedules'],
        summary='Create a schedule',
        description='Create a schedule for a campaign. Supports one-time, daily, weekly, and monthly schedules.',
        examples=[
            OpenApiExample(
                'Daily Schedule',
                value={
                    'campaign': 1,
                    'schedule_type': 'daily',
                    'start_date': '2024-01-01',
                    'time_windows': [{'start': '09:00', 'end': '17:00'}],
                    'timezone': 'Africa/Addis_Ababa'
                },
            ),
            OpenApiExample(
                'Weekly Schedule',
                value={
                    'campaign': 1,
                    'schedule_type': 'weekly',
                    'start_date': '2024-01-01',
                    'run_days': [0, 2, 4],  # Monday, Wednesday, Friday
                    'time_windows': [{'start': '10:00', 'end': '12:00'}, {'start': '14:00', 'end': '16:00'}],
                },
            ),
            OpenApiExample(
                'One-time Schedule',
                value={
                    'campaign': 1,
                    'schedule_type': 'once',
                    'start_date': '2024-01-01',
                    'time_windows': [{'start': '14:00', 'end': '15:00'}],
                },
            ),
        ],
    ),
    retrieve=extend_schema(
        tags=['Schedules'],
        summary='Get schedule details',
        description='Retrieve detailed information about a specific schedule including upcoming windows and execution progress.',
    ),
    update=extend_schema(
        tags=['Schedules'],
        summary='Update a schedule',
        description='Fully update an existing schedule. Only allowed when campaign is in draft status.',
    ),
    partial_update=extend_schema(
        tags=['⏰ Schedules'],
        summary='Partially update a schedule',
        description='Partially update an existing schedule. Only allowed when campaign is in draft status.',
    ),
    destroy=extend_schema(
        tags=['Schedules'],
        summary='Delete a schedule',
        description='Delete a schedule. Only allowed when campaign is in draft status.',
    ),
)
class ScheduleViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing campaign schedules with full CRUD operations.
    
    Provides:
    - List all schedules (with filtering)
    - Retrieve specific schedule
    - Create new schedule
    - Update schedule (full or partial)
    - Delete schedule
    - Additional actions for schedule management
    """
    
    queryset = Schedule.objects.all().select_related('campaign')
    serializer_class = ScheduleSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter queryset based on query parameters"""
        queryset = super().get_queryset()
        
        # Filter by schedule_type
        schedule_type = self.request.query_params.get('schedule_type')
        if schedule_type:
            queryset = queryset.filter(schedule_type=schedule_type)
        
        # Filter by campaign
        campaign_id = self.request.query_params.get('campaign_id')
        if campaign_id:
            queryset = queryset.filter(campaign_id=campaign_id)
        
        # Filter by campaign_status
        campaign_status = self.request.query_params.get('campaign_status')
        if campaign_status:
            queryset = queryset.filter(campaign_status=campaign_status)
        
        # Filter by current_window_status
        window_status = self.request.query_params.get('window_status')
        if window_status:
            queryset = queryset.filter(current_window_status=window_status)
        
        # Filter by is_active
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
        
        # Filter by next_run
        run_today = self.request.query_params.get('run_today')
        if run_today and run_today.lower() == 'true':
            today = timezone.now().date()
            queryset = queryset.filter(next_run_date=today)
        
        return queryset
    
    def create(self, request, *args, **kwargs):
        """
        Create a new schedule for a campaign.
        
        Expected payload:
        {
            "campaign": 1,
            "schedule_type": "daily",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "run_days": [0,1,2,3,4,5,6],  # for weekly
            "time_windows": [{"start": "09:00", "end": "17:00"}],
            "timezone": "Africa/Addis_Ababa"
        }
        """
        # Check if campaign already has a schedule
        campaign_id = request.data.get('campaign')
        if campaign_id:
            existing_schedule = Schedule.objects.filter(campaign_id=campaign_id).first()
            if existing_schedule:
                return Response(
                    {"detail": f"Campaign {campaign_id} already has a schedule. Use PUT or PATCH to update it."},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        # Check if campaign is in draft mode
        if campaign_id:
            try:
                campaign = Campaign.objects.get(id=campaign_id)
                if campaign.status != 'draft':
                    return Response(
                        {"detail": f"Schedule can only be created when campaign is in 'draft' status (current: {campaign.status})"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
            except Campaign.DoesNotExist:
                return Response(
                    {"detail": f"Campaign {campaign_id} does not exist"},
                    status=status.HTTP_404_NOT_FOUND
                )
        
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            schedule = serializer.save()
            # Initialize the schedule
            schedule.initialize_schedule()
            logger.info(f"Schedule created for campaign {schedule.campaign.id}")
        
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    
    def update(self, request, *args, **kwargs):
        """
        Fully update an existing schedule.
        """
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        
        # Check if campaign is in draft mode
        if instance.campaign.status != 'draft':
            return Response(
                {"detail": f"Schedule can only be modified when campaign is in 'draft' status (current: {instance.campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            schedule = serializer.save()
            # Recalculate next run after update
            schedule._calculate_next_run()
            logger.info(f"Schedule updated for campaign {schedule.campaign.id}")
        
        return Response(serializer.data)
    
    def partial_update(self, request, *args, **kwargs):
        """
        Partially update an existing schedule.
        """
        kwargs['partial'] = True
        return self.update(request, *args, **kwargs)
    
    def destroy(self, request, *args, **kwargs):
        """
        Delete a schedule (only if campaign is in draft mode).
        """
        instance = self.get_object()
        
        # Check if campaign is in draft mode
        if instance.campaign.status != 'draft':
            return Response(
                {"detail": f"Schedule can only be deleted when campaign is in 'draft' status (current: {instance.campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        campaign_id = instance.campaign.id
        campaign_name = instance.campaign.name
        
        instance.delete()
        
        logger.info(f"Schedule deleted for campaign {campaign_id} ({campaign_name})")
        
        return Response(
            {"detail": "Schedule deleted successfully", "campaign_id": campaign_id},
            status=status.HTTP_204_NO_CONTENT
        )
    
    def retrieve(self, request, *args, **kwargs):
        """
        Get a specific schedule with additional details.
        """
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        
        # Add additional schedule information
        data = serializer.data
        data['campaign_info'] = {
            'id': instance.campaign.id,
            'name': instance.campaign.name,
            'status': instance.campaign.status,
            'execution_status': instance.campaign.execution_status,
        }
        
        # Add schedule summary
        data['schedule_summary'] = instance.get_schedule_summary()
        
        # Add next execution info
        next_date, next_window = instance.get_next_window()
        data['next_execution'] = {
            'date': next_date.isoformat() if next_date else None,
            'window_index': next_window,
            'window_time': instance.time_windows[next_window] if next_window is not None and next_window < len(instance.time_windows) else None
        }
        
        # Add progress info
        data['execution_progress'] = {
            'current_round': instance.current_round,
            'total_windows_completed': instance.total_windows_completed,
            'current_window_status': instance.current_window_status,
            'current_window_date': instance.current_window_date.isoformat() if instance.current_window_date else None,
        }
        
        return Response(data)
    
    @extend_schema(
        tags=['⏰ Schedules'],
        summary='Activate schedule',
        description='Activate a schedule (set is_active=True).',
        request=None,
        responses={200: OpenApiResponse(description='Schedule activated successfully')},
    )
    @action(detail=True, methods=['post'])
    def activate(self, request, pk=None):
        """
        Activate a schedule (set is_active=True).
        """
        schedule = self.get_object()
        
        if schedule.campaign.status != 'draft':
            return Response(
                {"detail": f"Schedule can only be activated when campaign is in 'draft' status"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        schedule.is_active = True
        schedule.save()
        
        logger.info(f"Schedule activated for campaign {schedule.campaign.id}")
        
        return Response({
            "detail": "Schedule activated successfully",
            "is_active": schedule.is_active
        })
    
    @extend_schema(
        tags=['⏰ Schedules'],
        summary='Deactivate schedule',
        description='Deactivate a schedule (set is_active=False).',
        request=None,
        responses={200: OpenApiResponse(description='Schedule deactivated successfully')},
    )
    @action(detail=True, methods=['post'])
    def deactivate(self, request, pk=None):
        """
        Deactivate a schedule (set is_active=False).
        """
        schedule = self.get_object()
        
        if schedule.campaign.status != 'draft':
            return Response(
                {"detail": f"Schedule can only be deactivated when campaign is in 'draft' status"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        schedule.is_active = False
        schedule.save()
        
        logger.info(f"Schedule deactivated for campaign {schedule.campaign.id}")
        
        return Response({
            "detail": "Schedule deactivated successfully",
            "is_active": schedule.is_active
        })
    
    @extend_schema(
        tags=['⏰ Schedules'],
        summary='Reset schedule',
        description='Reset schedule to initial state.',
        request=None,
        responses={200: OpenApiResponse(description='Schedule reset successfully')},
    )
    @action(detail=True, methods=['post'])
    def reset(self, request, pk=None):
        """
        Reset schedule to initial state.
        """
        schedule = self.get_object()
        
        if schedule.campaign.status != 'draft':
            return Response(
                {"detail": f"Schedule can only be reset when campaign is in 'draft' status"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        with transaction.atomic():
            # Reset all tracking fields
            schedule.current_round = 0
            schedule.current_window_date = None
            schedule.current_window_index = 0
            schedule.current_window_status = 'pending'
            schedule.next_run_date = None
            schedule.next_run_window = None
            schedule.completed_windows = []
            schedule.total_windows_completed = 0
            schedule.campaign_status = 'active'
            schedule.is_active = True
            
            # Reinitialize
            schedule.initialize_schedule()
            
            logger.info(f"Schedule reset for campaign {schedule.campaign.id}")
        
        serializer = self.get_serializer(schedule)
        return Response({
            "detail": "Schedule reset successfully",
            "schedule": serializer.data
        })
    
    @extend_schema(
        tags=['⏰ Schedules'],
        summary='Get upcoming windows',
        description='Get upcoming execution windows for a specific schedule.',
        parameters=[
            OpenApiParameter(name='limit', type=int, location='query', description='Number of windows to return', default=5),
        ],
    )
    @action(detail=True, methods=['get'])
    def upcoming_windows(self, request, pk=None):
        """Get upcoming windows for a specific schedule"""
        schedule = self.get_object()
        limit = int(request.query_params.get('limit', 5))
        return Response(schedule.get_upcoming_windows(limit=limit))
    
    @extend_schema(
        tags=['⏰ Schedules'],
        summary='Get schedules due now',
        description='Get schedules that are due to run at the current time.',
    )
    @action(detail=False, methods=['get'])
    def due_now(self, request):
        """Get schedules that are due to run now"""
        from datetime import datetime
        
        now = timezone.now()
        current_time = now.time()
        current_date = now.date()
        
        schedules = self.get_queryset().filter(
            next_run_date=current_date,
            current_window_status='pending',
            is_active=True,
            campaign_status='active'
        )
        
        due_schedules = []
        for schedule in schedules:
            if schedule.next_run_window is not None:
                window = schedule.time_windows[schedule.next_run_window]
                start_time = datetime.strptime(window['start'], '%H:%M').time()
                end_time = datetime.strptime(window['end'], '%H:%M').time()
                
                if start_time <= current_time <= end_time:
                    due_schedules.append(schedule)
        
        page = self.paginate_queryset(due_schedules)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(due_schedules, many=True)
        return Response(serializer.data)
    
    @extend_schema(
        tags=['⏰ Schedules'],
        summary='Get schedule types',
        description='Get available schedule type choices.',
    )
    @action(detail=False, methods=['get'])
    def schedule_types(self, request):
        """Get available schedule type choices"""
        return Response([
            {'value': choice[0], 'display': choice[1]}
            for choice in Schedule.SCHEDULE_TYPE_CHOICES
        ])
    
    @extend_schema(
        tags=['⏰ Schedules'],
        summary='Get schedules summary',
        description='Get summary statistics for all schedules.',
    )
    @action(detail=False, methods=['get'])
    def summary(self, request):
        """Get summary statistics for all schedules"""
        schedules = self.get_queryset()
        
        summary = {
            'total_schedules': schedules.count(),
            'by_type': {},
            'by_status': {},
            'by_window_status': {},
            'active_schedules': schedules.filter(is_active=True).count(),
            'inactive_schedules': schedules.filter(is_active=False).count(),
        }
        
        # Count by schedule type
        for schedule_type, _ in Schedule.SCHEDULE_TYPE_CHOICES:
            count = schedules.filter(schedule_type=schedule_type).count()
            if count > 0:
                summary['by_type'][schedule_type] = count
        
        # Count by campaign status
        for status, _ in Schedule.CAMPAIGN_STATUS_CHOICES:
            count = schedules.filter(campaign_status=status).count()
            if count > 0:
                summary['by_status'][status] = count
        
        # Count by window status
        for status, _ in Schedule.WINDOW_STATUS_CHOICES:
            count = schedules.filter(current_window_status=status).count()
            if count > 0:
                summary['by_window_status'][status] = count
        
        # Schedules running today
        today = timezone.now().date()
        summary['running_today'] = schedules.filter(next_run_date=today).count()
        
        return Response(summary)


# =============== MESSAGE STATUS VIEWSET WITH SWAGGER DECORATORS ===============

@extend_schema_view(
    list=extend_schema(
        tags=['Message Statuses'],
        summary='List message statuses',
        description='Retrieve message delivery statuses with extensive filtering options.',
        parameters=[
            OpenApiParameter(name='campaign_id', type=int, location='query', description='Filter by campaign ID'),
            OpenApiParameter(name='batch_id', type=str, location='query', description='Filter by batch ID'),
            OpenApiParameter(name='status', type=str, location='query', description='Filter by status',
                           enum=['PENDING', 'SENT', 'DELIVERED', 'FAILED', 'RETRYING']),
            OpenApiParameter(name='phone_number', type=str, location='query', description='Filter by phone number'),
            OpenApiParameter(name='channel', type=str, location='query', description='Filter by channel',
                           enum=['sms', 'app_notification', 'flash_sms']),
        ],
    ),
    retrieve=extend_schema(
        tags=['Message Statuses'],
        summary='Get message status details',
        description='Retrieve detailed information about a specific message status.',
    ),
)
class MessageStatusViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing message statuses.
    Provides read-only access with extensive filtering capabilities.
    """
    
    queryset = MessageStatus.objects.all()
    serializer_class = MessageStatusSerializer
    permission_classes = [IsAuthenticated]
    
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
        
        # Filter by channel
        channel = self.request.query_params.get('channel')
        if channel:
            queryset = queryset.filter(channel=channel)
        
        # Filter by sender_id
        sender_id = self.request.query_params.get('sender_id')
        if sender_id:
            queryset = queryset.filter(sender_id=sender_id)
        
        # Filter by phone number
        phone_number = self.request.query_params.get('phone_number')
        if phone_number:
            queryset = queryset.filter(phone_number=phone_number)
        
        # Filter by provider_message_id
        provider_id = self.request.query_params.get('provider_message_id')
        if provider_id:
            queryset = queryset.filter(provider_message_id=provider_id)
        
        # Filter by date range
        created_after = self.request.query_params.get('created_after')
        if created_after:
            queryset = queryset.filter(created_at__date__gte=created_after)
        
        created_before = self.request.query_params.get('created_before')
        if created_before:
            queryset = queryset.filter(created_at__date__lte=created_before)
        
        # Filter by sent date range
        sent_after = self.request.query_params.get('sent_after')
        if sent_after:
            queryset = queryset.filter(sent_at__date__gte=sent_after)
        
        sent_before = self.request.query_params.get('sent_before')
        if sent_before:
            queryset = queryset.filter(sent_at__date__lte=sent_before)
        
        # Order by
        order_by = self.request.query_params.get('order_by', '-created_at')
        queryset = queryset.order_by(order_by)
        
        return queryset

    @extend_schema(
        tags=['📊 Message Statuses'],
        summary='Bulk create message statuses',
        description='Bulk create message statuses. This endpoint is intended for internal use by Layer 2/3.',
        request=MessageStatusBulkCreateSerializer,
        responses={201: MessageStatusSerializer(many=True)},
    )
    @action(detail=False, methods=['post'])
    def bulk_create(self, request):
        """
        Bulk create message statuses.
        This endpoint is intended for internal use by Layer 2/3.
        """
        serializer = MessageStatusBulkCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        messages = serializer.validated_data['messages']
        
        # Bulk create
        with transaction.atomic():
            created = MessageStatus.objects.bulk_create([
                MessageStatus(**message_data) for message_data in messages
            ])
        
        logger.info(f"Bulk created {len(created)} message statuses")
        
        output_serializer = MessageStatusSerializer(created, many=True)
        return Response(output_serializer.data, status=status.HTTP_201_CREATED)
    
    @extend_schema(
        tags=['📊 Message Statuses'],
        summary='Bulk update message statuses',
        description='Bulk update message statuses. This endpoint is intended for internal use by Layer 4.',
        request=OpenApiTypes.OBJECT,
        responses={200: OpenApiTypes.OBJECT},
    )
    @action(detail=False, methods=['post'], url_path='bulk-update')
    def bulk_update(self, request):
        """
        Bulk update message statuses.
        This endpoint is intended for internal use by Layer 4.
        """
        updates = request.data.get('updates', [])
        updated_count = 0
        
        with transaction.atomic():
            for update in updates:
                message_id = update.get('message_id')
                if not message_id:
                    continue
                
                try:
                    message = MessageStatus.objects.get(message_id=message_id)
                    
                    # Update fields
                    if 'status' in update:
                        message.status = update['status']
                    if 'provider_message_id' in update:
                        message.provider_message_id = update['provider_message_id']
                    if 'provider_status' in update:
                        message.provider_status = update['provider_status']
                    if 'provider_response' in update:
                        message.provider_response = update['provider_response']
                    if 'provider_response_raw' in update:
                        message.provider_response_raw = update['provider_response_raw']
                    if 'attempts' in update:
                        message.attempts = update['attempts']
                    
                    message.save()
                    updated_count += 1
                    
                except MessageStatus.DoesNotExist:
                    logger.warning(f"Message {message_id} not found for bulk update")
        
        logger.info(f"Bulk updated {updated_count} message statuses")
        
        return Response({
            "updated": updated_count,
            "total": len(updates)
        })
    
    @extend_schema(
        tags=['📊 Message Statuses'],
        summary='Get message status summary',
        description='Get summary statistics for message statuses.',
    )
    @action(detail=False, methods=['get'])
    def summary(self, request):
        """Get summary statistics for message statuses"""
        queryset = self.get_queryset()
        
        summary = {
            'total': queryset.count(),
            'by_status': {},
            'by_channel': {},
            'by_campaign': {},
            'sent_last_hour': 0,
            'delivered_last_hour': 0,
            'failed_last_hour': 0,
        }
        
        # Count by status
        status_counts = queryset.values('status').annotate(count=Count('status'))
        for item in status_counts:
            summary['by_status'][item['status']] = item['count']
        
        # Count by channel
        channel_counts = queryset.values('channel').annotate(count=Count('channel'))
        for item in channel_counts:
            summary['by_channel'][item['channel']] = item['count']
        
        # Count by campaign (top 10)
        campaign_counts = queryset.values('campaign_id').annotate(
            count=Count('campaign_id')
        ).order_by('-count')[:10]
        summary['by_campaign'] = list(campaign_counts)
        
        # Last hour stats
        one_hour_ago = timezone.now() - timedelta(hours=1)
        summary['sent_last_hour'] = queryset.filter(sent_at__gte=one_hour_ago).count()
        summary['delivered_last_hour'] = queryset.filter(delivered_at__gte=one_hour_ago).count()
        summary['failed_last_hour'] = queryset.filter(
            status='FAILED', 
            updated_at__gte=one_hour_ago
        ).count()
        
        return Response(summary)


# =============== CAMPAIGN PROGRESS VIEWSET ===============

class CampaignProgressViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing campaign progress.
    """
    
    queryset = CampaignProgress.objects.all().select_related('campaign')
    serializer_class = CampaignProgressSerializer
    permission_classes = [IsAuthenticated]
    
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
        
        # Filter by campaign
        campaign_id = self.request.query_params.get('campaign_id')
        if campaign_id:
            queryset = queryset.filter(campaign_id=campaign_id)
        
        return queryset


# =============== BATCH STATUS VIEWSET ===============

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
        
        # Filter by date range
        created_after = self.request.query_params.get('created_after')
        if created_after:
            queryset = queryset.filter(created_at__date__gte=created_after)
        
        created_before = self.request.query_params.get('created_before')
        if created_before:
            queryset = queryset.filter(created_at__date__lte=created_before)
        
        return queryset


# =============== CHECKPOINT VIEWSET ===============

class CheckpointViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing checkpoints.
    """
    
    queryset = Checkpoint.objects.all().select_related('campaign')
    serializer_class = CheckpointSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter queryset based on query parameters"""
        queryset = super().get_queryset()
        
        # Filter by status
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        # Filter by campaign
        campaign_id = self.request.query_params.get('campaign_id')
        if campaign_id:
            queryset = queryset.filter(campaign_id=campaign_id)
        
        return queryset


# =============== USER VIEWSET ===============

class UserViewSet(viewsets.ModelViewSet):
    """
    ViewSet for viewing and editing users.
    
    Provides:
    - User registration (anyone can register)
    - Profile management (authenticated users only)
    - Password change (authenticated users only)
    - List/Update/Delete users (admin/authenticated users only)
    """
    queryset = User.objects.all()
    serializer_class = UserSerializer
    
    def get_permissions(self):
        """
        Set permissions based on action:
        - 'create' (registration): Allow anyone
        - 'me' and 'change_password': Require authentication
        - 'list', 'retrieve', 'update', 'partial_update', 'destroy': Require authentication
        """
        if self.action == 'create':
            # Allow anyone to register
            permission_classes = [AllowAny]
        elif self.action in ['me', 'change_password']:
            # Require authentication for profile actions
            permission_classes = [IsAuthenticated]
        else:
            # Require authentication for other actions (list, retrieve, update, delete)
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]
    
    @extend_schema(
        tags=['👤 Users'],
        summary='Get current user',
        description='Get details of the currently authenticated user.',
        responses={200: UserSerializer()},
        operation_id='get_current_user',
    )
    @action(detail=False, methods=['get'], url_path='me')
    def me(self, request):
        """Get current user details"""
        serializer = self.get_serializer(request.user)
        return Response(serializer.data)
    
    @extend_schema(
        tags=['👤 Users'],
        summary='Change password',
        description='Change the password for the currently authenticated user.',
        request={
            'application/json': {
                'type': 'object',
                'properties': {
                    'old_password': {'type': 'string', 'description': 'Current password'},
                    'new_password': {'type': 'string', 'description': 'New password (min 8 characters)'},
                },
                'required': ['old_password', 'new_password']
            }
        },
        responses={
            200: OpenApiResponse(description='Password updated successfully'),
            400: OpenApiResponse(description='Validation error'),
        },
        operation_id='change_password',
    )
    @action(detail=False, methods=['post'], url_path='change-password')
    def change_password(self, request):
        """
        Change current user's password.
        
        Expected payload:
        {
            "old_password": "current_password",
            "new_password": "new_password"
        }
        """
        user = request.user
        old_password = request.data.get('old_password')
        new_password = request.data.get('new_password')
        
        # Validate input
        if not old_password or not new_password:
            return Response(
                {"detail": "Both old_password and new_password are required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Check old password
        if not user.check_password(old_password):
            return Response(
                {"detail": "Old password is incorrect"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Validate new password strength
        if len(new_password) < 8:
            return Response(
                {"detail": "New password must be at least 8 characters long"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Update password
        user.set_password(new_password)
        user.save()
        
        logger.info(f"Password changed for user {user.id}")
        
        return Response({"detail": "Password updated successfully"})
    
    @extend_schema(
        tags=['👤 Users'],
        summary='Register new user',
        description='Create a new user account (public endpoint - no authentication required).',
        request=UserSerializer,
        responses={201: UserSerializer(), 400: OpenApiResponse(description='Validation error')},
        operation_id='user_registration',
    )
    def create(self, request, *args, **kwargs):
        """
        Create a new user (public registration).
        Override to ensure no authentication is required.
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)
        
        logger.info(f"New user registered: {serializer.data.get('username')}")
        
        return Response(
            serializer.data, 
            status=status.HTTP_201_CREATED, 
            headers=headers
        )
    
    @extend_schema(
        tags=['👤 Users'],
        summary='List users',
        description='Retrieve list of all users (requires authentication).',
        responses={200: UserSerializer(many=True)},
        operation_id='user_list',
    )
    def list(self, request, *args, **kwargs):
        """List all users (authenticated only)"""
        return super().list(request, *args, **kwargs)
    
    @extend_schema(
        tags=['👤 Users'],
        summary='Get user by ID',
        description='Retrieve a specific user by ID (requires authentication).',
        responses={200: UserSerializer(), 404: OpenApiResponse(description='User not found')},
        operation_id='user_retrieve',
    )
    def retrieve(self, request, *args, **kwargs):
        """Retrieve specific user (authenticated only)"""
        return super().retrieve(request, *args, **kwargs)
    
    @extend_schema(
        tags=['👤 Users'],
        summary='Update user',
        description='Fully update a user (requires authentication).',
        responses={200: UserSerializer(), 400: OpenApiResponse(description='Validation error')},
        operation_id='user_update',
    )
    def update(self, request, *args, **kwargs):
        """Update user (authenticated only)"""
        return super().update(request, *args, **kwargs)
    
    @extend_schema(
        tags=['👤 Users'],
        summary='Partially update user',
        description='Partially update a user (requires authentication).',
        responses={200: UserSerializer(), 400: OpenApiResponse(description='Validation error')},
        operation_id='user_partial_update',
    )
    def partial_update(self, request, *args, **kwargs):
        """Partially update user (authenticated only)"""
        return super().partial_update(request, *args, **kwargs)
    
    @extend_schema(
        tags=['👤 Users'],
        summary='Delete user',
        description='Delete a user (requires authentication).',
        responses={204: OpenApiResponse(description='User deleted successfully')},
        operation_id='user_delete',
    )
    def destroy(self, request, *args, **kwargs):
        """Delete user (authenticated only)"""
        return super().destroy(request, *args, **kwargs)
# =============== AUDIENCE VIEWSET WITH SWAGGER DECORATORS ===============

@extend_schema_view(
    list=extend_schema(
        tags=['Audiences'],
        summary='List all audiences',
        description='Retrieve a list of all campaign audiences with filtering options.',
        parameters=[
            OpenApiParameter(name='campaign_id', type=int, location='query', description='Filter by campaign ID'),
            OpenApiParameter(name='min_recipients', type=int, location='query', description='Minimum number of recipients'),
            OpenApiParameter(name='max_recipients', type=int, location='query', description='Maximum number of recipients'),
            OpenApiParameter(name='has_invalid', type=bool, location='query', description='Filter audiences with invalid recipients'),
        ],
    ),
    retrieve=extend_schema(
        tags=['👥 Audiences'],
        summary='Get audience details',
        description='Retrieve detailed information about a specific audience including statistics.',
    ),
    create=extend_schema(
        tags=['👥 Audiences'],
        summary='Create audience',
        description='Create an audience for a campaign with recipient list. Validates phone numbers and languages.',
        examples=[
            OpenApiExample(
                'Audience Example',
                value={
                    'campaign': 1,
                    'recipients': [
                        {'msisdn': '+251912345678', 'lang': 'en'},
                        {'msisdn': '+251911223344', 'lang': 'am'},
                        {'msisdn': '+251922334455', 'lang': 'ti'},
                    ]
                },
                request_only=True,
            ),
        ],
    ),
    update=extend_schema(
        tags=['👥 Audiences'],
        summary='Update audience',
        description='Fully update an existing audience. Only allowed when campaign is in draft status.',
    ),
    partial_update=extend_schema(
        tags=['👥 Audiences'],
        summary='Partially update audience',
        description='Partially update an existing audience. Only allowed when campaign is in draft status.',
    ),
    destroy=extend_schema(
        tags=['👥 Audiences'],
        summary='Delete audience',
        description='Delete an audience. Only allowed when campaign is in draft status.',
    ),
)
class AudienceViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing campaign audiences.
    
    Provides CRUD operations for audience management:
    - List all audiences (with filtering)
    - Retrieve specific audience
    - Create new audience
    - Update audience (full or partial)
    - Delete audience
    """
    
    queryset = Audience.objects.all().select_related('campaign')
    serializer_class = AudienceSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter queryset based on query parameters"""
        queryset = super().get_queryset()
        
        # Filter by campaign
        campaign_id = self.request.query_params.get('campaign_id')
        if campaign_id:
            queryset = queryset.filter(campaign_id=campaign_id)
        
        # Filter by total_count range
        min_recipients = self.request.query_params.get('min_recipients')
        if min_recipients:
            queryset = queryset.filter(total_count__gte=min_recipients)
        
        max_recipients = self.request.query_params.get('max_recipients')
        if max_recipients:
            queryset = queryset.filter(total_count__lte=max_recipients)
        
        # Filter by valid_count range
        min_valid = self.request.query_params.get('min_valid')
        if min_valid:
            queryset = queryset.filter(valid_count__gte=min_valid)
        
        # Filter by has_invalid (valid_count < total_count)
        has_invalid = self.request.query_params.get('has_invalid')
        if has_invalid and has_invalid.lower() == 'true':
            queryset = queryset.filter(invalid_count__gt=0)
        
        # Filter by date range
        created_after = self.request.query_params.get('created_after')
        if created_after:
            queryset = queryset.filter(created_at__date__gte=created_after)
        
        created_before = self.request.query_params.get('created_before')
        if created_before:
            queryset = queryset.filter(created_at__date__lte=created_before)
        
        # Order by
        order_by = self.request.query_params.get('order_by', '-created_at')
        queryset = queryset.order_by(order_by)
        
        return queryset
    
    def retrieve(self, request, *args, **kwargs):
        """
        Get a specific audience with additional statistics.
        """
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        
        # Add additional statistics
        data = serializer.data
        data['statistics'] = {
            'total_recipients': instance.total_count,
            'valid_recipients': instance.valid_count,
            'invalid_recipients': instance.invalid_count,
            'valid_percentage': round((instance.valid_count / instance.total_count * 100), 2) if instance.total_count > 0 else 0,
            'invalid_percentage': round((instance.invalid_count / instance.total_count * 100), 2) if instance.total_count > 0 else 0,
        }
        
        # Add campaign info if available
        if instance.campaign:
            data['campaign_info'] = {
                'id': instance.campaign.id,
                'name': instance.campaign.name,
                'status': instance.campaign.status,
                'execution_status': instance.campaign.execution_status,
            }
        
        return Response(data)
    
    def create(self, request, *args, **kwargs):
        """
        Create a new audience for a campaign.
        
        Expected payload:
        {
            "campaign": 1,
            "recipients": [
                {"msisdn": "251912345678", "lang": "en"},
                {"msisdn": "251911223344", "lang": "am"}
            ]
        }
        """
        # Check if campaign already has an audience
        campaign_id = request.data.get('campaign')
        if campaign_id:
            existing_audience = Audience.objects.filter(campaign_id=campaign_id).first()
            if existing_audience:
                return Response(
                    {"detail": f"Campaign {campaign_id} already has an audience. Use PUT or PATCH to update it."},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            audience = serializer.save()
            
            # Update campaign totals
            campaign = audience.campaign
            campaign.total_messages = audience.valid_count
            campaign.save()
            
            # Update campaign progress
            if hasattr(campaign, 'progress'):
                campaign.progress.total_messages = audience.valid_count
                campaign.progress.save()
            
            # Update checkpoint
            if hasattr(campaign, 'checkpoint'):
                campaign.checkpoint.total_to_process = audience.valid_count
                campaign.checkpoint.save()
            
            logger.info(f"Audience created for campaign {campaign.id} with {audience.valid_count} valid recipients")
        
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    
    def update(self, request, *args, **kwargs):
        """
        Fully update an existing audience.
        """
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        
        # Check if campaign is in draft mode
        if instance.campaign.status != 'draft':
            return Response(
                {"detail": f"Audience can only be modified when campaign is in 'draft' status (current: {instance.campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            audience = serializer.save()
            
            # Update campaign totals
            campaign = audience.campaign
            campaign.total_messages = audience.valid_count
            campaign.save()
            
            # Update campaign progress
            if hasattr(campaign, 'progress'):
                campaign.progress.total_messages = audience.valid_count
                campaign.progress.save()
            
            # Update checkpoint
            if hasattr(campaign, 'checkpoint'):
                campaign.checkpoint.total_to_process = audience.valid_count
                campaign.checkpoint.save()
            
            logger.info(f"Audience updated for campaign {campaign.id}")
        
        return Response(serializer.data)
    
    def partial_update(self, request, *args, **kwargs):
        """
        Partially update an existing audience.
        """
        kwargs['partial'] = True
        return self.update(request, *args, **kwargs)
    
    def destroy(self, request, *args, **kwargs):
        """
        Delete an audience (soft delete or hard delete based on campaign status).
        """
        instance = self.get_object()
        
        # Check if campaign is in draft mode
        if instance.campaign.status != 'draft':
            return Response(
                {"detail": f"Audience can only be deleted when campaign is in 'draft' status (current: {instance.campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        campaign_id = instance.campaign.id
        campaign_name = instance.campaign.name
        
        instance.delete()
        
        logger.info(f"Audience deleted for campaign {campaign_id} ({campaign_name})")
        
        return Response(
            {"detail": "Audience deleted successfully", "campaign_id": campaign_id},
            status=status.HTTP_204_NO_CONTENT
        )
    
    @extend_schema(
        tags=['👥 Audiences'],
        summary='Preview recipients',
        description='Get a preview of the first 100 recipients in the audience.',
    )
    @action(detail=True, methods=['get'])
    def recipients_preview(self, request, pk=None):
        """
        Get a preview of recipients (first 100) for an audience.
        """
        audience = self.get_object()
        recipients = audience.recipients[:100]  # Limit to first 100
        
        return Response({
            "audience_id": audience.id,
            "campaign_id": audience.campaign.id,
            "campaign_name": audience.campaign.name,
            "total_recipients": audience.total_count,
            "valid_recipients": audience.valid_count,
            "invalid_recipients": audience.invalid_count,
            "preview": recipients,
            "preview_count": len(recipients),
            "has_more": audience.total_count > 100
        })
    
    @extend_schema(
        tags=['👥 Audiences'],
        summary='Get audience statistics',
        description='Get detailed statistics for an audience including language distribution and invalid samples.',
    )
    @action(detail=True, methods=['get'])
    def statistics(self, request, pk=None):
        """
        Get detailed statistics for an audience.
        """
        audience = self.get_object()
        
        # Count by language
        language_stats = {}
        for recipient in audience.recipients:
            lang = recipient.get('lang', 'unknown')
            language_stats[lang] = language_stats.get(lang, 0) + 1
        
        # Sample of invalid recipients (first 10)
        invalid_samples = []
        for recipient in audience.recipients:
            if len(invalid_samples) >= 10:
                break
            if 'msisdn' not in recipient or 'lang' not in recipient:
                invalid_samples.append(recipient)
            elif not recipient.get('msisdn') or not recipient.get('msisdn').strip():
                invalid_samples.append(recipient)
            elif recipient.get('lang') not in SUPPORTED_LANGUAGES:
                invalid_samples.append(recipient)
        
        return Response({
            "audience_id": audience.id,
            "campaign_id": audience.campaign.id,
            "campaign_name": audience.campaign.name,
            "total_count": audience.total_count,
            "valid_count": audience.valid_count,
            "invalid_count": audience.invalid_count,
            "valid_percentage": round((audience.valid_count / audience.total_count * 100), 2) if audience.total_count > 0 else 0,
            "invalid_percentage": round((audience.invalid_count / audience.total_count * 100), 2) if audience.total_count > 0 else 0,
            "language_distribution": language_stats,
            "invalid_samples": invalid_samples,
            "database_table": audience.database_table,
            "id_field": audience.id_field,
            "created_at": audience.created_at,
            "updated_at": audience.updated_at
        })
    
    @extend_schema(
        tags=['👥 Audiences'],
        summary='Get audiences summary',
        description='Get summary statistics for all audiences.',
    )
    @action(detail=False, methods=['get'])
    def summary(self, request):
        """
        Get summary statistics for all audiences.
        """
        audiences = self.get_queryset()
        
        summary = {
            'total_audiences': audiences.count(),
            'total_recipients': 0,
            'total_valid': 0,
            'total_invalid': 0,
            'avg_valid_percentage': 0,
            'by_campaign_status': {},
        }
        
        # Aggregate totals
        total_recipients = audiences.aggregate(Sum('total_count'))['total_count__sum'] or 0
        total_valid = audiences.aggregate(Sum('valid_count'))['valid_count__sum'] or 0
        total_invalid = audiences.aggregate(Sum('invalid_count'))['invalid_count__sum'] or 0
        
        summary['total_recipients'] = total_recipients
        summary['total_valid'] = total_valid
        summary['total_invalid'] = total_invalid
        
        if total_recipients > 0:
            summary['avg_valid_percentage'] = round((total_valid / total_recipients) * 100, 2)
        
        # Count by campaign status
        for audience in audiences:
            status = audience.campaign.status
            summary['by_campaign_status'][status] = summary['by_campaign_status'].get(status, 0) + 1
        
        return Response(summary)


# =============== MESSAGE CONTENT VIEWSET WITH SWAGGER DECORATORS ===============

@extend_schema_view(
    list=extend_schema(
        tags=['Message Contents'],
        summary='List message contents',
        description='Retrieve a list of all message contents with filtering options.',
        parameters=[
            OpenApiParameter(name='campaign_id', type=int, location='query', description='Filter by campaign ID'),
            OpenApiParameter(name='default_language', type=str, location='query', description='Filter by default language',
                           enum=SUPPORTED_LANGUAGES),
            OpenApiParameter(name='has_content', type=bool, location='query', description='Filter by content presence'),
        ],
    ),
    retrieve=extend_schema(
        tags=['Message Contents'],
        summary='Get message content details',
        description='Retrieve detailed information about a specific message content including language completeness.',
    ),
    create=extend_schema(
        tags=['Message Contents'],
        summary='Create message content',
        description='Create multi-language message content for a campaign.',
        examples=[
            OpenApiExample(
                'Multi-language Content',
                value={
                    'campaign': 1,
                    'content': {
                        'en': 'Hello {name}, welcome to our service!',
                        'am': 'ሰላም {name}፣ እንኳን ደህና መጣህ!',
                        'ti': 'ሰላም {name}፣ እንቋዕ ብደሓን መጻእካ!',
                        'om': 'Akkam {name}, baga nagaan dhufte!',
                        'so': 'Salaan {name}, soo dhawow!'
                    },
                    'default_language': 'en'
                },
                request_only=True,
            ),
        ],
    ),
    update=extend_schema(
        tags=['📝 Message Contents'],
        summary='Update message content',
        description='Fully update existing message content. Only allowed when campaign is in draft status.',
    ),
    partial_update=extend_schema(
        tags=['📝 Message Contents'],
        summary='Partially update message content',
        description='Partially update existing message content. Useful for updating just one language.',
    ),
    destroy=extend_schema(
        tags=['📝 Message Contents'],
        summary='Delete message content',
        description='Delete message content. Only allowed when campaign is in draft status.',
    ),
)


@extend_schema_view(
    list=extend_schema(
        tags=['📝 Message Contents'],
        summary='List all message contents',
        description='Retrieve a list of all message contents with filtering options.',
        parameters=[
            OpenApiParameter(name='campaign_id', type=int, location='query', description='Filter by campaign ID'),
            OpenApiParameter(name='default_language', type=str, location='query', description='Filter by default language'),
        ],
    ),
    create=extend_schema(
        tags=['📝 Message Contents'],
        summary='Create message content',
        description='Create new message content for a campaign. All 5 languages (en, am, ti, om, so) are required.',
        request=MessageContentSerializer,
        responses={201: MessageContentSerializer(), 400: OpenApiResponse(description='Validation error')},
        examples=[
            OpenApiExample(
                'Create Message Content',
                value={
                    'campaign': 1,
                    'content': {
                        'en': 'Hello {name}, welcome!',
                        'am': 'ሰላም {name}, እንኳን ደህና መጣህ!',
                        'ti': 'ሰላም {name}, እንቋዕ ብደሓን መጻእካ!',
                        'om': 'Akkam {name}, baga nagaan dhufte!',
                        'so': 'Salaan {name}, soo dhawow!'
                    },
                    'default_language': 'en'
                },
                request_only=True,
            ),
        ],
    ),
    retrieve=extend_schema(
        tags=['📝 Message Contents'],
        summary='Get message content',
        description='Retrieve detailed information about a specific message content.',
    ),
    update=extend_schema(
        tags=['📝 Message Contents'],
        summary='Update message content',
        description='Fully update existing message content. Only allowed when campaign is in draft status.',
    ),
    partial_update=extend_schema(
        tags=['📝 Message Contents'],
        summary='Partially update message content',
        description='Partially update existing message content. Useful for updating just one language.',
    ),
    destroy=extend_schema(
        tags=['📝 Message Contents'],
        summary='Delete message content',
        description='Delete message content. Only allowed when campaign is in draft status.',
    ),
)
class MessageContentViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing message content for campaigns.
    
    Provides CRUD operations for message content management:
    - List all message contents (with filtering)
    - Retrieve specific message content
    - Create new message content
    - Update message content (full or partial)
    - Delete message content
    """
    
    queryset = MessageContent.objects.all().select_related('campaign')
    serializer_class = MessageContentSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Filter queryset based on query parameters"""
        queryset = super().get_queryset()
        
        # Filter by campaign
        campaign_id = self.request.query_params.get('campaign_id')
        if campaign_id:
            queryset = queryset.filter(campaign_id=campaign_id)
        
        # Filter by default_language
        default_lang = self.request.query_params.get('default_language')
        if default_lang:
            queryset = queryset.filter(default_language=default_lang)
        
        # Filter by content presence (non-empty default language)
        has_content = self.request.query_params.get('has_content')
        if has_content and has_content.lower() == 'true':
            queryset = queryset.filter(
                content__has_key=F('default_language')
            ).exclude(
                content__default_language=''
            )
        
        # Filter by date range
        created_after = self.request.query_params.get('created_after')
        if created_after:
            queryset = queryset.filter(created_at__date__gte=created_after)
        
        created_before = self.request.query_params.get('created_before')
        if created_before:
            queryset = queryset.filter(created_at__date__lte=created_before)
        
        # Order by
        order_by = self.request.query_params.get('order_by', '-created_at')
        queryset = queryset.order_by(order_by)
        
        return queryset
    
    def retrieve(self, request, *args, **kwargs):
        """
        Get a specific message content with additional details.
        """
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        
        # Add additional data
        data = serializer.data
        data['campaign_info'] = {
            'id': instance.campaign.id,
            'name': instance.campaign.name,
            'status': instance.campaign.status,
            'execution_status': instance.campaign.execution_status,
        }
        
        # Add language completeness info
        languages_present = [lang for lang, content in instance.content.items() if content]
        languages_missing = [lang for lang, content in instance.content.items() if not content]
        
        data['language_completeness'] = {
            'total_languages': len(SUPPORTED_LANGUAGES),
            'languages_with_content': len(languages_present),
            'languages_missing_content': len(languages_missing),
            'present_languages': languages_present,
            'missing_languages': languages_missing,
            'completeness_percentage': round((len(languages_present) / len(SUPPORTED_LANGUAGES)) * 100, 2)
        }
        
        # Preview content for all languages
        data['content_preview'] = {
            lang: content[:100] + ('...' if len(content) > 100 else '')
            for lang, content in instance.content.items()
        }
        
        return Response(data)
    
    def create(self, request, *args, **kwargs):
        """
        Create new message content for a campaign.
        
        Expected payload:
        {
            "campaign": 1,
            "content": {
                "en": "Hello {name}, welcome!",
                "am": "ሰላም {name}, እንኳን ደህና መጣህ!",
                "ti": "...",
                "om": "...",
                "so": "..."
            },
            "default_language": "en"
        }
        """
        # Check if campaign already has message content
        campaign_id = request.data.get('campaign')
        if campaign_id:
            existing_content = MessageContent.objects.filter(campaign_id=campaign_id).first()
            if existing_content:
                return Response(
                    {"detail": f"Campaign {campaign_id} already has message content. Use PUT or PATCH to update it."},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        # Check if campaign is in draft mode
        if campaign_id:
            try:
                campaign = Campaign.objects.get(id=campaign_id)
                if campaign.status != 'draft':
                    return Response(
                        {"detail": f"Message content can only be created when campaign is in 'draft' status (current: {campaign.status})"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
            except Campaign.DoesNotExist:
                pass
        
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            content = serializer.save()
            logger.info(f"Message content created for campaign {content.campaign.id}")
        
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    
    def update(self, request, *args, **kwargs):
        """
        Fully update existing message content.
        """
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        
        # Check if campaign is in draft mode
        if instance.campaign.status != 'draft':
            return Response(
                {"detail": f"Message content can only be modified when campaign is in 'draft' status (current: {instance.campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        
        with transaction.atomic():
            content = serializer.save()
            logger.info(f"Message content updated for campaign {content.campaign.id}")
        
        return Response(serializer.data)
    
    def partial_update(self, request, *args, **kwargs):
        """
        Partially update existing message content.
        Useful for updating just one language.
        
        Example payload:
        {
            "content": {
                "am": "የተሻሻለ መልእክት እዚህ"
            }
        }
        """
        kwargs['partial'] = True
        return self.update(request, *args, **kwargs)
    
    def destroy(self, request, *args, **kwargs):
        """
        Delete message content.
        """
        instance = self.get_object()
        
        # Check if campaign is in draft mode
        if instance.campaign.status != 'draft':
            return Response(
                {"detail": f"Message content can only be deleted when campaign is in 'draft' status (current: {instance.campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        campaign_id = instance.campaign.id
        campaign_name = instance.campaign.name
        
        instance.delete()
        
        logger.info(f"Message content deleted for campaign {campaign_id} ({campaign_name})")
        
        return Response(
            {"detail": "Message content deleted successfully", "campaign_id": campaign_id},
            status=status.HTTP_204_NO_CONTENT
        )
    
    @extend_schema(
        tags=['📝 Message Contents'],
        summary='Update specific language',
        description='Update content for a specific language only.',
        request={
            'type': 'object',
            'properties': {
                'language': {'type': 'string', 'enum': SUPPORTED_LANGUAGES, 'description': 'Language code'},
                'content': {'type': 'string', 'description': 'Message content for the language'},
            },
            'required': ['language', 'content']
        },
        responses={200: OpenApiResponse(description='Language updated successfully')},
        examples=[
            OpenApiExample(
                'Update Amharic',
                value={
                    'language': 'am',
                    'content': 'የተሻሻለ መልእክት ጽሑፍ እዚህ'
                },
                request_only=True,
            ),
        ],
    )
    @action(detail=True, methods=['post'], url_path='update-language')
    def update_language(self, request, pk=None):
        """
        Update content for a specific language only.
        
        Expected payload:
        {
            "language": "am",
            "content": "የአማርኛ መልእክት ጽሑፍ"
        }
        """
        instance = self.get_object()
        
        # Check if campaign is in draft mode
        if instance.campaign.status != 'draft':
            return Response(
                {"detail": f"Message content can only be modified when campaign is in 'draft' status (current: {instance.campaign.status})"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        language = request.data.get('language')
        content = request.data.get('content')
        
        if not language:
            return Response(
                {"detail": "language field is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if language not in SUPPORTED_LANGUAGES:
            return Response(
                {"detail": f"Invalid language. Supported languages: {SUPPORTED_LANGUAGES}"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if content is None:
            return Response(
                {"detail": "content field is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Update the specific language
        instance.content[language] = content
        instance.save()
        
        logger.info(f"Updated {language} content for campaign {instance.campaign.id}")
        
        return Response({
            "campaign_id": instance.campaign.id,
            "language": language,
            "content": content,
            "message": f"Content for '{language}' updated successfully"
        })
    
    @extend_schema(
        tags=['📝 Message Contents'],
        summary='Preview rendered message',
        description='Preview rendered message with variable substitution.',
        parameters=[
            OpenApiParameter(name='language', type=str, location='query', 
                           description='Language code (default: campaign default)',
                           enum=SUPPORTED_LANGUAGES),
            OpenApiParameter(name='variables', type=str, location='query',
                           description='JSON object of variables to substitute (e.g., {"name": "John"})'),
        ],
        responses={200: OpenApiResponse(description='Rendered message preview')},
    )
    @action(detail=True, methods=['get'])
    def render_preview(self, request, pk=None):
        """
        Preview rendered message with variable substitution.
        
        Query params:
        - language: Language code (default: campaign's default_language)
        - variables: JSON object of variables to substitute (e.g., {"name": "John"})
        """
        instance = self.get_object()
        
        language = request.query_params.get('language', instance.default_language)
        variables = request.query_params.get('variables', '{}')
        
        try:
            import json
            variables = json.loads(variables)
        except json.JSONDecodeError:
            variables = {}
        
        if language not in SUPPORTED_LANGUAGES:
            return Response(
                {"detail": f"Invalid language. Supported languages: {SUPPORTED_LANGUAGES}"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        template = instance.content.get(language, instance.content.get(instance.default_language, ""))
        
        # Perform variable substitution
        rendered = template
        for key, value in variables.items():
            rendered = rendered.replace(f"{{{key}}}", str(value))
        
        return Response({
            "campaign_id": instance.campaign.id,
            "campaign_name": instance.campaign.name,
            "language": language,
            "original_template": template,
            "rendered_message": rendered,
            "variables_used": variables,
            "character_count": len(rendered),
            "sms_segments": (len(rendered) + 159) // 160  # SMS segment calculation
        })
    
    @extend_schema(
        tags=['📝 Message Contents'],
        summary='Get supported languages',
        description='Get list of supported languages for message content.',
        responses={200: OpenApiResponse(description='List of supported languages')},
    )
    @action(detail=False, methods=['get'])
    def supported_languages(self, request):
        """
        Get list of supported languages.
        """
        return Response({
            "languages": [
                {"code": lang, "name": self._get_language_name(lang)}
                for lang in SUPPORTED_LANGUAGES
            ],
            "default": "en"
        })
    
    def _get_language_name(self, code):
        """Get human-readable language name"""
        names = {
            'en': 'English',
            'am': 'Amharic',
            'ti': 'Tigrinya',
            'om': 'Oromo',
            'so': 'Somali'
        }
        return names.get(code, code)
    
    @extend_schema(
        tags=['📝 Message Contents'],
        summary='Get message contents summary',
        description='Get summary statistics for all message contents.',
        responses={200: OpenApiResponse(description='Summary statistics')},
    )
    @action(detail=False, methods=['get'])
    def summary(self, request):
        """
        Get summary statistics for all message contents.
        """
        contents = self.get_queryset()
        
        summary = {
            'total_message_contents': contents.count(),
            'by_default_language': {},
            'total_languages_used': {},
            'content_completeness': {
                'complete': 0,
                'partial': 0,
                'minimal': 0
            }
        }
        
        for content in contents:
            # Count by default language
            lang = content.default_language
            summary['by_default_language'][lang] = summary['by_default_language'].get(lang, 0) + 1
            
            # Count languages with content
            languages_with_content = sum(1 for c in content.content.values() if c)
            summary['total_languages_used'][content.campaign.id] = languages_with_content
            
            # Categorize by completeness
            if languages_with_content == len(SUPPORTED_LANGUAGES):
                summary['content_completeness']['complete'] += 1
            elif languages_with_content >= 2:
                summary['content_completeness']['partial'] += 1
            else:
                summary['content_completeness']['minimal'] += 1
        
        return Response(summary)
# =============== EXPORTS ===============

__all__ = [
    'CampaignViewSet',
    'ScheduleViewSet',
    'MessageStatusViewSet',
    'CampaignProgressViewSet',
    'BatchStatusViewSet',
    'CheckpointViewSet',
    'UserViewSet',
    'AudienceViewSet',
    'MessageContentViewSet'
]