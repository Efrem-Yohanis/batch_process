from rest_framework import serializers
from django.core.validators import validate_email
from django.utils import timezone
from django.contrib.auth.models import User
from django.contrib.auth.hashers import make_password
from datetime import datetime

from .models import (
    Campaign, Schedule, MessageContent, Audience, 
    CampaignProgress, BatchStatus, MessageStatus, Checkpoint,
    SUPPORTED_LANGUAGES
)
import re
import json


# =============== HELPER FUNCTIONS ===============

def validate_phone_number(msisdn):
    """Validate phone number in E.164 format"""
    phone_pattern = re.compile(r'^\+?[1-9]\d{1,14}$')
    if not phone_pattern.match(msisdn):
        raise serializers.ValidationError(
            "Phone number must be in E.164 format (e.g., +251912345678)"
        )
    return msisdn


# =============== SCHEDULE SERIALIZER ===============

class ScheduleSerializer(serializers.ModelSerializer):
    """Comprehensive Schedule serializer with validation for enhanced schedule model"""
    
    campaign_name = serializers.CharField(source='campaign.name', read_only=True)
    schedule_type_display = serializers.CharField(source='get_schedule_type_display', read_only=True)
    campaign_status_display = serializers.CharField(source='get_campaign_status_display', read_only=True)
    current_window_status_display = serializers.CharField(source='get_current_window_status_display', read_only=True)
    upcoming_windows = serializers.SerializerMethodField(read_only=True)
    schedule_summary = serializers.SerializerMethodField(read_only=True)
    
    class Meta:
        model = Schedule
        exclude = ['campaign']
        read_only_fields = [
            'created_at', 'updated_at', 'current_round', 
            'current_window_date', 'current_window_index', 'current_window_status',
            'next_run_date', 'next_run_window', 'completed_windows',
            'total_windows_completed', 'last_processed_at'
        ]

    def get_upcoming_windows(self, obj):
        """Get upcoming windows for display"""
        return obj.get_upcoming_windows(limit=5)
    
    def get_schedule_summary(self, obj):
        """Get human-readable schedule summary"""
        return obj.get_schedule_summary()

    def validate_schedule_type(self, value):
        """Validate schedule type"""
        valid_types = ['once', 'daily', 'weekly', 'monthly']
        if value not in valid_types:
            raise serializers.ValidationError(
                f"Invalid schedule type. Choose from: {valid_types}"
            )
        return value

    def validate_run_days(self, value):
        """Validate run_days based on schedule type"""
        schedule_type = self.initial_data.get('schedule_type', getattr(self.instance, 'schedule_type', None))
        
        if not isinstance(value, list):
            raise serializers.ValidationError("run_days must be a list")
        
        if schedule_type == 'weekly':
            valid_days = [0, 1, 2, 3, 4, 5, 6]
            for day in value:
                if not isinstance(day, int) or day not in valid_days:
                    raise serializers.ValidationError(
                        f"Invalid day: {day}. Use integers 0-6 (Monday=0, Sunday=6)"
                    )
        elif schedule_type == 'daily':
            if value and value != [0, 1, 2, 3, 4, 5, 6]:
                raise serializers.ValidationError(
                    "For daily schedule, run_days should be [0,1,2,3,4,5,6] or empty"
                )
        
        return value

    def validate_time_windows(self, value):
        """Validate time windows format and ordering"""
        if not isinstance(value, list):
            raise serializers.ValidationError("time_windows must be a list")
        
        if not value:
            raise serializers.ValidationError("At least one time window is required")
        
        for i, window in enumerate(value):
            if not isinstance(window, dict):
                raise serializers.ValidationError(f"Window {i} must be a dictionary")
            
            if 'start' not in window or 'end' not in window:
                raise serializers.ValidationError(
                    f"Window {i} must have 'start' and 'end' times"
                )
            
            try:
                start_time = datetime.strptime(window['start'], '%H:%M').time()
                end_time = datetime.strptime(window['end'], '%H:%M').time()
            except ValueError:
                raise serializers.ValidationError(
                    f"Window {i}: Invalid time format. Use HH:MM (24-hour)"
                )
            
            if start_time >= end_time:
                raise serializers.ValidationError(
                    f"Window {i}: start time must be before end time"
                )
            
            for j, other in enumerate(value[:i]):
                other_start = datetime.strptime(other['start'], '%H:%M').time()
                other_end = datetime.strptime(other['end'], '%H:%M').time()
                
                if (start_time < other_end and end_time > other_start):
                    raise serializers.ValidationError(
                        f"Window {i} overlaps with window {j}"
                    )
        
        return value

    def validate(self, data):
        """Cross-field validation for schedule"""
        instance = getattr(self, 'instance', None)
        
        if 'campaign_status' in data and instance and data['campaign_status'] != instance.campaign_status:
            raise serializers.ValidationError({
                'campaign_status': "Use campaign pause/resume/stop endpoints to change status"
            })
        
        start_date = data.get('start_date', getattr(instance, 'start_date', None))
        end_date = data.get('end_date', getattr(instance, 'end_date', None))
        
        if end_date and start_date > end_date:
            raise serializers.ValidationError({
                'end_date': "End date must be after start date"
            })
        
        schedule_type = data.get('schedule_type', getattr(instance, 'schedule_type', None))
        if schedule_type == 'once' and end_date and end_date != start_date:
            raise serializers.ValidationError({
                'end_date': "For one-time schedules, end_date should equal start_date or be null"
            })
        
        if schedule_type == 'weekly' and not data.get('run_days'):
            raise serializers.ValidationError({
                'run_days': "Weekly schedule requires run_days to be specified"
            })
        
        return data


# =============== MESSAGE CONTENT SERIALIZER ===============

class MessageContentSerializer(serializers.ModelSerializer):
    """Enhanced MessageContent serializer with language validation"""
    
    languages_available = serializers.SerializerMethodField(read_only=True)
    preview = serializers.SerializerMethodField(read_only=True)
    
    class Meta:
        model = MessageContent
        exclude = ['campaign']
        read_only_fields = ['created_at', 'updated_at']

    def get_languages_available(self, obj):
        """Get list of languages that have non-empty content"""
        return [lang for lang, msg in obj.content.items() if msg]
    
    def get_preview(self, obj):
        """Get preview of first non-empty message"""
        for lang, msg in obj.content.items():
            if msg:
                return {
                    'language': lang,
                    'preview': msg[:100] + '...' if len(msg) > 100 else msg
                }
        return None

    def validate_content(self, value):
        """Require exactly the supported languages when content is provided."""
        if not isinstance(value, dict):
            raise serializers.ValidationError("Content must be a dictionary")
        
        required = set(SUPPORTED_LANGUAGES)
        provided = set(value.keys())
        
        if provided != required:
            missing = required - provided
            extra = provided - required
            msg = []
            if missing:
                msg.append(f"missing keys: {sorted(missing)}")
            if extra:
                msg.append(f"unexpected keys: {sorted(extra)}")
            raise serializers.ValidationError("; ".join(msg))
        
        for lang, text in value.items():
            if text and len(text) > 1600:
                raise serializers.ValidationError(
                    f"Content for {lang} exceeds 1600 characters (found {len(text)})"
                )
        
        return value

    def validate_default_language(self, value):
        """Validate default language is in SUPPORTED_LANGUAGES"""
        if value not in SUPPORTED_LANGUAGES:
            raise serializers.ValidationError(
                f"'{value}' is not a supported language. Choose from: {SUPPORTED_LANGUAGES}"
            )
        return value


# =============== AUDIENCE SERIALIZER (FIXED) ===============

class AudienceSerializer(serializers.ModelSerializer):
    """Enhanced Audience serializer with phone number validation"""
    
    summary = serializers.SerializerMethodField(read_only=True)
    database_info = serializers.SerializerMethodField(read_only=True)
    valid_percentage = serializers.SerializerMethodField(read_only=True)
    recipients_preview = serializers.SerializerMethodField(read_only=True)
    
    class Meta:
        model = Audience
        fields = [
            'id', 'campaign', 'recipients', 'total_count', 'valid_count', 
            'invalid_count', 'database_table', 'id_field', 'filter_condition',
            'created_at', 'updated_at', 'summary', 'database_info', 
            'valid_percentage', 'recipients_preview'
        ]
        read_only_fields = [
            'id', 'created_at', 'updated_at', 'total_count', 
            'valid_count', 'invalid_count', 'summary', 'database_info', 
            'valid_percentage', 'recipients_preview'
        ]
        extra_kwargs = {
            'campaign': {'required': True, 'allow_null': False},
            'recipients': {'required': True, 'allow_null': False},
            'database_table': {'required': False},
            'id_field': {'required': False},
            'filter_condition': {'required': False},
        }

    def get_summary(self, obj):
        """Return summary statistics"""
        return {
            'total': obj.total_count,
            'valid': obj.valid_count,
            'invalid': obj.invalid_count
        }
    
    def get_database_info(self, obj):
        """Return database information for Layer 2 to fetch recipients directly"""
        return {
            'table': obj.database_table,
            'id_field': obj.id_field,
            'filter': obj.filter_condition or f"campaign_id = {obj.campaign_id} AND status = 'PENDING'"
        }
    
    def get_valid_percentage(self, obj):
        """Calculate valid percentage"""
        if obj.total_count > 0:
            return round((obj.valid_count / obj.total_count) * 100, 2)
        return 0.0
    
    def get_recipients_preview(self, obj):
        """Get preview of first 5 recipients (without exposing all)"""
        if obj.recipients:
            return obj.recipients[:5]
        return []

    def validate_recipients(self, value):
        """
        Enhanced validation for recipients list.
        Validates structure and phone number format.
        """
        if not isinstance(value, list):
            raise serializers.ValidationError("recipients must be a list")
        
        if not value:
            return value
        
        if len(value) > 1000000:
            raise serializers.ValidationError(
                "Recipients list exceeds maximum limit of 1,000,000"
            )
        
        required_keys = {'msisdn', 'lang'}
        phone_pattern = re.compile(r'^\+?[1-9]\d{1,14}$')
        
        valid_count = 0
        invalid_count = 0
        errors = []
        
        for i, recipient in enumerate(value):
            recipient_errors = []
            
            if not isinstance(recipient, dict):
                recipient_errors.append(f"must be an object/dict, got {type(recipient).__name__}")
                invalid_count += 1
                if recipient_errors:
                    errors.append(f"recipients[{i}]: {'; '.join(recipient_errors)}")
                continue
            
            provided_keys = set(recipient.keys())
            if provided_keys != required_keys:
                missing = required_keys - provided_keys
                extra = provided_keys - required_keys
                if missing:
                    recipient_errors.append(f"missing: {sorted(missing)}")
                if extra:
                    recipient_errors.append(f"unexpected: {sorted(extra)}")
            
            if 'msisdn' in recipient:
                msisdn = recipient['msisdn']
                if not isinstance(msisdn, str):
                    recipient_errors.append("msisdn must be a string")
                elif not phone_pattern.match(msisdn):
                    recipient_errors.append(
                        "msisdn must be in E.164 format (e.g., +251912345678)"
                    )
                elif len(msisdn) > 20:
                    recipient_errors.append("msisdn exceeds maximum length of 20 characters")
            else:
                recipient_errors.append("msisdn is required")
            
            if 'lang' in recipient:
                lang = recipient['lang']
                if lang not in SUPPORTED_LANGUAGES:
                    recipient_errors.append(
                        f"lang '{lang}' must be one of {SUPPORTED_LANGUAGES}"
                    )
            else:
                recipient_errors.append("lang is required")
            
            if recipient_errors:
                invalid_count += 1
                errors.append(f"recipients[{i}]: {'; '.join(recipient_errors)}")
            else:
                valid_count += 1
        
        self.context['validation_stats'] = {
            'total': len(value),
            'valid': valid_count,
            'invalid': invalid_count
        }
        
        if errors:
            raise serializers.ValidationError(errors)
        
        return value

    def create(self, validated_data):
        """Create audience with validation statistics"""
        recipients = validated_data.pop('recipients', [])
        stats = self.context.get('validation_stats', {})
        
        audience = Audience.objects.create(
            **validated_data,
            total_count=stats.get('total', len(recipients)),
            valid_count=stats.get('valid', 0),
            invalid_count=stats.get('invalid', 0)
        )
        
        if hasattr(audience, 'recipients') and recipients:
            audience.recipients = recipients
            audience.save()
        
        return audience

    def update(self, instance, validated_data):
        """Update audience with validation statistics"""
        if 'recipients' in validated_data:
            recipients = validated_data.pop('recipients')
            stats = self.context.get('validation_stats', {})
            
            instance.total_count = stats.get('total', len(recipients))
            instance.valid_count = stats.get('valid', 0)
            instance.invalid_count = stats.get('invalid', 0)
            instance.recipients = recipients
            
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        
        instance.save()
        return instance


# =============== CHECKPOINT INFO SERIALIZER ===============

class CheckpointInfoSerializer(serializers.ModelSerializer):
    """Serializer for checkpoint information"""
    
    progress_percent = serializers.SerializerMethodField(read_only=True)
    
    class Meta:
        model = Checkpoint
        fields = ['last_processed_index', 'status', 'updated_at', 'progress_percent']
    
    def get_progress_percent(self, obj):
        """Get checkpoint progress percentage"""
        return obj.progress_percent()


# =============== CAMPAIGN PROGRESS SERIALIZER ===============

class CampaignProgressSerializer(serializers.ModelSerializer):
    """Serializer for campaign progress"""
    
    campaign_name = serializers.CharField(source='campaign.name', read_only=True)
    progress_bar = serializers.SerializerMethodField(read_only=True)
    
    class Meta:
        model = CampaignProgress
        fields = '__all__'
        read_only_fields = ['started_at', 'updated_at']
    
    def get_progress_bar(self, obj):
        """Generate progress bar data"""
        percent = float(obj.progress_percent)
        return {
            'percent': percent,
            'filled': int(percent),
            'remaining': 100 - int(percent)
        }


# =============== CHANNEL CHOICE SERIALIZER ===============

class ChannelChoiceSerializer(serializers.Serializer):
    """Serializer for channel choices"""
    value = serializers.CharField()
    display = serializers.CharField()

    @classmethod
    def get_choices(cls):
        """Return list of channel choices"""
        return [{'value': ch[0], 'display': ch[1]} for ch in Campaign.CHANNEL_CHOICES]


# =============== CAMPAIGN SERIALIZER ===============

class CampaignSerializer(serializers.ModelSerializer):
    """Enhanced Campaign serializer with nested related data"""
    
    schedule = ScheduleSerializer(read_only=True)
    message_content = MessageContentSerializer(read_only=True)
    audience = AudienceSerializer(read_only=True)
    progress = serializers.SerializerMethodField(read_only=True)
    checkpoint_info = serializers.SerializerMethodField(read_only=True)
    provider_stats = serializers.SerializerMethodField(read_only=True)
    can_start = serializers.BooleanField(read_only=True)
    can_pause = serializers.BooleanField(read_only=True)
    can_resume = serializers.BooleanField(read_only=True)
    can_stop = serializers.BooleanField(read_only=True)
    can_complete = serializers.BooleanField(read_only=True)
    execution_status_display = serializers.CharField(source='get_execution_status_display', read_only=True)
    
    class Meta:
        model = Campaign
        fields = [
            'id', 'name', 'status', 'execution_status', 'execution_status_display',
            'sender_id', 'channels', 'created_by', 'created_at', 'updated_at',
            'schedule', 'message_content', 'audience', 'progress', 'checkpoint_info',
            'provider_stats', 'last_processed_id', 'total_processed',
            'can_start', 'can_pause', 'can_resume', 'can_stop', 'can_complete',
            'is_deleted', 'execution_started_at', 'execution_completed_at', 'execution_paused_at'
        ]
        read_only_fields = [
            'created_at', 'updated_at', 'created_by', 'is_deleted',
            'execution_started_at', 'execution_completed_at', 'execution_paused_at',
            'last_processed_id', 'total_processed', 'execution_status',
            'total_messages', 'sent_count', 'delivered_count', 'failed_count', 'pending_count'
        ]
        extra_kwargs = {
            'sender_id': {
                'required': False,
                'allow_blank': True,
                'help_text': 'Sender ID for this campaign (3-11 characters, alphanumeric and underscores only)'
            },
            'channels': {
                'required': True,
                'help_text': 'List of channels for this campaign. Options: sms, app_notification, flash_sms'
            }
        }

    def validate_sender_id(self, value):
        """Validate sender ID format"""
        if value:
            if len(value) < 3 or len(value) > 11:
                raise serializers.ValidationError(
                    "Sender ID must be between 3 and 11 characters"
                )
            if not re.match(r'^[A-Za-z0-9_]+$', value):
                raise serializers.ValidationError(
                    "Sender ID can only contain letters, numbers, and underscores"
                )
        return value

    def validate_channels(self, value):
        """Validate channels"""
        if not value:
            raise serializers.ValidationError("At least one channel must be selected")
        
        valid_channels = ['sms', 'app_notification', 'flash_sms']
        for channel in value:
            if channel not in valid_channels:
                raise serializers.ValidationError(
                    f"Invalid channel: {channel}. Valid options are: {valid_channels}"
                )
        return value

    def get_progress(self, obj):
        """Get campaign progress if exists"""
        if hasattr(obj, 'progress'):
            return {
                'total_messages': obj.progress.total_messages,
                'sent_count': obj.progress.sent_count,
                'delivered_count': obj.progress.delivered_count,
                'failed_count': obj.progress.failed_count,
                'pending_count': obj.progress.pending_count,
                'progress_percent': float(obj.progress.progress_percent),
                'status': obj.progress.status
            }
        return {
            'total_messages': obj.total_messages,
            'sent_count': obj.sent_count,
            'delivered_count': obj.delivered_count,
            'failed_count': obj.failed_count,
            'pending_count': obj.pending_count,
            'progress_percent': obj.progress_percentage,
            'status': 'PENDING'
        }
    
    def get_checkpoint_info(self, obj):
        """Get checkpoint information for resume capability"""
        return obj.checkpoint_info
    
    def get_provider_stats(self, obj):
        """Get provider statistics"""
        return obj.provider_stats

    def create(self, validated_data):
        """Create campaign with current user as creator"""
        request = self.context.get('request')
        if request and hasattr(request, 'user'):
            validated_data['created_by'] = request.user
        
        campaign = Campaign.objects.create(**validated_data)
        return campaign


# =============== CAMPAIGN SCHEDULE SERIALIZER ===============

class CampaignScheduleSerializer(serializers.ModelSerializer):
    """Serializer for creating/updating schedule for a specific campaign"""
    
    class Meta:
        model = Schedule
        fields = [
            'schedule_type', 'start_date', 'end_date', 'run_days',
            'time_windows', 'timezone', 'auto_reset'
        ]

    def validate(self, data):
        """Enhanced validation for schedule updates"""
        instance = getattr(self, 'instance', None)
        
        if instance and instance.campaign.status != 'draft':
            raise serializers.ValidationError(
                "Schedule can only be modified when campaign is in 'draft' status"
            )
        
        return data


# =============== CAMPAIGN MESSAGE CONTENT SERIALIZER ===============

class CampaignMessageContentSerializer(serializers.ModelSerializer):
    """Serializer for creating/updating message content for a specific campaign"""
    
    class Meta:
        model = MessageContent
        fields = ['content', 'default_language']

    def validate(self, data):
        """Validate message content can only be modified when campaign is in draft"""
        instance = getattr(self, 'instance', None)
        
        if instance and instance.campaign.status != 'draft':
            raise serializers.ValidationError(
                "Message content can only be modified when campaign is in 'draft' status"
            )
        
        return data

    def validate_content(self, value):
        """Reuse validation from MessageContentSerializer"""
        serializer = MessageContentSerializer()
        return serializer.validate_content(value)


# =============== CAMPAIGN AUDIENCE SERIALIZER ===============

class CampaignAudienceSerializer(serializers.ModelSerializer):
    """Serializer for creating/updating audience for a specific campaign"""
    
    class Meta:
        model = Audience
        fields = ['recipients', 'database_table', 'id_field', 'filter_condition']
        extra_kwargs = {
            'recipients': {'required': True},
        }

    def validate(self, data):
        """Validate audience can only be modified when campaign is in draft"""
        instance = getattr(self, 'instance', None)
        
        if instance and instance.campaign.status != 'draft':
            raise serializers.ValidationError(
                "Audience can only be modified when campaign is in 'draft' status"
            )
        
        return data

    def validate_recipients(self, value):
        """Reuse validation from AudienceSerializer"""
        context = {'request': self.context.get('request')}
        serializer = AudienceSerializer(context=context)
        return serializer.validate_recipients(value)


# =============== BATCH STATUS SERIALIZER ===============

class BatchStatusSerializer(serializers.ModelSerializer):
    """Serializer for batch status"""
    
    campaign_id = serializers.IntegerField(source='campaign.campaign_id', read_only=True)
    campaign_name = serializers.CharField(source='campaign.campaign.name', read_only=True)
    progress_percent = serializers.SerializerMethodField()
    success_rate = serializers.SerializerMethodField()
    
    class Meta:
        model = BatchStatus
        fields = '__all__'
        read_only_fields = ['created_at', 'updated_at', 'completed_at']

    def get_progress_percent(self, obj):
        """Calculate batch progress percentage"""
        if obj.total_messages > 0:
            completed = obj.success_count + obj.failed_count
            return (completed / obj.total_messages) * 100
        return 0
    
    def get_success_rate(self, obj):
        """Calculate success rate"""
        if obj.total_messages > 0:
            return (obj.success_count / obj.total_messages) * 100
        return 0


# =============== MESSAGE STATUS SERIALIZER ===============

class MessageStatusSerializer(serializers.ModelSerializer):
    """Serializer for message status with all provider tracking fields"""
    
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    short_message_id = serializers.SerializerMethodField(read_only=True)
    short_provider_id = serializers.SerializerMethodField(read_only=True)
    
    class Meta:
        model = MessageStatus
        fields = '__all__'
        read_only_fields = ['created_at', 'updated_at', 'sent_at', 'delivered_at']
        extra_kwargs = {
            'sender_id': {
                'required': False,
                'allow_null': True,
                'help_text': 'Sender ID used for this message'
            },
            'channel': {
                'required': False,
                'default': 'sms',
                'help_text': 'Channel used for this message'
            },
            'provider_response_raw': {
                'required': False,
                'allow_null': True,
                'help_text': 'Complete provider response for debugging'
            }
        }
    
    def get_short_message_id(self, obj):
        """Get shortened message ID for display"""
        return obj.message_id[:8] + '...' if len(obj.message_id) > 8 else obj.message_id
    
    def get_short_provider_id(self, obj):
        """Get shortened provider ID for display"""
        if obj.provider_message_id:
            return obj.provider_message_id[:8] + '...' if len(obj.provider_message_id) > 8 else obj.provider_message_id
        return None
    
    def validate_status(self, value):
        """Validate status transition"""
        instance = getattr(self, 'instance', None)
        if instance:
            allowed_transitions = {
                'PENDING': ['SENT', 'FAILED', 'RETRYING'],
                'SENT': ['DELIVERED', 'FAILED'],
                'RETRYING': ['SENT', 'FAILED'],
                'FAILED': [],
                'DELIVERED': [],
            }
            
            old_status = instance.status
            if value != old_status and value not in allowed_transitions.get(old_status, []):
                raise serializers.ValidationError(
                    f"Invalid status transition from {old_status} to {value}"
                )
        return value


# =============== MESSAGE STATUS BULK CREATE SERIALIZER ===============

class MessageStatusBulkCreateSerializer(serializers.Serializer):
    """Serializer for bulk creating message statuses (used by Layer 2)"""
    
    messages = serializers.ListField(
        child=serializers.DictField(),
        allow_empty=False,
        max_length=1000
    )
    
    def validate_messages(self, value):
        """Validate each message in the bulk create"""
        required_fields = {'message_id', 'campaign_id', 'batch_id', 'phone_number'}
        errors = []
        valid_channels = ['sms', 'app_notification', 'flash_sms']
        
        for i, message in enumerate(value):
            message_errors = []
            
            provided = set(message.keys())
            missing = required_fields - provided
            if missing:
                message_errors.append(f"missing required fields: {sorted(missing)}")
            
            if 'message_id' in message and not isinstance(message['message_id'], str):
                message_errors.append("message_id must be a string")
            
            if 'campaign_id' in message and not isinstance(message['campaign_id'], int):
                message_errors.append("campaign_id must be an integer")
            
            if 'phone_number' in message and not isinstance(message['phone_number'], str):
                message_errors.append("phone_number must be a string")
            
            if 'sender_id' in message:
                sender_id = message['sender_id']
                if not isinstance(sender_id, str):
                    message_errors.append("sender_id must be a string")
                elif sender_id and (len(sender_id) < 3 or len(sender_id) > 11):
                    message_errors.append("sender_id must be between 3 and 11 characters")
            
            if 'channel' in message and message['channel'] not in valid_channels:
                message_errors.append(f"Invalid channel: {message['channel']}. Valid options: {valid_channels}")
            
            if message_errors:
                errors.append(f"message[{i}]: {'; '.join(message_errors)}")
        
        if errors:
            raise serializers.ValidationError(errors)
        
        return value


# =============== CHECKPOINT SERIALIZER ===============

class CheckpointSerializer(serializers.ModelSerializer):
    """Serializer for campaign checkpoints"""
    
    campaign_name = serializers.CharField(source='campaign.name', read_only=True)
    progress_percent = serializers.SerializerMethodField()
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    
    class Meta:
        model = Checkpoint
        fields = '__all__'
        read_only_fields = ['created_at', 'updated_at']

    def get_progress_percent(self, obj):
        """Get checkpoint progress percentage"""
        return obj.progress_percent()


# =============== CAMPAIGN ACTION SERIALIZER ===============

class CampaignActionSerializer(serializers.Serializer):
    """Serializer for campaign actions (start/stop/pause/resume/complete)"""
    
    action = serializers.ChoiceField(choices=['start', 'stop', 'pause', 'resume', 'complete'])
    reason = serializers.CharField(required=False, allow_blank=True)
    
    def validate(self, data):
        """Validate action based on campaign state"""
        campaign = self.context.get('campaign')
        action = data.get('action')
        
        if not campaign:
            return data
        
        if action == 'start' and not campaign.can_start():
            errors = []
            if campaign.status != 'draft':
                errors.append(f"Campaign must be in 'draft' status (current: {campaign.status})")
            if not campaign.sender_id:
                errors.append("Sender ID is required")
            if not campaign.channels:
                errors.append("At least one channel must be selected")
            if not hasattr(campaign, 'schedule'):
                errors.append("Schedule is required")
            if not hasattr(campaign, 'message_content'):
                errors.append("Message content is required")
            if not hasattr(campaign, 'audience'):
                errors.append("Audience is required")
            if hasattr(campaign, 'audience') and campaign.audience.valid_count == 0:
                errors.append("No valid recipients in audience")
            
            raise serializers.ValidationError(
                f"Campaign cannot be started. {'; '.join(errors)}"
            )
        elif action == 'pause' and not campaign.can_pause():
            raise serializers.ValidationError(
                "Campaign cannot be paused. It must be in PROCESSING state."
            )
        elif action == 'resume' and not campaign.can_resume():
            raise serializers.ValidationError(
                "Campaign cannot be resumed. It must be in PAUSED state."
            )
        elif action == 'stop' and not campaign.can_stop():
            raise serializers.ValidationError(
                "Campaign cannot be stopped. It must be in PROCESSING or PAUSED state."
            )
        elif action == 'complete' and not campaign.can_complete():
            raise serializers.ValidationError(
                "Campaign cannot be completed. It must be in PROCESSING or PAUSED state."
            )
        
        return data


# =============== USER SERIALIZER ===============

class UserSerializer(serializers.ModelSerializer):
    """User serializer with password confirmation"""
    
    password = serializers.CharField(write_only=True, required=True, style={'input_type': 'password'})
    confirm_password = serializers.CharField(write_only=True, required=True, style={'input_type': 'password'})
    
    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'password', 'confirm_password', 
                  'first_name', 'last_name', 'is_active', 'date_joined']
        read_only_fields = ['id', 'date_joined', 'is_active']
    
    def validate(self, data):
        if data['password'] != data['confirm_password']:
            raise serializers.ValidationError({"password": "Passwords don't match"})
        
        if len(data['password']) < 8:
            raise serializers.ValidationError({"password": "Password must be at least 8 characters"})
        
        return data
    
    def create(self, validated_data):
        validated_data.pop('confirm_password')
        
        user = User.objects.create_user(
            username=validated_data['username'],
            email=validated_data.get('email', ''),
            password=validated_data['password']
        )
        
        if 'first_name' in validated_data:
            user.first_name = validated_data['first_name']
        if 'last_name' in validated_data:
            user.last_name = validated_data['last_name']
        
        user.save()
        return user
    
    def update(self, instance, validated_data):
        if 'password' in validated_data:
            password = validated_data.pop('password')
            validated_data.pop('confirm_password', None)
            instance.set_password(password)
        
        return super().update(instance, validated_data)


# =============== EXPORTS ===============

__all__ = [
    'ScheduleSerializer',
    'MessageContentSerializer',
    'AudienceSerializer',
    'UserSerializer',
    'CampaignSerializer',
    'CampaignScheduleSerializer',
    'CampaignMessageContentSerializer',
    'CampaignAudienceSerializer',
    'CampaignProgressSerializer',
    'BatchStatusSerializer',
    'MessageStatusSerializer',
    'MessageStatusBulkCreateSerializer',
    'CheckpointSerializer',
    'CampaignActionSerializer',
    'CheckpointInfoSerializer',
    'ChannelChoiceSerializer',
]