from rest_framework import serializers
from django.core.validators import validate_email
from django.utils import timezone
from django.contrib.auth.models import User
from rest_framework import serializers
from django.contrib.auth.hashers import make_password

from .models import (
    Campaign, Schedule, MessageContent, Audience, 
    CampaignProgress, BatchStatus, MessageStatus, Checkpoint,
    SUPPORTED_LANGUAGES
)
import re


class ScheduleSerializer(serializers.ModelSerializer):
    """Comprehensive Schedule serializer with validation"""
    
    campaign_name = serializers.CharField(source='campaign.name', read_only=True)
    
    class Meta:
        model = Schedule
        exclude = ['campaign']
        read_only_fields = ['created_at', 'updated_at', 'status']

    def validate(self, data):
        """Cross-field validation for schedule"""
        instance = getattr(self, 'instance', None)
        
        # Don't allow status changes through this serializer
        if 'status' in data and instance and data['status'] != instance.status:
            raise serializers.ValidationError({
                'status': "Use campaign start/stop/complete endpoints to change status"
            })
        
        # Validate send_times and end_times if both provided
        if 'send_times' in data and 'end_times' in data:
            send_times = data['send_times']
            end_times = data['end_times']
            
            if len(send_times) != len(end_times):
                raise serializers.ValidationError(
                    "send_times and end_times must have the same number of entries"
                )
        
        return data


class MessageContentSerializer(serializers.ModelSerializer):
    """Enhanced MessageContent serializer with language validation"""
    
    class Meta:
        model = MessageContent
        exclude = ['campaign']
        read_only_fields = ['created_at', 'updated_at']

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
        
        # Validate that content strings are not too long
        for lang, text in value.items():
            if text and len(text) > 1600:  # SMS concatenation limit
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


class AudienceSerializer(serializers.ModelSerializer):
    """Enhanced Audience serializer with phone number validation"""
    
    summary = serializers.SerializerMethodField(read_only=True)
    
    class Meta:
        model = Audience
        exclude = ['campaign']
        read_only_fields = ['created_at', 'updated_at', 'total_count', 'valid_count', 'invalid_count']

    def get_summary(self, obj):
        """Return summary statistics"""
        return {
            'total': obj.total_count,
            'valid': obj.valid_count,
            'invalid': obj.invalid_count
        }

    def validate_recipients(self, value):
        """
        Enhanced validation for recipients list.
        Validates structure and phone number format.
        """
        if not isinstance(value, list):
            raise serializers.ValidationError("recipients must be a list")
        
        if not value:  # empty list is allowed
            return value
        
        # Limit check
        if len(value) > 1000000:
            raise serializers.ValidationError(
                "Recipients list exceeds maximum limit of 1,000,000"
            )
        
        required_keys = {'msisdn', 'lang'}
        phone_pattern = re.compile(r'^\+?[1-9]\d{1,14}$')  # E.164 format
        
        valid_count = 0
        invalid_count = 0
        errors = []
        
        for i, recipient in enumerate(value):
            recipient_errors = []
            
            # Check type
            if not isinstance(recipient, dict):
                recipient_errors.append(f"must be an object/dict, got {type(recipient).__name__}")
                invalid_count += 1
                if recipient_errors:
                    errors.append(f"recipients[{i}]: {'; '.join(recipient_errors)}")
                continue
            
            # Check required keys
            provided_keys = set(recipient.keys())
            if provided_keys != required_keys:
                missing = required_keys - provided_keys
                extra = provided_keys - required_keys
                if missing:
                    recipient_errors.append(f"missing: {sorted(missing)}")
                if extra:
                    recipient_errors.append(f"unexpected: {sorted(extra)}")
            
            # Validate msisdn
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
            
            # Validate language
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
        
        # Store counts in context for later use
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
        recipients = validated_data.get('recipients', [])
        stats = self.context.get('validation_stats', {})
        
        # Add statistics to the instance
        audience = Audience.objects.create(
            **validated_data,
            total_count=stats.get('total', len(recipients)),
            valid_count=stats.get('valid', 0),
            invalid_count=stats.get('invalid', 0)
        )
        return audience

    def update(self, instance, validated_data):
        """Update audience with validation statistics"""
        if 'recipients' in validated_data:
            recipients = validated_data['recipients']
            stats = self.context.get('validation_stats', {})
            
            validated_data['total_count'] = stats.get('total', len(recipients))
            validated_data['valid_count'] = stats.get('valid', 0)
            validated_data['invalid_count'] = stats.get('invalid', 0)
        
        return super().update(instance, validated_data)


class CampaignSerializer(serializers.ModelSerializer):
    """Enhanced Campaign serializer with nested related data"""
    
    schedule = ScheduleSerializer(read_only=True)
    message_content = MessageContentSerializer(read_only=True)
    audience = AudienceSerializer(read_only=True)
    progress = serializers.SerializerMethodField(read_only=True)
    can_start = serializers.BooleanField(read_only=True)
    can_pause = serializers.BooleanField(read_only=True)
    can_complete = serializers.BooleanField(read_only=True)
    
    class Meta:
        model = Campaign
        fields = [
            'id', 'name', 'status', 'created_by', 'created_at', 'updated_at',
            'schedule', 'message_content', 'audience', 'progress',
            'can_start', 'can_pause', 'can_complete', 'is_deleted'
        ]
        read_only_fields = ['created_at', 'updated_at', 'created_by', 'is_deleted']

    def get_progress(self, obj):
        """Get campaign progress if exists"""
        if hasattr(obj, 'progress'):
            return {
                'total_messages': obj.progress.total_messages,
                'sent_count': obj.progress.sent_count,
                'failed_count': obj.progress.failed_count,
                'pending_count': obj.progress.pending_count,
                'progress_percent': float(obj.progress.progress_percent),
                'status': obj.progress.status
            }
        return None

    def create(self, validated_data):
        """Create campaign with current user as creator"""
        request = self.context.get('request')
        if request and hasattr(request, 'user'):
            validated_data['created_by'] = request.user
        
        campaign = Campaign.objects.create(**validated_data)
        return campaign


class CampaignScheduleSerializer(serializers.ModelSerializer):
    """Serializer for creating/updating schedule for a specific campaign"""
    
    class Meta:
        model = Schedule
        fields = [
            'start_date', 'end_date', 'frequency', 'run_days',
            'send_times', 'end_times', 'is_active'
        ]

    def validate(self, data):
        """Enhanced validation for schedule updates"""
        instance = getattr(self, 'instance', None)
        
        # Validate schedule can only be modified when campaign is in draft
        if instance and instance.campaign.status != 'draft':
            raise serializers.ValidationError(
                "Schedule can only be modified when campaign is in 'draft' status"
            )
        
        # Validate dates
        if 'start_date' in data and 'end_date' in data:
            if data['end_date'] and data['start_date'] > data['end_date']:
                raise serializers.ValidationError({
                    'end_date': "End date must be after start date"
                })
        
        return data


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


class CampaignAudienceSerializer(serializers.ModelSerializer):
    """Serializer for creating/updating audience for a specific campaign"""
    
    class Meta:
        model = Audience
        fields = ['recipients']

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
        context = self.context.copy()
        serializer = AudienceSerializer(context=context)
        return serializer.validate_recipients(value)


class CampaignProgressSerializer(serializers.ModelSerializer):
    """Serializer for campaign progress"""
    
    campaign_name = serializers.CharField(source='campaign.name', read_only=True)
    
    class Meta:
        model = CampaignProgress
        fields = '__all__'
        read_only_fields = ['started_at', 'updated_at']


class BatchStatusSerializer(serializers.ModelSerializer):
    """Serializer for batch status"""
    
    campaign_id = serializers.IntegerField(source='campaign.campaign_id', read_only=True)
    progress_percent = serializers.SerializerMethodField()
    
    class Meta:
        model = BatchStatus
        fields = '__all__'
        read_only_fields = ['created_at', 'updated_at']

    def get_progress_percent(self, obj):
        """Calculate batch progress percentage"""
        if obj.total_messages > 0:
            completed = obj.success_count + obj.failed_count
            return (completed / obj.total_messages) * 100
        return 0


class MessageStatusSerializer(serializers.ModelSerializer):
    """Serializer for message status"""
    
    class Meta:
        model = MessageStatus
        fields = '__all__'
        read_only_fields = ['created_at', 'updated_at']


class MessageStatusBulkCreateSerializer(serializers.Serializer):
    """Serializer for bulk creating message statuses"""
    
    messages = serializers.ListField(
        child=serializers.DictField(),
        allow_empty=False,
        max_length=1000  # Batch size limit
    )
    
    def validate_messages(self, value):
        """Validate each message in the bulk create"""
        required_fields = {'message_id', 'campaign_id', 'batch_id', 'phone_number'}
        errors = []
        
        for i, message in enumerate(value):
            message_errors = []
            
            # Check required fields
            provided = set(message.keys())
            missing = required_fields - provided
            if missing:
                message_errors.append(f"missing fields: {sorted(missing)}")
            
            # Validate message_id format
            if 'message_id' in message:
                if not isinstance(message['message_id'], str):
                    message_errors.append("message_id must be a string")
            
            # Validate campaign_id
            if 'campaign_id' in message:
                if not isinstance(message['campaign_id'], int):
                    message_errors.append("campaign_id must be an integer")
            
            # Validate phone number
            if 'phone_number' in message:
                phone = message['phone_number']
                if not isinstance(phone, str):
                    message_errors.append("phone_number must be a string")
            
            if message_errors:
                errors.append(f"message[{i}]: {'; '.join(message_errors)}")
        
        if errors:
            raise serializers.ValidationError(errors)
        
        return value


class CheckpointSerializer(serializers.ModelSerializer):
    """Serializer for campaign checkpoints"""
    
    campaign_name = serializers.CharField(source='campaign.name', read_only=True)
    progress_percent = serializers.SerializerMethodField()
    
    class Meta:
        model = Checkpoint
        fields = '__all__'
        read_only_fields = ['created_at', 'updated_at']

    def get_progress_percent(self, obj):
        """Get checkpoint progress percentage"""
        return obj.progress_percent()


class CampaignActionSerializer(serializers.Serializer):
    """Serializer for campaign actions (start/stop/complete)"""
    
    action = serializers.ChoiceField(choices=['start', 'stop', 'complete'])
    reason = serializers.CharField(required=False, allow_blank=True)
    
    def validate(self, data):
        """Validate action based on campaign state"""
        campaign = self.context.get('campaign')
        action = data.get('action')
        
        if not campaign:
            return data
        
        # Check if action is allowed
        if action == 'start' and not campaign.can_start():
            raise serializers.ValidationError(
                "Campaign cannot be started. Ensure it has schedule, message content, and audience."
            )
        elif action == 'stop' and not campaign.can_pause():
            raise serializers.ValidationError(
                "Campaign cannot be paused. It must be active."
            )
        elif action == 'complete' and not campaign.can_complete():
            raise serializers.ValidationError(
                "Campaign cannot be completed. It must be active or paused."
            )
        
        return data


class UserSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True, required=True)
    confirm_password = serializers.CharField(write_only=True, required=True)
    
    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'password', 'confirm_password', 
                  'first_name', 'last_name']
        extra_kwargs = {
            'password': {'write_only': True},
        }
    
    def validate(self, data):
        if data['password'] != data['confirm_password']:
            raise serializers.ValidationError({"password": "Passwords don't match"})
        return data
    
    def create(self, validated_data):
        # Remove confirm_password as it's not part of the User model
        validated_data.pop('confirm_password')
        
        # Hash the password properly
        user = User.objects.create_user(
            username=validated_data['username'],
            email=validated_data.get('email', ''),
            password=validated_data['password']
        )
        
        # Set optional fields
        if 'first_name' in validated_data:
            user.first_name = validated_data['first_name']
        if 'last_name' in validated_data:
            user.last_name = validated_data['last_name']
        
        user.save()
        return user
# Export all serializers
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
]