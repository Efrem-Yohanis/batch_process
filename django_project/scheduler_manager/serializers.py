from rest_framework import serializers
from .models import Campaign, Schedule, MessageContent, Audience, SUPPORTED_LANGUAGES


class ScheduleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Schedule
        exclude = ['campaign']


class MessageContentSerializer(serializers.ModelSerializer):
    class Meta:
        model = MessageContent
        exclude = ['campaign']

    def validate_content(self, value):
        """Require exactly the supported languages when content is provided."""
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
        return value


class AudienceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Audience
        exclude = ['campaign']

    def validate_recipients(self, value):
        """
        Validate that recipients is a list of dicts, each with exactly
        'msisdn' (phone) and 'lang' (language) keys.
        """
        if not isinstance(value, list):
            raise serializers.ValidationError("recipients must be a list")
        
        if not value:  # empty list is allowed
            return value
        
        required_keys = {'msisdn', 'lang'}
        for i, recipient in enumerate(value):
            if not isinstance(recipient, dict):
                raise serializers.ValidationError(
                    f"recipients[{i}]: must be an object/dict, got {type(recipient).__name__}"
                )
            provided_keys = set(recipient.keys())
            if provided_keys != required_keys:
                missing = required_keys - provided_keys
                extra = provided_keys - required_keys
                msg = []
                if missing:
                    msg.append(f"missing: {sorted(missing)}")
                if extra:
                    msg.append(f"unexpected: {sorted(extra)}")
                raise serializers.ValidationError(
                    f"recipients[{i}]: {'; '.join(msg)}"
                )
            
            # validate msisdn is a string (phone number)
            if not isinstance(recipient['msisdn'], str):
                raise serializers.ValidationError(
                    f"recipients[{i}].msisdn must be a string"
                )
            
            # validate lang is one of the supported languages
            lang = recipient['lang']
            if lang not in SUPPORTED_LANGUAGES:
                raise serializers.ValidationError(
                    f"recipients[{i}].lang '{lang}' must be one of {SUPPORTED_LANGUAGES}"
                )
        
        return value


class CampaignSerializer(serializers.ModelSerializer):
    # Make nested serializers read-only since they're created by signals
    schedule = ScheduleSerializer(read_only=True)
    message_content = MessageContentSerializer(read_only=True)
    audience = AudienceSerializer(read_only=True)

    class Meta:
        model = Campaign
        fields = ['id', 'name', 'status', 'created_at', 'updated_at', 
                  'schedule', 'message_content', 'audience']
        read_only_fields = ['created_at', 'updated_at']

    def create(self, validated_data):
        # Just create the Campaign - signals will create MessageContent
        # Schedule and Audience will be created via separate endpoints
        campaign = Campaign.objects.create(**validated_data)
        return campaign


class CampaignScheduleSerializer(serializers.ModelSerializer):
    """Serializer for creating/updating schedule for a specific campaign"""
    class Meta:
        model = Schedule
        fields = ['start_date', 'end_date', 'frequency', 'run_days', 
                  'send_times', 'end_times', 'is_active', 'status']


class CampaignMessageContentSerializer(serializers.ModelSerializer):
    """Serializer for creating/updating message content for a specific campaign"""
    class Meta:
        model = MessageContent
        fields = ['content', 'default_language']

    def validate_content(self, value):
        """Reuse the same validation from MessageContentSerializer"""
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
        return value


class CampaignAudienceSerializer(serializers.ModelSerializer):
    """Serializer for creating/updating audience for a specific campaign"""
    class Meta:
        model = Audience
        fields = ['recipients']

    def validate_recipients(self, value):
        """Reuse the same validation from AudienceSerializer"""
        if not isinstance(value, list):
            raise serializers.ValidationError("recipients must be a list")
        
        if not value:
            return value
        
        required_keys = {'msisdn', 'lang'}
        for i, recipient in enumerate(value):
            if not isinstance(recipient, dict):
                raise serializers.ValidationError(
                    f"recipients[{i}]: must be an object/dict, got {type(recipient).__name__}"
                )
            provided_keys = set(recipient.keys())
            if provided_keys != required_keys:
                missing = required_keys - provided_keys
                extra = provided_keys - required_keys
                msg = []
                if missing:
                    msg.append(f"missing: {sorted(missing)}")
                if extra:
                    msg.append(f"unexpected: {sorted(extra)}")
                raise serializers.ValidationError(
                    f"recipients[{i}]: {'; '.join(msg)}"
                )
            
            if not isinstance(recipient['msisdn'], str):
                raise serializers.ValidationError(
                    f"recipients[{i}].msisdn must be a string"
                )
            
            lang = recipient['lang']
            if lang not in SUPPORTED_LANGUAGES:
                raise serializers.ValidationError(
                    f"recipients[{i}].lang '{lang}' must be one of {SUPPORTED_LANGUAGES}"
                )
        
        return value