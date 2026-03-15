from django.contrib import admin
from .models import (
    Campaign,
    Schedule,
    MessageContent,
    Audience,
    CampaignProgress,
    BatchStatus,
    MessageStatus,
    Checkpoint
)


@admin.register(Campaign)
class CampaignAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'status', 'created_by', 'created_at', 'is_deleted')
    list_filter = ('status', 'is_deleted', 'created_at')
    search_fields = ('name', 'id')
    readonly_fields = ('created_at', 'updated_at')


@admin.register(Schedule)
class ScheduleAdmin(admin.ModelAdmin):
    list_display = ('campaign', 'status', 'start_date', 'end_date', 'frequency', 'is_active')
    list_filter = ('status', 'frequency', 'is_active')
    search_fields = ('campaign__name',)


@admin.register(MessageContent)
class MessageContentAdmin(admin.ModelAdmin):
    list_display = ('campaign', 'default_language', 'created_at')
    list_filter = ('default_language',)
    search_fields = ('campaign__name',)


@admin.register(Audience)
class AudienceAdmin(admin.ModelAdmin):
    list_display = ('campaign', 'total_count', 'valid_count', 'invalid_count', 'created_at')
    search_fields = ('campaign__name',)


@admin.register(CampaignProgress)
class CampaignProgressAdmin(admin.ModelAdmin):
    list_display = (
        'campaign',
        'status',
        'total_messages',
        'sent_count',
        'failed_count',
        'pending_count',
        'progress_percent',
        'started_at'
    )
    list_filter = ('status',)
    search_fields = ('campaign__name',)


@admin.register(BatchStatus)
class BatchStatusAdmin(admin.ModelAdmin):
    list_display = (
        'batch_id',
        'campaign',
        'status',
        'total_messages',
        'success_count',
        'failed_count',
        'created_at'
    )
    list_filter = ('status',)
    search_fields = ('batch_id',)


@admin.register(MessageStatus)
class MessageStatusAdmin(admin.ModelAdmin):
    list_display = (
        'message_id',
        'campaign_id',
        'batch_id',
        'phone_number',
        'status',
        'attempts',
        'created_at'
    )
    list_filter = ('status',)
    search_fields = ('message_id', 'phone_number', 'batch_id')
    readonly_fields = ('created_at', 'updated_at')


@admin.register(Checkpoint)
class CheckpointAdmin(admin.ModelAdmin):
    list_display = (
        'campaign',
        'last_processed_index',
        'total_to_process',
        'status',
        'updated_at'
    )
    list_filter = ('status',)
    search_fields = ('campaign__name',)