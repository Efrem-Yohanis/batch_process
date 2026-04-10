from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils import timezone
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
    list_display = (
        'id', 
        'name', 
        'status_colored', 
        'execution_status_colored',
        'sender_id',
        'channels_display',
        'created_by', 
        'total_processed',
        'progress_bar',
        'created_at'
    )
    list_filter = (
        'status', 
        'execution_status',
        'is_deleted', 
        'created_at',
        'channels'
    )
    search_fields = ('name', 'id', 'sender_id')
    readonly_fields = (
        'created_at', 
        'updated_at',
        'execution_started_at',
        'execution_completed_at',
        'execution_paused_at',
        'total_messages',
        'sent_count',
        'delivered_count',
        'failed_count',
        'pending_count'
    )
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'status', 'created_by', 'sender_id', 'channels')
        }),
        ('Execution Status', {
            'fields': (
                'execution_status',
                'execution_started_at',
                'execution_completed_at',
                'execution_paused_at',
                'last_processed_id',
                'total_processed'
            )
        }),
        ('Provider Statistics', {
            'fields': (
                'total_messages',
                'sent_count',
                'delivered_count',
                'failed_count',
                'pending_count'
            )
        }),
        ('Soft Delete', {
            'fields': ('is_deleted', 'deleted_at'),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def status_colored(self, obj):
        colors = {
            'draft': 'gray',
            'active': 'green',
            'paused': 'orange',
            'completed': 'blue',
            'archived': 'darkgray',
        }
        color = colors.get(obj.status, 'black')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.get_status_display()
        )
    status_colored.short_description = 'Status'
    
    def execution_status_colored(self, obj):
        colors = {
            'PENDING': 'gray',
            'PROCESSING': 'blue',
            'PAUSED': 'orange',
            'STOPPED': 'red',
            'COMPLETED': 'green',
            'FAILED': 'darkred',
        }
        color = colors.get(obj.execution_status, 'black')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.get_execution_status_display()
        )
    execution_status_colored.short_description = 'Execution Status'
    
    def channels_display(self, obj):
        channels = obj.get_channels_display()
        return format_html(', '.join(channels))
    channels_display.short_description = 'Channels'
    
    def progress_bar(self, obj):
        percent = obj.progress_percentage
        bar_width = 100
        filled = int(percent * bar_width / 100)
        empty = bar_width - filled
        return format_html(
            '<div style="width:100px; background:#ddd;">'
            '<div style="width:{}px; background:#5cb85c; height:20px;"></div>'
            '</div> {}%',
            filled, percent
        )
    progress_bar.short_description = 'Progress'
    
    actions = ['mark_as_completed', 'reset_execution_status']
    
    def mark_as_completed(self, request, queryset):
        queryset.update(
            status='completed',
            execution_status='COMPLETED',
            execution_completed_at=timezone.now()
        )
    mark_as_completed.short_description = "Mark selected campaigns as completed"
    
    def reset_execution_status(self, request, queryset):
        queryset.update(
            execution_status='PENDING',
            execution_started_at=None,
            execution_completed_at=None,
            execution_paused_at=None
        )
    reset_execution_status.short_description = "Reset execution status to PENDING"


@admin.register(Schedule)
class ScheduleAdmin(admin.ModelAdmin):
    list_display = (
        'campaign_link',
        'schedule_type',
        'campaign_status_colored',
        'current_round',
        'next_run_display',
        'time_windows_display',
        'total_windows_completed',
        'is_active'
    )
    list_filter = (
        'schedule_type',
        'campaign_status',
        'current_window_status',
        'is_active',
        'start_date'
    )
    search_fields = ('campaign__name',)
    readonly_fields = (
        'current_round',
        'current_window_date',
        'current_window_index',
        'current_window_status',
        'next_run_date',
        'next_run_window',
        'completed_windows',
        'total_windows_completed',
        'last_processed_at',
        'created_at',
        'updated_at'
    )
    
    fieldsets = (
        ('Campaign', {
            'fields': ('campaign',)
        }),
        ('Schedule Type', {
            'fields': ('schedule_type', 'timezone', 'auto_reset')
        }),
        ('Date Range', {
            'fields': ('start_date', 'end_date')
        }),
        ('Run Configuration', {
            'fields': ('run_days', 'time_windows'),
            'description': 'For weekly: run_days = [0,1,2,3,4,5,6] (0=Monday). For daily: use all days.'
        }),
        ('Current Status', {
            'fields': (
                'campaign_status',
                'is_active',
                'current_round',
                'current_window_date',
                'current_window_index',
                'current_window_status'
            )
        }),
        ('Next Run', {
            'fields': ('next_run_date', 'next_run_window')
        }),
        ('History', {
            'fields': ('completed_windows', 'total_windows_completed', 'last_processed_at')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def campaign_link(self, obj):
        url = reverse('admin:campaigns_campaign_change', args=[obj.campaign.id])
        return format_html('<a href="{}">{}</a>', url, obj.campaign.name)
    campaign_link.short_description = 'Campaign'
    campaign_link.admin_order_field = 'campaign__name'
    
    def campaign_status_colored(self, obj):
        colors = {
            'active': 'green',
            'paused': 'orange',
            'stopped': 'red',
            'completed': 'blue',
            'expired': 'gray',
        }
        color = colors.get(obj.campaign_status, 'black')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.get_campaign_status_display()
        )
    campaign_status_colored.short_description = 'Campaign Status'
    
    def next_run_display(self, obj):
        if obj.next_run_date and obj.next_run_window is not None:
            window = obj.time_windows[obj.next_run_window]
            return format_html(
                '{} at {}',
                obj.next_run_date,
                window['start']
            )
        return '-'
    next_run_display.short_description = 'Next Run'
    
    def time_windows_display(self, obj):
        windows = []
        for w in obj.time_windows:
            windows.append(f"{w['start']}-{w['end']}")
        return format_html(', '.join(windows))
    time_windows_display.short_description = 'Time Windows'
    
    actions = ['activate_schedules', 'pause_schedules', 'reset_schedules']
    
    def activate_schedules(self, request, queryset):
        queryset.update(campaign_status='active', is_active=True)
    activate_schedules.short_description = "Activate selected schedules"
    
    def pause_schedules(self, request, queryset):
        for schedule in queryset:
            schedule.pause()
    pause_schedules.short_description = "Pause selected schedules"
    
    def reset_schedules(self, request, queryset):
        queryset.update(
            current_round=0,
            current_window_date=None,
            current_window_index=0,
            current_window_status='pending',
            completed_windows=[],
            total_windows_completed=0
        )
    reset_schedules.short_description = "Reset schedule history"


@admin.register(MessageContent)
class MessageContentAdmin(admin.ModelAdmin):
    list_display = (
        'campaign_link',
        'default_language',
        'languages_available',
        'preview_message',
        'updated_at'
    )
    list_filter = ('default_language',)
    search_fields = ('campaign__name',)
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        ('Campaign', {
            'fields': ('campaign',)
        }),
        ('Message Configuration', {
            'fields': ('default_language', 'content')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def campaign_link(self, obj):
        url = reverse('admin:campaigns_campaign_change', args=[obj.campaign.id])
        return format_html('<a href="{}">{}</a>', url, obj.campaign.name)
    campaign_link.short_description = 'Campaign'
    
    def languages_available(self, obj):
        available = [lang for lang, msg in obj.content.items() if msg]
        return format_html(', '.join(available))
    languages_available.short_description = 'Languages Available'
    
    def preview_message(self, obj):
        if obj.content:
            # Show first non-empty message
            for lang, msg in obj.content.items():
                if msg:
                    preview = msg[:50] + '...' if len(msg) > 50 else msg
                    return format_html(
                        '<span title="{}">{} ({})</span>',
                        msg,
                        preview,
                        lang
                    )
        return '-'
    preview_message.short_description = 'Preview'


@admin.register(Audience)
class AudienceAdmin(admin.ModelAdmin):
    list_display = (
        'campaign_link',
        'total_count',
        'valid_count',
        'invalid_count',
        'valid_percentage',
        'database_table',
        'created_at'
    )
    search_fields = ('campaign__name',)
    readonly_fields = ('total_count', 'valid_count', 'invalid_count', 'created_at', 'updated_at')
    
    fieldsets = (
        ('Campaign', {
            'fields': ('campaign',)
        }),
        ('Recipient Counts', {
            'fields': ('total_count', 'valid_count', 'invalid_count')
        }),
        ('Recipients Data', {
            'fields': ('recipients',),
            'classes': ('collapse',)
        }),
        ('Database Access (Layer 2)', {
            'fields': ('database_table', 'id_field', 'filter_condition'),
            'description': 'These fields tell Layer 2 how to access recipient data directly'
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def campaign_link(self, obj):
        url = reverse('admin:campaigns_campaign_change', args=[obj.campaign.id])
        return format_html('<a href="{}">{}</a>', url, obj.campaign.name)
    campaign_link.short_description = 'Campaign'
    
    def valid_percentage(self, obj):
        if obj.total_count > 0:
            percent = (obj.valid_count / obj.total_count) * 100
            color = 'green' if percent > 90 else 'orange' if percent > 50 else 'red'
            return format_html(
                '<span style="color: {};">{:.1f}%</span>',
                color,
                percent
            )
        return '0%'
    valid_percentage.short_description = 'Valid %'


@admin.register(CampaignProgress)
class CampaignProgressAdmin(admin.ModelAdmin):
    list_display = (
        'campaign_link',
        'status_colored',
        'total_messages',
        'sent_count',
        'delivered_count',
        'failed_count',
        'pending_count',
        'progress_bar',
        'started_at'
    )
    list_filter = ('status',)
    search_fields = ('campaign__name',)
    readonly_fields = (
        'started_at',
        'completed_at',
        'updated_at'
    )
    
    fieldsets = (
        ('Campaign', {
            'fields': ('campaign',)
        }),
        ('Progress Status', {
            'fields': ('status', 'progress_percent')
        }),
        ('Message Counts', {
            'fields': (
                'total_messages',
                'sent_count',
                'delivered_count',
                'failed_count',
                'pending_count'
            )
        }),
        ('Timestamps', {
            'fields': ('started_at', 'completed_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def campaign_link(self, obj):
        url = reverse('admin:campaigns_campaign_change', args=[obj.campaign.id])
        return format_html('<a href="{}">{}</a>', url, obj.campaign.name)
    campaign_link.short_description = 'Campaign'
    
    def status_colored(self, obj):
        colors = {
            'ACTIVE': 'green',
            'COMPLETED': 'blue',
            'STOPPED': 'orange',
            'FAILED': 'red',
        }
        color = colors.get(obj.status, 'black')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.status
        )
    status_colored.short_description = 'Status'
    
    def progress_bar(self, obj):
        percent = float(obj.progress_percent)
        bar_width = 100
        filled = int(percent * bar_width / 100)
        empty = bar_width - filled
        color = '#5cb85c' if percent < 100 else '#337ab7'
        return format_html(
            '<div style="width:100px; background:#ddd;">'
            '<div style="width:{}px; background:{}; height:20px;"></div>'
            '</div> {}%',
            filled, color, percent
        )
    progress_bar.short_description = 'Progress'


@admin.register(BatchStatus)
class BatchStatusAdmin(admin.ModelAdmin):
    list_display = (
        'batch_id',
        'campaign_link',
        'status_colored',
        'total_messages',
        'success_count',
        'failed_count',
        'success_rate',
        'created_at',
        'completed_at'
    )
    list_filter = ('status', 'created_at')
    search_fields = ('batch_id', 'campaign__campaign__name')
    readonly_fields = ('created_at', 'updated_at', 'completed_at')
    
    fieldsets = (
        ('Batch Information', {
            'fields': ('batch_id', 'campaign', 'campaign_id_direct')
        }),
        ('Status', {
            'fields': ('status',)
        }),
        ('Message Counts', {
            'fields': ('total_messages', 'success_count', 'failed_count')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'completed_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def campaign_link(self, obj):
        url = reverse('admin:campaigns_campaign_change', args=[obj.campaign.campaign_id])
        return format_html('<a href="{}">{}</a>', url, obj.campaign.campaign.name)
    campaign_link.short_description = 'Campaign'
    
    def status_colored(self, obj):
        colors = {
            'PENDING': 'gray',
            'IN_PROGRESS': 'blue',
            'COMPLETED': 'green',
            'FAILED': 'red',
        }
        color = colors.get(obj.status, 'black')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.status
        )
    status_colored.short_description = 'Status'
    
    def success_rate(self, obj):
        if obj.total_messages > 0:
            rate = (obj.success_count / obj.total_messages) * 100
            color = 'green' if rate > 95 else 'orange' if rate > 80 else 'red'
            return format_html(
                '<span style="color: {};">{:.1f}%</span>',
                color,
                rate
            )
        return '0%'
    success_rate.short_description = 'Success Rate'


@admin.register(MessageStatus)
class MessageStatusAdmin(admin.ModelAdmin):
    list_display = (
        'message_id_short',
        'campaign_id',
        'batch_id_short',
        'phone_number',
        'status_colored',
        'channel',
        'sender_id',
        'attempts',
        'provider_message_id_short',
        'sent_at',
        'delivered_at'
    )
    list_filter = ('status', 'channel', 'created_at', 'sent_at', 'delivered_at')
    search_fields = ('message_id', 'phone_number', 'batch_id', 'provider_message_id')
    readonly_fields = (
        'created_at', 
        'updated_at',
        'sent_at',
        'delivered_at'
    )
    date_hierarchy = 'created_at'
    
    fieldsets = (
        ('Message Identification', {
            'fields': ('message_id', 'campaign_id', 'batch_id')
        }),
        ('Recipient', {
            'fields': ('phone_number', 'channel', 'sender_id')
        }),
        ('Status', {
            'fields': ('status', 'attempts', 'last_attempt')
        }),
        ('Provider Information', {
            'fields': (
                'provider_message_id',
                'provider_status',
                'provider_response',
                'provider_response_raw'
            ),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('sent_at', 'delivered_at', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def message_id_short(self, obj):
        return obj.message_id[:8] + '...' if len(obj.message_id) > 8 else obj.message_id
    message_id_short.short_description = 'Message ID'
    
    def batch_id_short(self, obj):
        return obj.batch_id[:8] + '...' if len(obj.batch_id) > 8 else obj.batch_id
    batch_id_short.short_description = 'Batch ID'
    
    def provider_message_id_short(self, obj):
        if obj.provider_message_id:
            return obj.provider_message_id[:8] + '...' if len(obj.provider_message_id) > 8 else obj.provider_message_id
        return '-'
    provider_message_id_short.short_description = 'Provider ID'
    
    def status_colored(self, obj):
        colors = {
            'PENDING': 'gray',
            'SENT': 'blue',
            'DELIVERED': 'green',
            'FAILED': 'red',
            'RETRYING': 'orange',
        }
        color = colors.get(obj.status, 'black')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.status
        )
    status_colored.short_description = 'Status'
    
    actions = ['mark_as_failed', 'reset_to_pending']
    
    def mark_as_failed(self, request, queryset):
        queryset.update(
            status='FAILED',
            last_attempt=timezone.now()
        )
    mark_as_failed.short_description = "Mark selected messages as FAILED"
    
    def reset_to_pending(self, request, queryset):
        queryset.update(
            status='PENDING',
            attempts=0,
            last_attempt=None,
            provider_message_id=None,
            provider_status=None,
            provider_response=None,
            provider_response_raw=None,
            sent_at=None,
            delivered_at=None
        )
    reset_to_pending.short_description = "Reset to PENDING"


@admin.register(Checkpoint)
class CheckpointAdmin(admin.ModelAdmin):
    list_display = (
        'campaign_link',
        'status_colored',
        'last_processed_index',
        'total_to_process',
        'progress_percent_display',
        'updated_at'
    )
    list_filter = ('status',)
    search_fields = ('campaign__name',)
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        ('Campaign', {
            'fields': ('campaign',)
        }),
        ('Checkpoint Status', {
            'fields': ('status',)
        }),
        ('Progress', {
            'fields': ('last_processed_index', 'total_to_process')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def campaign_link(self, obj):
        url = reverse('admin:campaigns_campaign_change', args=[obj.campaign.id])
        return format_html('<a href="{}">{}</a>', url, obj.campaign.name)
    campaign_link.short_description = 'Campaign'
    
    def status_colored(self, obj):
        colors = {
            'RUNNING': 'green',
            'COMPLETED': 'blue',
            'FAILED': 'red',
            'PAUSED': 'orange',
        }
        color = colors.get(obj.status, 'black')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.status
        )
    status_colored.short_description = 'Status'
    
    def progress_percent_display(self, obj):
        percent = obj.progress_percent()
        bar_width = 100
        filled = int(percent * bar_width / 100)
        return format_html(
            '<div style="width:100px; background:#ddd;">'
            '<div style="width:{}px; background:#5cb85c; height:20px;"></div>'
            '</div> {:.1f}%',
            filled, percent
        )
    progress_percent_display.short_description = 'Progress'