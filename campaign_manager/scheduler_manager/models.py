"""
Django Models for SMS Campaign Management System
Integrated with Microservices Architecture (Layers 1-4)
Includes enhanced schedule system for one-time and recurring campaigns
"""

from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils import timezone
import logging
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import uuid

logger = logging.getLogger(__name__)

# =============== CONSTANTS ===============

SUPPORTED_LANGUAGES = ['en', 'am', 'ti', 'om', 'so']

def _default_message_content():
    """Return a dict with all supported languages set to an empty string."""
    return {lang: "" for lang in SUPPORTED_LANGUAGES}


# =============== CAMPAIGN CORE MODEL ===============

class Campaign(models.Model):
    """Core campaign model for SMS marketing campaigns"""
    
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('completed', 'Completed'),
        ('archived', 'Archived'),
    ]
    
    EXECUTION_STATUS_CHOICES = [
        ('PENDING', 'Pending'),          # Not yet started by Layer 2
        ('PROCESSING', 'Processing'),     # Currently being processed
        ('PAUSED', 'Paused'),             # Paused by user
        ('STOPPED', 'Stopped'),           # Stopped by user
        ('COMPLETED', 'Completed'),        # All messages processed
        ('FAILED', 'Failed'),              # Processing failed
    ]
    
    # Channel choices - can select multiple
    CHANNEL_CHOICES = [
        ('sms', 'SMS'),
        ('app_notification', 'App Notification'),
        ('flash_sms', 'Flash SMS'),
    ]
    
    # Basic campaign info
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft')
    created_by = models.ForeignKey('auth.User', on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Soft delete fields
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    
    # Sender ID for this campaign
    sender_id = models.CharField(
        max_length=11,
        blank=True,
        null=True,
        help_text="Sender ID that will appear as the message sender (e.g., 'SMSINFO', 'MPESA')"
    )
    
    # Multiple channels support
    channels = models.JSONField(
        default=list,
        help_text="List of channels for this campaign. Options: sms, app_notification, flash_sms"
    )
    
    # =============== LAYER 2 INTEGRATION FIELDS ===============
    # These fields are updated by Layer 2 (Campaign Execution Engine)
    # to enable resumable processing
    
    last_processed_id = models.PositiveIntegerField(
        default=0,
        help_text="Last recipient ID processed by Layer 2 (for keyset pagination)"
    )
    total_processed = models.PositiveIntegerField(
        default=0,
        help_text="Total number of messages successfully processed so far"
    )
    execution_status = models.CharField(
        max_length=20,
        choices=EXECUTION_STATUS_CHOICES,
        default='PENDING',
        db_index=True,
        help_text="Current execution status for Layer 2 processing"
    )
    execution_started_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When Layer 2 started processing this campaign"
    )
    execution_completed_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When Layer 2 completed processing this campaign"
    )
    execution_paused_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When Layer 2 paused processing this campaign"
    )
    
    # =============== LAYER 3/4 INTEGRATION FIELDS ===============
    # Summary statistics updated by Layer 4 via Kafka events
    
    total_messages = models.PositiveIntegerField(
        default=0,
        help_text="Total messages in this campaign (from audience)"
    )
    sent_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of messages sent to provider"
    )
    delivered_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of messages confirmed delivered"
    )
    failed_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of messages permanently failed"
    )
    pending_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of messages still pending"
    )

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['created_by']),
            models.Index(fields=['sender_id']),
            models.Index(fields=['execution_status']),  # For Layer 2 queries
            models.Index(fields=['last_processed_id']),  # For checkpoint queries
            models.Index(fields=['-created_at']),  # For recent campaigns
        ]

    def __str__(self):
        channels_display = ', '.join(self.get_channels_display())
        return f"{self.name} (ID: {self.id}) - {self.status} - Exec: {self.execution_status}"

    def clean(self):
        """Validate sender ID and channels"""
        super().clean()
        
        # Validate sender ID
        if self.sender_id:
            if len(self.sender_id) < 3 or len(self.sender_id) > 11:
                raise ValidationError({
                    'sender_id': 'Sender ID must be between 3 and 11 characters'
                })
            if not self.sender_id.replace('_', '').isalnum():
                raise ValidationError({
                    'sender_id': 'Sender ID can only contain letters, numbers, and underscores'
                })
        
        # Validate channels
        if not self.channels:
            raise ValidationError({
                'channels': 'At least one channel must be selected'
            })
        
        valid_channels = [choice[0] for choice in self.CHANNEL_CHOICES]
        for channel in self.channels:
            if channel not in valid_channels:
                raise ValidationError({
                    'channels': f"Invalid channel: {channel}. Valid options are: {valid_channels}"
                })

    def save(self, *args, **kwargs):
        """Auto-set timestamps based on execution status changes"""
        # Track execution status changes
        if self.pk:
            try:
                old = Campaign.objects.get(pk=self.pk)
                if old.execution_status != self.execution_status:
                    # Status changed
                    if self.execution_status == 'PROCESSING' and not self.execution_started_at:
                        self.execution_started_at = timezone.now()
                    elif self.execution_status == 'COMPLETED' and not self.execution_completed_at:
                        self.execution_completed_at = timezone.now()
                    elif self.execution_status == 'PAUSED' and not self.execution_paused_at:
                        self.execution_paused_at = timezone.now()
            except Campaign.DoesNotExist:
                pass
        
        self.full_clean()
        super().save(*args, **kwargs)

    # =============== CHANNEL METHODS ===============

    def get_channels_display(self):
        """Return human-readable channel names"""
        channel_map = dict(self.CHANNEL_CHOICES)
        return [channel_map.get(ch, ch) for ch in self.channels]

    def has_channel(self, channel):
        """Check if campaign has a specific channel"""
        return channel in self.channels

    # =============== EXECUTION CONTROL METHODS ===============

    def can_start(self):
        """Check if campaign can be started by Layer 2"""
        return (
            self.status == 'draft' and
            self.execution_status == 'PENDING' and
            hasattr(self, 'schedule') and
            hasattr(self, 'message_content') and
            hasattr(self, 'audience') and
            self.audience.valid_count > 0 and
            self.sender_id and
            self.channels
        )
    
    def can_pause(self):
        """Check if campaign can be paused"""
        return self.execution_status == 'PROCESSING'
    
    def can_resume(self):
        """Check if campaign can be resumed"""
        return self.execution_status == 'PAUSED'
    
    def can_stop(self):
        """Check if campaign can be stopped"""
        return self.execution_status in ['PROCESSING', 'PAUSED']
    
    def can_complete(self):
        """Check if campaign can be marked as completed"""
        return self.execution_status in ['PROCESSING', 'PAUSED']
    
    def soft_delete(self):
        """Soft delete the campaign"""
        self.is_deleted = True
        self.deleted_at = timezone.now()
        self.save()

    # =============== LAYER 1/2 API PROPERTIES ===============

    @property
    def summary(self):
        """Summary data for Layer 1 API responses (validation)"""
        return {
            'id': self.id,
            'name': self.name,
            'status': self.status,
            'execution_status': self.execution_status,
            'sender_id': self.sender_id,
            'channels': self.channels,
            'total_recipients': getattr(self.audience, 'total_count', 0) if hasattr(self, 'audience') else 0,
            'valid_recipients': getattr(self.audience, 'valid_count', 0) if hasattr(self, 'audience') else 0,
            'has_content': hasattr(self, 'message_content'),
            'has_schedule': hasattr(self, 'schedule'),
            'has_audience': hasattr(self, 'audience'),
            'last_processed': self.last_processed_id,
            'total_processed': self.total_processed,
            'progress_percent': self.progress_percentage,
        }
    
    @property
    def message_content_data(self):
        """Message content for Layer 2 (templates)"""
        if not hasattr(self, 'message_content'):
            return {
                'content': {},
                'default_language': 'en',
                'templates': {}
            }
        return {
            'content': self.message_content.content,
            'default_language': self.message_content.default_language,
            'templates': self.message_content.content,  # Alias for Layer 2
        }
    
    @property
    def audience_data(self):
        """Audience info for Layer 2 (direct DB access)"""
        if not hasattr(self, 'audience'):
            return {
                'summary': {'total': 0, 'valid': 0, 'invalid': 0},
                'database_info': {
                    'table': 'scheduler_manager_audience',
                    'id_field': 'id',
                    'filter': f"campaign_id = {self.id} AND status = 'PENDING'"
                }
            }
        return {
            'summary': {
                'total': self.audience.total_count,
                'valid': self.audience.valid_count,
                'invalid': self.audience.invalid_count,
            },
            'database_info': {
                'table': self.audience.database_table,
                'id_field': self.audience.id_field,
                'filter': self.audience.filter_condition or f"campaign_id = {self.id} AND status = 'PENDING'",
            }
        }
    
    @property
    def checkpoint_info(self):
        """Checkpoint info for Layer 2 (resumability)"""
        return {
            'last_processed': self.last_processed_id,
            'total_processed': self.total_processed,
            'has_checkpoint': self.last_processed_id > 0,
            'execution_status': self.execution_status,
            'progress_percent': self.progress_percentage,
        }
    
    @property
    def progress_percentage(self):
        """Calculate progress percentage"""
        total = getattr(self.audience, 'valid_count', 0) if hasattr(self, 'audience') else 0
        if total > 0:
            return round((self.total_processed / total) * 100, 2)
        return 0.0
    
    @property
    def provider_stats(self):
        """Provider statistics for monitoring"""
        return {
            'sent': self.sent_count,
            'delivered': self.delivered_count,
            'failed': self.failed_count,
            'pending': self.pending_count,
            'total': self.total_messages,
        }


# =============== ENHANCED SCHEDULE MODEL ===============

class Schedule(models.Model):
    """
    Campaign scheduling model with support for both one-time and recurring campaigns.
    
    For one-time campaigns:
    - Type = 'once'
    - After execution, status becomes 'completed'
    
    For recurring campaigns:
    - Type = 'daily', 'weekly', or 'monthly'
    - After each execution window, automatically resets for next window
    - Tracks current execution round
    """
    
    SCHEDULE_TYPE_CHOICES = [
        ('once', 'One Time'),          # Single execution, then completed
        ('daily', 'Daily'),             # Recurring daily
        ('weekly', 'Weekly'),           # Recurring weekly (on specified days)
        ('monthly', 'Monthly'),         # Recurring monthly
    ]
    
    WINDOW_STATUS_CHOICES = [
        ('pending', 'Pending'),          # Not yet started
        ('active', 'Active'),             # Currently inside send window
        ('completed', 'Completed'),       # Window finished successfully
        ('partial', 'Partial'),           # Window finished but with issues
        ('skipped', 'Skipped'),           # Window was skipped (no run day)
    ]
    
    CAMPAIGN_STATUS_CHOICES = [
        ('active', 'Active'),              # Overall campaign is active
        ('paused', 'Paused'),              # Manually paused
        ('stopped', 'Stopped'),            # Manually stopped
        ('completed', 'Completed'),        # All windows completed (for 'once')
        ('expired', 'Expired'),            # Past end_date
    ]
    
    campaign = models.OneToOneField(
        Campaign, 
        on_delete=models.CASCADE, 
        related_name='schedule'
    )
    
    # =============== BASIC SCHEDULE INFO ===============
    schedule_type = models.CharField(
        max_length=20,
        choices=SCHEDULE_TYPE_CHOICES,
        default='once',
        help_text="Type of schedule: one-time or recurring"
    )
    
    # Date range (for both one-time and recurring)
    start_date = models.DateField(
        help_text="First date when campaign can run"
    )
    end_date = models.DateField(
        null=True, 
        blank=True,
        help_text="Last date when campaign can run (null for infinite)"
    )
    
    # For weekly recurring: which days of the week
    run_days = models.JSONField(
        default=list,
        help_text="Days of week to run (0=Monday to 6=Sunday). For daily, use [0,1,2,3,4,5,6]"
    )
    
    # Time windows (multiple windows per day supported)
    time_windows = models.JSONField(
        default=list,
        help_text="List of time windows: [{'start': '08:00', 'end': '12:00'}, ...]"
    )
    
    # Timezone
    timezone = models.CharField(
        max_length=50,
        default='UTC',
        help_text="Timezone for schedule (e.g., 'Africa/Addis_Ababa')"
    )
    
    # =============== STATUS TRACKING ===============
    campaign_status = models.CharField(
        max_length=20,
        choices=CAMPAIGN_STATUS_CHOICES,
        default='active',
        db_index=True
    )
    
    # Current execution tracking
    current_round = models.PositiveIntegerField(
        default=0,
        help_text="Current execution round (increments after each window)"
    )
    current_window_date = models.DateField(
        null=True, 
        blank=True,
        help_text="Current date being processed"
    )
    current_window_index = models.IntegerField(
        default=0,
        help_text="Index of current time window (0-based)"
    )
    current_window_status = models.CharField(
        max_length=20,
        choices=WINDOW_STATUS_CHOICES,
        default='pending'
    )
    
    # Next scheduled execution
    next_run_date = models.DateField(
        null=True, 
        blank=True,
        db_index=True,
        help_text="Next date when campaign will run"
    )
    next_run_window = models.IntegerField(
        null=True, 
        blank=True,
        help_text="Index of next time window to run"
    )
    
    # History tracking
    completed_windows = models.JSONField(
        default=list,
        help_text="History of completed windows: [{'date': '2024-01-01', 'window': 0, 'status': 'completed'}]"
    )
    total_windows_completed = models.PositiveIntegerField(default=0)
    
    # Control flags
    is_active = models.BooleanField(default=True)
    auto_reset = models.BooleanField(
        default=True,
        help_text="Automatically reset for next window after completion"
    )
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_processed_at = models.DateTimeField(
        null=True, 
        blank=True,
        help_text="Last time this schedule was processed"
    )

    class Meta:
        indexes = [
            models.Index(fields=['campaign_status', 'is_active']),
            models.Index(fields=['schedule_type', 'next_run_date']),
            models.Index(fields=['start_date', 'end_date']),
            models.Index(fields=['current_window_status']),
        ]

    def __str__(self):
        return f"Schedule for {self.campaign.name} - Type: {self.schedule_type}, Round: {self.current_round}"

    def clean(self):
        """Validate schedule configuration"""
        super().clean()
        
        # Validate dates
        if self.end_date and self.start_date > self.end_date:
            raise ValidationError({
                'end_date': "End date must be after start date"
            })
        
        # Validate time windows
        self._validate_time_windows()
        
        # Validate run_days for weekly
        if self.schedule_type == 'weekly':
            self._validate_run_days()
        
        # For 'once' schedule, ensure end_date matches start_date or is null
        if self.schedule_type == 'once' and self.end_date and self.end_date != self.start_date:
            raise ValidationError({
                'end_date': "For one-time schedules, end_date should equal start_date or be null"
            })

    def _validate_time_windows(self):
        """Validate time windows format and ordering"""
        if not self.time_windows:
            raise ValidationError({
                'time_windows': "At least one time window is required"
            })
        
        for i, window in enumerate(self.time_windows):
            # Check required fields
            if 'start' not in window or 'end' not in window:
                raise ValidationError({
                    'time_windows': f"Window {i} must have 'start' and 'end' times"
                })
            
            # Validate time format
            try:
                start_time = datetime.strptime(window['start'], '%H:%M').time()
                end_time = datetime.strptime(window['end'], '%H:%M').time()
            except ValueError:
                raise ValidationError({
                    'time_windows': f"Window {i}: Invalid time format. Use HH:MM (24-hour)"
                })
            
            # Validate start before end
            if start_time >= end_time:
                raise ValidationError({
                    'time_windows': f"Window {i}: start time must be before end time"
                })
            
            # Check for overlapping windows
            for j, other in enumerate(self.time_windows[:i]):
                other_start = datetime.strptime(other['start'], '%H:%M').time()
                other_end = datetime.strptime(other['end'], '%H:%M').time()
                
                if (start_time < other_end and end_time > other_start):
                    raise ValidationError({
                        'time_windows': f"Window {i} overlaps with window {j}"
                    })

    def _validate_run_days(self):
        """Validate run_days for weekly schedules"""
        valid_days = [0, 1, 2, 3, 4, 5, 6]
        for day in self.run_days:
            if not isinstance(day, int) or day not in valid_days:
                raise ValidationError({
                    'run_days': f"Invalid day: {day}. Use integers 0-6 (Monday=0, Sunday=6)"
                })

    # =============== SCHEDULE MANAGEMENT METHODS ===============

    def initialize_schedule(self):
        """Initialize schedule on campaign start"""
        self.current_round = 0
        self.campaign_status = 'active'
        self._calculate_next_run()
        self.save()
        logger.info(f"Schedule initialized for campaign {self.campaign.id}")

    def get_next_window(self):
        """
        Determine next window to execute based on schedule type.
        Returns (next_date, window_index) or (None, None) if no more windows.
        """
        today = date.today()
        
        # If past end_date, no more windows
        if self.end_date and today > self.end_date:
            self.campaign_status = 'expired'
            self.save()
            return None, None
        
        # If campaign is paused or stopped
        if self.campaign_status != 'active':
            return None, None
        
        # For one-time schedules
        if self.schedule_type == 'once':
            if self.current_round == 0:
                # First window on start_date
                if today >= self.start_date:
                    return self.start_date, 0
            return None, None
        
        # For daily schedules
        if self.schedule_type == 'daily':
            return self._get_next_daily_window()
        
        # For weekly schedules
        if self.schedule_type == 'weekly':
            return self._get_next_weekly_window()
        
        # For monthly schedules
        if self.schedule_type == 'monthly':
            return self._get_next_monthly_window()
        
        return None, None

    def _get_next_daily_window(self):
        """Get next window for daily schedule"""
        current_date = date.today()
        
        # If we haven't started yet
        if current_date < self.start_date:
            return self.start_date, 0
        
        # If we have a current window date, move to next day
        if self.current_window_date:
            next_date = self.current_window_date + timedelta(days=1)
        else:
            next_date = current_date
        
        # Check if past end_date
        if self.end_date and next_date > self.end_date:
            return None, None
        
        return next_date, 0  # Always use first window for daily

    def _get_next_weekly_window(self):
        """Get next window for weekly schedule (respecting run_days)"""
        current_date = date.today()
        check_date = max(current_date, self.start_date)
        
        # Look ahead up to 14 days for next run day
        for days_ahead in range(14):
            candidate = check_date + timedelta(days=days_ahead)
            
            # Check if past end_date
            if self.end_date and candidate > self.end_date:
                return None, None
            
            # Check if this day should run (0=Monday in Python's weekday)
            if candidate.weekday() in self.run_days:
                return candidate, 0  # Always use first window
        
        return None, None

    def _get_next_monthly_window(self):
        """Get next window for monthly schedule"""
        current_date = date.today()
        
        if not self.current_window_date:
            # First run
            next_date = max(current_date, self.start_date)
            # Set to same day of month as start_date
            if next_date.day != self.start_date.day:
                if next_date.day < self.start_date.day:
                    # Move to this month's run day
                    try:
                        next_date = next_date.replace(day=self.start_date.day)
                    except ValueError:
                        # Day doesn't exist (e.g., Feb 30), use last day of month
                        import calendar
                        last_day = calendar.monthrange(next_date.year, next_date.month)[1]
                        next_date = next_date.replace(day=last_day)
                else:
                    # Move to next month's run day
                    next_date = next_date + relativedelta(months=1)
                    try:
                        next_date = next_date.replace(day=self.start_date.day)
                    except ValueError:
                        import calendar
                        last_day = calendar.monthrange(next_date.year, next_date.month)[1]
                        next_date = next_date.replace(day=last_day)
        else:
            # Next month
            next_date = self.current_window_date + relativedelta(months=1)
            try:
                next_date = next_date.replace(day=self.start_date.day)
            except ValueError:
                import calendar
                last_day = calendar.monthrange(next_date.year, next_date.month)[1]
                next_date = next_date.replace(day=last_day)
        
        # Check if past end_date
        if self.end_date and next_date > self.end_date:
            return None, None
        
        return next_date, 0

    def _calculate_next_run(self):
        """Update next_run_date and next_run_window"""
        next_date, next_window = self.get_next_window()
        self.next_run_date = next_date
        self.next_run_window = next_window
        self.save()

    def start_window(self, window_date, window_index):
        """
        Mark a window as started.
        Called by scheduler when it's time to execute.
        """
        self.current_window_date = window_date
        self.current_window_index = window_index
        self.current_window_status = 'active'
        self.last_processed_at = timezone.now()
        self.save()
        
        logger.info(f"Started window {window_date} (index {window_index}) for campaign {self.campaign.id}")
        
        # Update campaign execution status
        campaign = self.campaign
        campaign.execution_status = 'PROCESSING'
        campaign.save()

    def complete_window(self, status='completed', stats=None):
        """
        Mark current window as completed.
        For recurring campaigns, automatically resets for next window.
        
        Args:
            status: 'completed', 'partial', or 'skipped'
            stats: Optional dict with execution stats
        """
        if not self.current_window_date:
            logger.warning(f"No active window to complete for campaign {self.campaign.id}")
            return
        
        # Record completed window
        window_record = {
            'date': self.current_window_date.isoformat(),
            'window': self.current_window_index,
            'status': status,
            'completed_at': timezone.now().isoformat(),
            'stats': stats or {}
        }
        
        # Ensure completed_windows is a list
        if not isinstance(self.completed_windows, list):
            self.completed_windows = []
        
        self.completed_windows.append(window_record)
        self.total_windows_completed += 1
        
        # Update campaign totals if stats provided
        if stats and 'messages_sent' in stats:
            self.campaign.total_processed += stats['messages_sent']
            self.campaign.save()
        
        # Handle based on schedule type
        if self.schedule_type == 'once':
            # One-time campaign is now complete
            self.campaign_status = 'completed'
            self.current_window_status = 'completed'
            self.current_window_date = None
            
            # Update campaign
            campaign = self.campaign
            campaign.execution_status = 'COMPLETED'
            campaign.status = 'completed'
            campaign.execution_completed_at = timezone.now()
            campaign.save()
            
            logger.info(f"One-time campaign {self.campaign.id} completed")
        
        elif self.auto_reset:
            # Recurring campaign: reset for next window
            self.current_round += 1
            self.current_window_status = 'pending'
            self.current_window_date = None
            
            # Calculate next run
            self._calculate_next_run()
            
            # If no more runs, mark as completed
            if not self.next_run_date:
                self.campaign_status = 'completed'
                campaign = self.campaign
                campaign.execution_status = 'COMPLETED'
                campaign.save()
                logger.info(f"Recurring campaign {self.campaign.id} completed (no more windows)")
            else:
                logger.info(f"Campaign {self.campaign.id} completed round {self.current_round}, next run: {self.next_run_date}")
        
        self.save()

    def pause(self):
        """Pause the schedule"""
        self.campaign_status = 'paused'
        self.save()
        
        # Update campaign
        campaign = self.campaign
        campaign.execution_status = 'PAUSED'
        campaign.execution_paused_at = timezone.now()
        campaign.save()
        
        logger.info(f"Schedule paused for campaign {self.campaign.id}")

    def resume(self):
        """Resume the schedule"""
        self.campaign_status = 'active'
        self._calculate_next_run()
        self.save()
        
        # Update campaign
        campaign = self.campaign
        campaign.execution_status = 'PROCESSING' if self.current_window_status == 'active' else 'PENDING'
        campaign.save()
        
        logger.info(f"Schedule resumed for campaign {self.campaign.id}")

    def stop(self):
        """Permanently stop the schedule"""
        self.campaign_status = 'stopped'
        self.is_active = False
        self.save()
        
        # Update campaign
        campaign = self.campaign
        campaign.execution_status = 'STOPPED'
        campaign.save()
        
        logger.info(f"Schedule stopped for campaign {self.campaign.id}")

    def skip_current_window(self, reason=None):
        """Skip the current window (e.g., due to technical issues)"""
        self.complete_window(status='skipped', stats={'reason': reason})

    # =============== QUERY METHODS ===============

    def is_window_active(self, check_time=None):
        """Check if currently inside an active window"""
        if not self.current_window_date or self.current_window_status != 'active':
            return False
        
        if check_time is None:
            check_time = datetime.now().time()
        
        # Get current window times
        window = self.time_windows[self.current_window_index]
        start_time = datetime.strptime(window['start'], '%H:%M').time()
        end_time = datetime.strptime(window['end'], '%H:%M').time()
        
        return start_time <= check_time <= end_time

    def get_upcoming_windows(self, limit=5):
        """Get list of upcoming windows for display"""
        upcoming = []
        current_date = date.today()
        check_date = max(current_date, self.start_date)
        
        while len(upcoming) < limit and (not self.end_date or check_date <= self.end_date):
            if self.schedule_type == 'daily':
                upcoming.append({
                    'date': check_date.isoformat(),
                    'windows': list(range(len(self.time_windows))),
                    'type': 'daily'
                })
                check_date += timedelta(days=1)
            
            elif self.schedule_type == 'weekly':
                if check_date.weekday() in self.run_days:
                    upcoming.append({
                        'date': check_date.isoformat(),
                        'windows': list(range(len(self.time_windows))),
                        'type': 'weekly',
                        'day_name': check_date.strftime('%A')
                    })
                check_date += timedelta(days=1)
            
            elif self.schedule_type == 'monthly':
                # Simplified: just add the next candidate
                upcoming.append({
                    'date': check_date.isoformat(),
                    'windows': list(range(len(self.time_windows))),
                    'type': 'monthly'
                })
                check_date += relativedelta(months=1)
            
            elif self.schedule_type == 'once':
                if len(upcoming) == 0 and self.current_round == 0:
                    upcoming.append({
                        'date': self.start_date.isoformat(),
                        'windows': list(range(len(self.time_windows))),
                        'type': 'once'
                    })
                break
        
        return upcoming

    def get_schedule_summary(self):
        """Get human-readable schedule summary"""
        if self.schedule_type == 'once':
            return f"One-time on {self.start_date} at {self.time_windows[0]['start']}"
        
        days_map = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        if self.schedule_type == 'daily':
            days_str = "Every day"
        elif self.schedule_type == 'weekly':
            day_names = [days_map[d] for d in sorted(self.run_days)]
            days_str = f"Every {', '.join(day_names)}"
        else:  # monthly
            days_str = f"Monthly on day {self.start_date.day}"
        
        windows_str = ', '.join([f"{w['start']}-{w['end']}" for w in self.time_windows])
        
        date_range = f"from {self.start_date}"
        if self.end_date:
            date_range += f" to {self.end_date}"
        
        return f"{days_str} at {windows_str} {date_range}"


# =============== MESSAGE CONTENT MODEL ===============

class MessageContent(models.Model):
    """Multi-language message content for campaigns"""
    
    campaign = models.OneToOneField(
        Campaign, 
        on_delete=models.CASCADE, 
        related_name='message_content'
    )
    content = models.JSONField(default=_default_message_content)
    default_language = models.CharField(
        max_length=10, 
        default='en',
        choices=[(lang, lang.upper()) for lang in SUPPORTED_LANGUAGES]
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['default_language']),
        ]

    def __str__(self):
        return f"Message Content for {self.campaign.name}"

    def clean(self):
        """Ensure the JSON has exactly the supported language keys."""
        super().clean()
        
        if not isinstance(self.content, dict):
            raise ValidationError({'content': "Content must be a dictionary"})
        
        if set(self.content.keys()) != set(SUPPORTED_LANGUAGES):
            missing = set(SUPPORTED_LANGUAGES) - set(self.content.keys())
            extra = set(self.content.keys()) - set(SUPPORTED_LANGUAGES)
            errors = []
            if missing:
                errors.append(f"Missing languages: {sorted(missing)}")
            if extra:
                errors.append(f"Unexpected languages: {sorted(extra)}")
            raise ValidationError({'content': "; ".join(errors)})
        
        # Validate default_language is in content
        if self.default_language not in self.content:
            raise ValidationError({
                'default_language': f"Default language '{self.default_language}' not found in content"
            })

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def get_message(self, language=None):
        """Get message in specified language or default"""
        lang = language or self.default_language
        return self.content.get(lang, self.content.get(self.default_language, ""))


# =============== AUDIENCE MODEL ===============

class Audience(models.Model):
    """Campaign audience/recipients model with direct DB access info for Layer 2"""
    
    campaign = models.OneToOneField(
        Campaign, 
        on_delete=models.CASCADE, 
        related_name='audience'
    )
    recipients = models.JSONField(
        help_text="List of recipients with msisdn and lang"
    )
    total_count = models.PositiveIntegerField(default=0)
    valid_count = models.PositiveIntegerField(default=0)
    invalid_count = models.PositiveIntegerField(default=0)
    
    # =============== LAYER 2 INTEGRATION FIELDS ===============
    # These tell Layer 2 how to access the audience table directly
    
    database_table = models.CharField(
        max_length=100,
        default='scheduler_manager_audience',
        help_text="Table name where recipients are stored for direct DB access"
    )
    id_field = models.CharField(
        max_length=50,
        default='id',
        help_text="Primary key field name for keyset pagination"
    )
    filter_condition = models.TextField(
        blank=True,
        help_text="Additional SQL WHERE clause for filtering recipients (e.g., 'AND status = 'PENDING'')"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['total_count']),
            models.Index(fields=['database_table']),  # For Layer 2 queries
        ]

    def __str__(self):
        return f"Audience for {self.campaign.name} ({self.total_count} recipients)"

    def clean(self):
        """Validate recipients data and update counts"""
        super().clean()
        
        if not isinstance(self.recipients, list):
            raise ValidationError({'recipients': "recipients must be a list"})
        
        # Validate recipient count
        if len(self.recipients) > 1000000:  # 1M limit
            raise ValidationError({
                'recipients': "Recipients list exceeds maximum limit of 1,000,000"
            })
        
        # Validate each recipient
        valid_count = 0
        invalid_count = 0
        
        for i, recipient in enumerate(self.recipients):
            if not isinstance(recipient, dict):
                invalid_count += 1
                continue
            
            # Check required keys
            if 'msisdn' not in recipient or 'lang' not in recipient:
                invalid_count += 1
                continue
            
            # Validate phone number format (basic)
            if not isinstance(recipient['msisdn'], str) or not recipient['msisdn'].strip():
                invalid_count += 1
                continue
            
            # Validate language
            if recipient['lang'] not in SUPPORTED_LANGUAGES:
                invalid_count += 1
                continue
            
            valid_count += 1
        
        self.total_count = len(self.recipients)
        self.valid_count = valid_count
        self.invalid_count = invalid_count

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)


# =============== CAMPAIGN PROGRESS MODEL ===============

class CampaignProgress(models.Model):
    """
    Tracks the high-level health of a campaign.
    Updated by Layer 4 via Kafka events.
    """
    
    STATUS_CHOICES = [
        ('ACTIVE', 'Active'),
        ('COMPLETED', 'Completed'),
        ('STOPPED', 'Stopped'),
        ('FAILED', 'Failed'),
    ]

    campaign = models.OneToOneField(
        Campaign, 
        on_delete=models.CASCADE, 
        primary_key=True,
        related_name='progress'
    )
    total_messages = models.IntegerField(default=0, validators=[MinValueValidator(0)])
    sent_count = models.PositiveIntegerField(default=0)
    delivered_count = models.PositiveIntegerField(default=0)
    failed_count = models.PositiveIntegerField(default=0)
    pending_count = models.PositiveIntegerField(default=0)
    progress_percent = models.DecimalField(
        max_digits=5, 
        decimal_places=2, 
        default=0.00,
        validators=[MinValueValidator(0), MaxValueValidator(100)]
    )
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='ACTIVE')
    
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['progress_percent']),
        ]
        verbose_name_plural = "Campaign Progresses"

    def __str__(self):
        return f"Progress for {self.campaign.name}: {self.progress_percent}%"

    def update_percentage(self):
        """Update progress percentage based on sent and failed counts"""
        if self.total_messages > 0:
            completed = self.sent_count + self.failed_count
            self.progress_percent = (completed / self.total_messages) * 100
            
            # Also update campaign totals
            campaign = self.campaign
            campaign.sent_count = self.sent_count
            campaign.delivered_count = self.delivered_count
            campaign.failed_count = self.failed_count
            campaign.pending_count = self.pending_count
            campaign.total_messages = self.total_messages
            
            # Auto-complete if all messages processed
            if completed >= self.total_messages and self.status != 'COMPLETED':
                self.status = 'COMPLETED'
                self.completed_at = timezone.now()
                campaign.execution_status = 'COMPLETED'
                campaign.execution_completed_at = timezone.now()
            
            campaign.save()
            self.save()

    def increment_sent(self, count=1):
        """Increment sent count (called by Layer 4 via signal)"""
        self.sent_count += count
        self.pending_count = max(0, self.pending_count - count)
        self.update_percentage()

    def increment_delivered(self, count=1):
        """Increment delivered count (called by Layer 4)"""
        self.delivered_count += count
        self.update_percentage()

    def increment_failed(self, count=1):
        """Increment failed count (called by Layer 4)"""
        self.failed_count += count
        self.pending_count = max(0, self.pending_count - count)
        self.update_percentage()


# =============== BATCH STATUS MODEL ===============

class BatchStatus(models.Model):
    """
    Groups messages into chunks for efficiency.
    Tracks the status of each message batch.
    Used by Layer 2 and Layer 4 for tracking.
    """
    
    BATCH_STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('IN_PROGRESS', 'In Progress'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]
    
    batch_id = models.CharField(max_length=50, primary_key=True)
    campaign = models.ForeignKey(
        CampaignProgress, 
        on_delete=models.CASCADE, 
        related_name='batches'
    )
    campaign_id_direct = models.IntegerField(
        db_index=True, 
        null=True,
        help_text="Direct campaign ID for faster queries without joins"
    )
    total_messages = models.IntegerField(default=0, validators=[MinValueValidator(0)])
    success_count = models.PositiveIntegerField(default=0)
    failed_count = models.PositiveIntegerField(default=0)
    status = models.CharField(
        max_length=20, 
        choices=BATCH_STATUS_CHOICES,
        default='PENDING'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['campaign_id_direct']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f"Batch {self.batch_id} - {self.status}"

    def save(self, *args, **kwargs):
        # Auto-set campaign_id_direct for direct queries (used by Layer 4)
        if self.campaign_id and not self.campaign_id_direct:
            self.campaign_id_direct = self.campaign.campaign_id
        super().save(*args, **kwargs)

    def update_status(self):
        """Update batch status based on message counts"""
        if self.success_count + self.failed_count >= self.total_messages:
            self.status = 'COMPLETED'
            self.completed_at = timezone.now()
        self.save()


# =============== MESSAGE STATUS MODEL ===============

class MessageStatus(models.Model):
    """
    The high-volume receipt table.
    Uses campaign_id (not FK) to avoid heavy join overhead during high TPS writes.
    Updated by Layer 3 (via Kafka) and Layer 4.
    """
    
    MESSAGE_STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('SENT', 'Sent'),
        ('DELIVERED', 'Delivered'),
        ('FAILED', 'Failed'),
        ('RETRYING', 'Retrying'),
    ]
    
    message_id = models.CharField(max_length=100, primary_key=True)
    campaign_id = models.IntegerField(db_index=True)
    batch_id = models.CharField(max_length=50, db_index=True)
    phone_number = models.CharField(max_length=20, db_index=True)
    
    # Sender and channel info
    sender_id = models.CharField(
        max_length=11, 
        blank=True, 
        null=True,
        help_text="Sender ID used for this message"
    )
    channel = models.CharField(
        max_length=20,
        choices=Campaign.CHANNEL_CHOICES,
        default='sms',
        help_text="Channel used for this message (sms, app_notification, flash_sms)"
    )
    
    # Status tracking
    status = models.CharField(
        max_length=20, 
        choices=MESSAGE_STATUS_CHOICES,
        default='PENDING',
        db_index=True
    )
    attempts = models.PositiveSmallIntegerField(default=0)
    last_attempt = models.DateTimeField(null=True, blank=True)
    
    # =============== LAYER 3/4 INTEGRATION FIELDS ===============
    # Provider tracking fields
    
    provider_message_id = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        db_index=True,
        help_text="Provider's message ID for tracking (from Layer 3)"
    )
    provider_status = models.CharField(
        max_length=50,
        blank=True,
        help_text="Raw status from provider"
    )
    provider_response = models.TextField(
        null=True, 
        blank=True,
        help_text="Provider response message"
    )
    provider_response_raw = models.JSONField(
        null=True,
        blank=True,
        help_text="Complete provider response for debugging"
    )
    
    # Timing fields
    sent_at = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="When message was sent to provider"
    )
    delivered_at = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="When delivery was confirmed"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['campaign_id', 'status']),
            models.Index(fields=['batch_id']),
            models.Index(fields=['created_at']),
            models.Index(fields=['status', 'last_attempt']),
            models.Index(fields=['phone_number', 'campaign_id']),
            models.Index(fields=['sender_id']),
            models.Index(fields=['channel']),
            models.Index(fields=['provider_message_id']),  # For provider lookups
            models.Index(fields=['sent_at']),  # For time-based queries
            models.Index(fields=['delivered_at']),  # For delivery tracking
        ]
        verbose_name_plural = "Message Statuses"

    def __str__(self):
        return f"Message {self.message_id} - {self.status} (Channel: {self.channel})"

    def save(self, *args, **kwargs):
        is_new = not self.pk
        
        # Auto-set timestamps based on status
        if self.status == 'SENT' and not self.sent_at:
            self.sent_at = timezone.now()
        elif self.status == 'DELIVERED' and not self.delivered_at:
            self.delivered_at = timezone.now()
        
        super().save(*args, **kwargs)
        
        # Log important status changes
        if is_new:
            logger.debug(f"New message created: {self.message_id}")
        elif self.status in ['FAILED', 'DELIVERED']:
            logger.info(f"Message {self.message_id} status: {self.status}")

    def can_retry(self, max_attempts=3):
        """Check if message can be retried (used by Layer 3)"""
        return self.status == 'FAILED' and self.attempts < max_attempts


# =============== CHECKPOINT MODEL ===============

class Checkpoint(models.Model):
    """
    Prevents resending if the Ingestion Layer restarts.
    Tracks processing progress for large campaigns.
    Updated by Layer 2 during execution.
    """
    
    CHECKPOINT_STATUS_CHOICES = [
        ('RUNNING', 'Running'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
        ('PAUSED', 'Paused'),
    ]
    
    campaign = models.OneToOneField(
        Campaign, 
        on_delete=models.CASCADE, 
        primary_key=True,
        related_name='checkpoint'
    )
    last_processed_index = models.PositiveIntegerField(default=0)
    total_to_process = models.PositiveIntegerField(default=0)
    status = models.CharField(
        max_length=20, 
        choices=CHECKPOINT_STATUS_CHOICES,
        default='RUNNING'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return f"Checkpoint for {self.campaign.name} - Index: {self.last_processed_index}"

    def progress_percent(self):
        """Calculate progress percentage"""
        if self.total_to_process > 0:
            return (self.last_processed_index / self.total_to_process) * 100
        return 0


# =============== MODEL TRACKING UTILITY ===============

class ModelTracker:
    """Simple model field change tracker for detecting status changes"""
    
    def __init__(self, instance):
        self.instance = instance
        self._initial = self._get_state()
    
    def _get_state(self):
        return {
            field.name: getattr(self.instance, field.name)
            for field in self.instance._meta.fields
        }
    
    def has_changed(self, field):
        return self._initial.get(field) != getattr(self.instance, field)


# Add tracker property to MessageStatus
@property
def message_status_tracker(self):
    return ModelTracker(self)

MessageStatus.tracker = message_status_tracker


# =============== SIGNALS ===============

@receiver(post_save, sender=Campaign)
def create_campaign_related_models(sender, instance, created, **kwargs):
    """Create all related models when a campaign is created"""
    if created:
        # Create message content
        MessageContent.objects.get_or_create(campaign=instance)
        
        # Create progress tracker
        CampaignProgress.objects.get_or_create(campaign=instance)
        
        # Create checkpoint
        Checkpoint.objects.get_or_create(
            campaign=instance,
            defaults={'total_to_process': getattr(instance.audience, 'valid_count', 0) if hasattr(instance, 'audience') else 0}
        )
        
        logger.info(f"Created related models for campaign {instance.id}")


@receiver(post_save, sender=Schedule)
def schedule_post_save(sender, instance, created, **kwargs):
    """Initialize schedule on creation"""
    if created:
        instance.initialize_schedule()


@receiver(post_save, sender=MessageStatus)
def update_batch_and_progress(sender, instance, created, **kwargs):
    """Update batch status and campaign progress when message status changes"""
    if not created and hasattr(instance, 'tracker') and instance.tracker.has_changed('status'):
        try:
            # Update batch status
            batch = BatchStatus.objects.filter(batch_id=instance.batch_id).first()
            if batch:
                if instance.status in ['SENT', 'DELIVERED']:
                    batch.success_count += 1
                elif instance.status == 'FAILED':
                    batch.failed_count += 1
                batch.update_status()
            
            # Update campaign progress
            progress = CampaignProgress.objects.filter(
                campaign_id=instance.campaign_id
            ).first()
            if progress:
                if instance.status in ['SENT', 'DELIVERED']:
                    progress.increment_sent()
                elif instance.status == 'FAILED':
                    progress.increment_failed()
                    
        except Exception as e:
            logger.error(f"Error updating batch/progress for message {instance.message_id}: {e}")