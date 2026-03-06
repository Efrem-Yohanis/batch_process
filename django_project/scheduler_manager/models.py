from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils import timezone
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

SUPPORTED_LANGUAGES = ['en', 'am', 'ti', 'om', 'so']


def _default_message_content():
    """Return a dict with all supported languages set to an empty string.
    Used as the default factory for the JSONField so that new
    :class:`MessageContent` objects already contain the five keys.
    """
    return {lang: "" for lang in SUPPORTED_LANGUAGES}


class Campaign(models.Model):
    """Core campaign model for SMS marketing campaigns"""
    
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('completed', 'Completed'),
        ('archived', 'Archived'),
    ]
    
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft')
    created_by = models.ForeignKey('auth.User', on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Soft delete fields
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.name} (ID: {self.id})"

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['created_by']),
        ]

    def can_start(self):
        """Check if campaign can be started"""
        return (
            self.status == 'draft' and
            hasattr(self, 'schedule') and
            hasattr(self, 'message_content') and
            hasattr(self, 'audience') and
            self.audience.recipients
        )
    
    def can_pause(self):
        """Check if campaign can be paused"""
        return self.status == 'active'
    
    def can_complete(self):
        """Check if campaign can be marked as completed"""
        return self.status in ['active', 'paused']
    
    def soft_delete(self):
        """Soft delete the campaign"""
        self.is_deleted = True
        self.deleted_at = timezone.now()
        self.save()


class Schedule(models.Model):
    """Campaign scheduling model with comprehensive validation"""
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),   # Created, not yet started
        ('running', 'Running'),   # Inside 8:00 AM - 10:00 PM window
        ('stop', 'Stop'),         # Outside window OR manually paused
        ('completed', 'Completed'), # End date/time reached
    ]

    campaign = models.OneToOneField(
        Campaign, 
        on_delete=models.CASCADE, 
        related_name='schedule'
    )
    start_date = models.DateField()
    end_date = models.DateField(null=True, blank=True)
    frequency = models.CharField(max_length=20) 
    run_days = models.JSONField(default=list)      # [0,1,2,3,4,5,6] where 0=Monday
    send_times = models.JSONField(default=list)    # e.g., ["08:00", "14:00"]
    end_times = models.JSONField(default=list)     # e.g., ["12:00", "22:00"]
    is_active = models.BooleanField(default=True)
    status = models.CharField(
        max_length=20, 
        choices=STATUS_CHOICES, 
        default='pending'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['status', 'is_active']),
            models.Index(fields=['start_date', 'end_date']),
        ]

    def __str__(self):
        return f"Schedule for {self.campaign.name} ({self.status})"

    def clean(self):
        """Add comprehensive validation"""
        super().clean()
        
        # Validate dates
        if self.end_date and self.start_date > self.end_date:
            raise ValidationError({
                'end_date': "End date must be after start date"
            })
        
        # Validate time formats and relationships
        self._validate_times()
        
        # Validate run_days
        self._validate_run_days()
        
        # Validate frequency
        self._validate_frequency()

    def _validate_times(self):
        """Validate time formats and relationships"""
        valid_days = [0, 1, 2, 3, 4, 5, 6]
        
        for time_str in self.send_times:
            try:
                datetime.strptime(time_str, '%H:%M')
            except ValueError:
                raise ValidationError({
                    'send_times': f"Invalid time format: {time_str}. Use HH:MM (24-hour format)"
                })
        
        for time_str in self.end_times:
            try:
                datetime.strptime(time_str, '%H:%M')
            except ValueError:
                raise ValidationError({
                    'end_times': f"Invalid time format: {time_str}. Use HH:MM (24-hour format)"
                })
        
        # Validate send_times and end_times match
        if len(self.send_times) != len(self.end_times):
            raise ValidationError({
                'end_times': "send_times and end_times must have the same number of entries"
            })
        
        # Validate each send_time is before corresponding end_time
        for i, (send, end) in enumerate(zip(self.send_times, self.end_times)):
            if send >= end:
                raise ValidationError(
                    f"send_time {send} must be before end_time {end} at position {i}"
                )

    def _validate_run_days(self):
        """Validate run_days values"""
        valid_days = [0, 1, 2, 3, 4, 5, 6]  # Monday=0, Sunday=6
        for day in self.run_days:
            if not isinstance(day, int) or day not in valid_days:
                raise ValidationError({
                    'run_days': f"Invalid day: {day}. Use integers 0-6 (Monday=0, Sunday=6)"
                })

    def _validate_frequency(self):
        """Validate frequency value"""
        valid_frequencies = ['daily', 'weekly', 'monthly', 'once']
        if self.frequency not in valid_frequencies:
            raise ValidationError({
                'frequency': f"Frequency must be one of: {valid_frequencies}"
            })

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)


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


class Audience(models.Model):
    """Campaign audience/recipients model"""
    
    campaign = models.OneToOneField(
        Campaign, 
        on_delete=models.CASCADE, 
        related_name='audience'
    )
    recipients = models.JSONField()
    total_count = models.PositiveIntegerField(default=0)
    valid_count = models.PositiveIntegerField(default=0)
    invalid_count = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['total_count']),
        ]

    def __str__(self):
        return f"Audience for {self.campaign.name} ({self.total_count} recipients)"

    def clean(self):
        """Validate recipients data"""
        super().clean()
        
        if not isinstance(self.recipients, list):
            raise ValidationError({'recipients': "recipients must be a list"})
        
        # Validate recipient count
        if len(self.recipients) > 1000000:  # 1M limit
            raise ValidationError({
                'recipients': "Recipients list exceeds maximum limit of 1,000,000"
            })
        
        # Validate each recipient
        required_keys = {'msisdn', 'lang'}
        valid_count = 0
        invalid_count = 0
        
        for i, recipient in enumerate(self.recipients):
            if not isinstance(recipient, dict):
                invalid_count += 1
                continue
                
            provided_keys = set(recipient.keys())
            if provided_keys != required_keys:
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


class CampaignProgress(models.Model):
    """
    Tracks the high-level health of a campaign.
    This links the 'Planning' models to the 'Execution' models.
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
            
            # Auto-complete if all messages processed
            if completed >= self.total_messages:
                self.status = 'COMPLETED'
                self.completed_at = timezone.now()
            
            self.save()

    def increment_sent(self, count=1):
        """Increment sent count"""
        self.sent_count += count
        self.update_percentage()

    def increment_failed(self, count=1):
        """Increment failed count"""
        self.failed_count += count
        self.update_percentage()


class BatchStatus(models.Model):
    """
    Groups messages into chunks for efficiency.
    Tracks the status of each message batch.
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
    campaign_id_direct = models.IntegerField(db_index=True, null=True)  # For direct queries
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
        # Auto-set campaign_id_direct for direct queries
        if self.campaign_id and not self.campaign_id_direct:
            self.campaign_id_direct = self.campaign.campaign_id
        super().save(*args, **kwargs)

    def update_status(self):
        """Update batch status based on message counts"""
        if self.success_count + self.failed_count >= self.total_messages:
            self.status = 'COMPLETED'
            self.completed_at = timezone.now()
        self.save()


class MessageStatus(models.Model):
    """
    The high-volume receipt table.
    Uses campaign_id (not FK) to avoid heavy join overhead during high TPS writes.
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
    status = models.CharField(
        max_length=20, 
        choices=MESSAGE_STATUS_CHOICES,
        default='PENDING',
        db_index=True
    )
    attempts = models.PositiveSmallIntegerField(default=0)
    last_attempt = models.DateTimeField(null=True, blank=True)
    provider_response = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['campaign_id', 'status']),
            models.Index(fields=['batch_id']),
            models.Index(fields=['created_at']),  # For time-based queries
            models.Index(fields=['status', 'last_attempt']),  # For retry logic
            models.Index(fields=['phone_number', 'campaign_id']),  # For recipient lookup
        ]
        verbose_name_plural = "Message Statuses"

    def __str__(self):
        return f"Message {self.message_id} - {self.status}"

    def save(self, *args, **kwargs):
        is_new = not self.pk
        super().save(*args, **kwargs)
        
        # Log important status changes
        if is_new:
            logger.debug(f"New message created: {self.message_id}")
        elif self.status in ['FAILED', 'DELIVERED']:
            logger.info(f"Message {self.message_id} status: {self.status}")

    def can_retry(self, max_attempts=3):
        """Check if message can be retried"""
        return self.status == 'FAILED' and self.attempts < max_attempts


class Checkpoint(models.Model):
    """
    Prevents resending if the Ingestion Layer restarts.
    Tracks processing progress for large campaigns.
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


# ========== SIGNALS ==========

@receiver(post_save, sender=Campaign)
def create_campaign_related_models(sender, instance, created, **kwargs):
    """Create all related models when a campaign is created"""
    if created:
        # Create message content
        MessageContent.objects.get_or_create(campaign=instance)
        
        # Create progress tracker
        CampaignProgress.objects.get_or_create(campaign=instance)
        
        # Create checkpoint
        Checkpoint.objects.get_or_create(campaign=instance)
        
        logger.info(f"Created related models for campaign {instance.id}")


@receiver(post_save, sender=MessageStatus)
def update_batch_and_progress(sender, instance, created, **kwargs):
    """Update batch status and campaign progress when message status changes"""
    if not created and instance.tracker.has_changed('status'):
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


# Add model tracking for MessageStatus to detect status changes
from django.db.models import Model
from django.utils.functional import cached_property

class MessageStatus(models.Model):
    # ... (existing fields) ...
    
    @cached_property
    def tracker(self):
        return ModelTracker(self)


class ModelTracker:
    """Simple model field change tracker"""
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