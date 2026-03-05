from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver


SUPPORTED_LANGUAGES = ['en', 'am', 'ti', 'om', 'so']


def _default_message_content():
    """Return a dict with all supported languages set to an empty string.
    Used as the default factory for the JSONField so that new
    :class:`MessageContent` objects already contain the five keys.
    """
    return {lang: "" for lang in SUPPORTED_LANGUAGES}


class Campaign(models.Model):
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, default='draft')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} (ID: {self.id})"

    class Meta:
        ordering = ['-created_at']


class Schedule(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),   # Created, not yet started
        ('running', 'Running'),   # Inside 8:00 AM - 10:00 PM window
        ('stop', 'Stop'),         # Outside window OR manually paused
        ('completed', 'Completed'), # End date/time reached
    ]

    campaign = models.OneToOneField(Campaign, on_delete=models.CASCADE, related_name='schedule')
    start_date = models.DateField()
    end_date = models.DateField(null=True, blank=True)
    frequency = models.CharField(max_length=20) 
    run_days = models.JSONField(default=list)      
    send_times = models.JSONField(default=list)    # e.g., ["08:00"]
    end_times = models.JSONField(default=list)     # e.g., ["22:00"]
    is_active = models.BooleanField(default=True)
    status = models.CharField(
        max_length=20, 
        choices=STATUS_CHOICES, 
        default='pending'
    )

    def __str__(self):
        return f"Schedule for {self.campaign.name} ({self.status})"


class MessageContent(models.Model):
    campaign = models.OneToOneField(Campaign, on_delete=models.CASCADE, related_name='message_content')
    # when a MessageContent row is created we want the five languages present
    content = models.JSONField(default=_default_message_content)
    default_language = models.CharField(max_length=10, default='en')

    def __str__(self):
        return f"Message Content for {self.campaign.name}"

    def clean(self):
        """Ensure the JSON has exactly the supported language keys."""
        super().clean()
        if set(self.content.keys()) != set(SUPPORTED_LANGUAGES):
            raise ValueError(f"content must have keys {SUPPORTED_LANGUAGES}")

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)


@receiver(post_save, sender=Campaign)
def _create_initial_message_content(sender, instance, created, **kwargs):
    if created:
        # use get_or_create to avoid "already exists" errors when serializer
        # has also created it.  the default factory populates the five languages.
        MessageContent.objects.get_or_create(campaign=instance)


class Audience(models.Model):
    campaign = models.OneToOneField(Campaign, on_delete=models.CASCADE, related_name='audience')
    recipients = models.JSONField()

    def __str__(self):
        count = len(self.recipients) if self.recipients else 0
        return f"Audience for {self.campaign.name} ({count} recipients)"


class CampaignProgress(models.Model):
    """
    L3-09: Tracks the high-level health of a campaign.
    This links the 'Planning' models to the 'Execution' models.
    """
    STATUS_CHOICES = [
        ('ACTIVE', 'Active'),
        ('COMPLETED', 'Completed'),
        ('STOPPED', 'Stopped'),
        ('FAILED', 'Failed'),
    ]

    # OneToOne ensures 1 Campaign has exactly 1 Progress tracker
    campaign = models.OneToOneField(
        Campaign, 
        on_delete=models.CASCADE, 
        primary_key=True,
        related_name='progress'
    )
    total_messages = models.IntegerField(default=0)
    sent_count = models.PositiveIntegerField(default=0)
    failed_count = models.PositiveIntegerField(default=0)
    pending_count = models.PositiveIntegerField(default=0)
    
    # Decimal for precise percentage (e.g., 99.99%)
    progress_percent = models.DecimalField(max_digits=5, decimal_places=2, default=0.00)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='ACTIVE')
    
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Progress for {self.campaign.name}: {self.progress_percent}%"

    def update_percentage(self):
        if self.total_messages > 0:
            self.progress_percent = (self.sent_count + self.failed_count) / self.total_messages * 100
            self.save()


class BatchStatus(models.Model):
    """
    L3-08: Groups messages into chunks of 1000 for efficiency.
    We use UUID or CharField for batch_id to allow Layer 3 to generate it.
    """
    batch_id = models.CharField(max_length=50, primary_key=True)
    campaign = models.ForeignKey(CampaignProgress, on_delete=models.CASCADE, related_name='batches')
    total_messages = models.IntegerField(default=0)
    success_count = models.PositiveIntegerField(default=0)
    failed_count = models.PositiveIntegerField(default=0)
    status = models.CharField(max_length=20, default='IN_PROGRESS') 
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"Batch {self.batch_id} - {self.status}"


class MessageStatus(models.Model):
    """
    L4-12: The high-volume receipt table.
    We use campaign_id (not FK) to avoid heavy join overhead during 1000 TPS writes.
    """
    message_id = models.CharField(max_length=100, primary_key=True)
    campaign_id = models.IntegerField(db_index=True) 
    batch_id = models.CharField(max_length=50, db_index=True)
    phone_number = models.CharField(max_length=20, db_index=True)
    status = models.CharField(max_length=20, default='PENDING') 
    attempts = models.PositiveSmallIntegerField(default=0)
    last_attempt = models.DateTimeField(null=True)
    provider_response = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Message {self.message_id} - {self.status}"

    class Meta:
        # Crucial: Allows the DB to filter "Which numbers failed for Campaign X?" instantly
        indexes = [
            models.Index(fields=['campaign_id', 'status']),
            models.Index(fields=['batch_id']),
        ]
        verbose_name_plural = "Message Statuses"


class Checkpoint(models.Model):
    """
    L2-11: Prevents resending if the Ingestion Layer (Layer 2) restarts.
    """
    campaign = models.OneToOneField(Campaign, on_delete=models.CASCADE, primary_key=True)
    last_processed_index = models.PositiveIntegerField(default=0) # Index in the 3M list
    status = models.CharField(max_length=20, default='RUNNING')
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Checkpoint for {self.campaign.name} - Index: {self.last_processed_index}"