from django.db import models
from django.utils import timezone
from django.core.validators import RegexValidator
import json
from croniter import croniter
from datetime import datetime, timedelta


class Job(models.Model):
    """
    Model representing a scheduled job in the AgriSense Pro system.
    """
    JOB_TYPES = [
        ('data_pipeline', 'Data Pipeline'),
        ('email_notification', 'Email Notification'),
        ('system_maintenance', 'System Maintenance'),
    ]
    
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('paused', 'Paused'),
    ]
    
    # Core fields
    name = models.CharField(max_length=255, unique=True)
    job_type = models.CharField(max_length=50, choices=JOB_TYPES, default='data_pipeline')
    
    # Schedule configuration
    schedule = models.CharField(
        max_length=100,
        help_text="Cron expression or predefined interval (daily, hourly, weekly)"
    )
    
    # Timestamps
    last_run_timestamp = models.DateTimeField(null=True, blank=True)
    next_run_timestamp = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Status and configuration
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    configuration_parameters = models.JSONField(default=dict, blank=True)
    
    # Optional fields
    description = models.TextField(blank=True)
    owner = models.CharField(max_length=100, blank=True)
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status', 'next_run_timestamp']),
            models.Index(fields=['job_type']),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.job_type})"
    
    def save(self, *args, **kwargs):
        """Override save to calculate next_run_timestamp on creation/update."""
        if not self.next_run_timestamp or 'schedule' in kwargs.get('update_fields', []):
            self.calculate_next_run()
        super().save(*args, **kwargs)
    
    def calculate_next_run(self):
        """Calculate the next run timestamp based on the schedule."""
        now = timezone.now()
        
        # Handle predefined intervals
        if self.schedule.lower() == 'daily':
            self.next_run_timestamp = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if self.next_run_timestamp <= now:
                self.next_run_timestamp += timedelta(days=1)
        elif self.schedule.lower() == 'hourly':
            self.next_run_timestamp = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        elif self.schedule.lower() == 'weekly':
            days_ahead = 0 - now.weekday()  # Monday is 0
            if days_ahead <= 0:
                days_ahead += 7
            self.next_run_timestamp = (now + timedelta(days=days_ahead)).replace(hour=2, minute=0, second=0, microsecond=0)
        else:
            # Handle cron expressions
            try:
                cron = croniter(self.schedule, now)
                self.next_run_timestamp = cron.get_next(datetime)
            except Exception as e:
                # Fallback to daily if cron parsing fails
                self.next_run_timestamp = now.replace(hour=2, minute=0, second=0, microsecond=0) + timedelta(days=1)
    
    def is_due(self):
        """Check if the job is due to run."""
        if not self.next_run_timestamp or self.status != 'active':
            return False
        return timezone.now() >= self.next_run_timestamp
    
    def update_after_run(self, success=True):
        """Update timestamps after job execution."""
        self.last_run_timestamp = timezone.now()
        self.calculate_next_run()
        self.save(update_fields=['last_run_timestamp', 'next_run_timestamp'])
    
    @property
    def last_run_status(self):
        """Get the status of the last job run."""
        last_run = self.job_runs.order_by('-start_time').first()
        return last_run.status if last_run else 'never_run'
    
    @property
    def run_history(self):
        """Get recent run history (last 10 runs)."""
        return self.job_runs.order_by('-start_time')[:10]


class JobRun(models.Model):
    """
    Model representing an individual execution of a job.
    """
    RUN_STATUS_CHOICES = [
        ('queued', 'Queued'),
        ('running', 'Running'),
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('timeout', 'Timeout'),
        ('cancelled', 'Cancelled'),
    ]
    
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='job_runs')
    
    # Execution details
    start_time = models.DateTimeField(auto_now_add=True)
    end_time = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=RUN_STATUS_CHOICES, default='queued')
    
    # Execution metadata
    log_summary = models.TextField(blank=True)
    error_message = models.TextField(blank=True)
    execution_details = models.JSONField(default=dict, blank=True)
    
    # Performance metrics
    records_processed = models.IntegerField(null=True, blank=True)
    records_failed = models.IntegerField(null=True, blank=True)
    execution_time_seconds = models.FloatField(null=True, blank=True)
    
    # Celery task tracking
    celery_task_id = models.CharField(max_length=255, blank=True)
    
    class Meta:
        ordering = ['-start_time']
        indexes = [
            models.Index(fields=['job', '-start_time']),
            models.Index(fields=['status']),
        ]
    
    def __str__(self):
        return f"{self.job.name} - {self.start_time.strftime('%Y-%m-%d %H:%M:%S')} ({self.status})"
    
    @property
    def duration(self):
        """Calculate execution duration."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def mark_as_running(self):
        """Mark the job run as running."""
        self.status = 'running'
        self.save(update_fields=['status'])
    
    def mark_as_completed(self, success=True, error_message='', execution_details=None):
        """Mark the job run as completed."""
        self.end_time = timezone.now()
        self.status = 'success' if success else 'failed'
        if error_message:
            self.error_message = error_message
        if execution_details:
            self.execution_details = execution_details
        self.execution_time_seconds = self.duration
        self.save(update_fields=['end_time', 'status', 'error_message', 'execution_details', 'execution_time_seconds'])


class JobTemplate(models.Model):
    """
    Model for job templates to facilitate easy job creation.
    """
    name = models.CharField(max_length=255, unique=True)
    job_type = models.CharField(max_length=50, choices=Job.JOB_TYPES)
    default_schedule = models.CharField(max_length=100)
    default_configuration = models.JSONField(default=dict)
    description = models.TextField()
    is_active = models.BooleanField(default=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"Template: {self.name}"
    
    def create_job(self, name, schedule=None, configuration=None):
        """Create a job from this template."""
        job_data = {
            'name': name,
            'job_type': self.job_type,
            'schedule': schedule or self.default_schedule,
            'configuration_parameters': {**self.default_configuration, **(configuration or {})},
            'description': f"Created from template: {self.name}",
        }
        return Job.objects.create(**job_data)