"""
Admin configuration for jobs app.
"""
from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from .models import Job, JobRun, JobTemplate


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = [
        'name', 'job_type', 'status', 'schedule', 
        'last_run_timestamp', 'next_run_timestamp', 'owner'
    ]
    list_filter = ['job_type', 'status', 'created_at']
    search_fields = ['name', 'description', 'owner']
    readonly_fields = ['created_at', 'updated_at', 'last_run_timestamp', 'next_run_timestamp']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'job_type', 'description', 'owner')
        }),
        ('Schedule Configuration', {
            'fields': ('schedule', 'status', 'next_run_timestamp')
        }),
        ('Execution History', {
            'fields': ('last_run_timestamp', 'last_run_status'),
            'classes': ('collapse',)
        }),
        ('Configuration', {
            'fields': ('configuration_parameters',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def last_run_status(self, obj):
        """Display last run status with color coding."""
        status = obj.last_run_status
        if status == 'success':
            return format_html('<span style="color: green;">✓ {}</span>', status)
        elif status == 'failed':
            return format_html('<span style="color: red;">✗ {}</span>', status)
        elif status == 'running':
            return format_html('<span style="color: orange;">⟳ {}</span>', status)
        else:
            return format_html('<span style="color: gray;">- {}</span>', status)
    
    last_run_status.short_description = 'Last Run Status'


@admin.register(JobRun)
class JobRunAdmin(admin.ModelAdmin):
    list_display = [
        'job_name', 'status', 'start_time', 'end_time', 
        'duration_display', 'records_processed', 'records_failed'
    ]
    list_filter = ['status', 'start_time', 'job__job_type']
    search_fields = ['job__name', 'log_summary', 'error_message']
    readonly_fields = ['start_time', 'end_time', 'execution_time_seconds', 'duration']
    
    fieldsets = (
        ('Job Information', {
            'fields': ('job', 'celery_task_id')
        }),
        ('Execution Details', {
            'fields': ('status', 'start_time', 'end_time', 'duration')
        }),
        ('Performance Metrics', {
            'fields': ('records_processed', 'records_failed', 'execution_time_seconds'),
            'classes': ('collapse',)
        }),
        ('Logs and Errors', {
            'fields': ('log_summary', 'error_message'),
            'classes': ('collapse',)
        }),
        ('Execution Details', {
            'fields': ('execution_details',),
            'classes': ('collapse',)
        }),
    )
    
    def job_name(self, obj):
        """Display job name with link."""
        if obj.job:
            url = reverse('admin:jobs_job_change', args=[obj.job.id])
            return format_html('<a href="{}">{}</a>', url, obj.job.name)
        return '-'
    job_name.short_description = 'Job'
    
    def duration_display(self, obj):
        """Display duration in a readable format."""
        if obj.duration:
            if obj.duration < 60:
                return f"{obj.duration:.1f}s"
            elif obj.duration < 3600:
                return f"{obj.duration/60:.1f}m"
            else:
                return f"{obj.duration/3600:.1f}h"
        return '-'
    duration_display.short_description = 'Duration'


@admin.register(JobTemplate)
class JobTemplateAdmin(admin.ModelAdmin):
    list_display = ['name', 'job_type', 'default_schedule', 'is_active', 'created_at']
    list_filter = ['job_type', 'is_active', 'created_at']
    search_fields = ['name', 'description']
    readonly_fields = ['created_at', 'updated_at']
    
    fieldsets = (
        ('Template Information', {
            'fields': ('name', 'job_type', 'description', 'is_active')
        }),
        ('Default Configuration', {
            'fields': ('default_schedule', 'default_configuration'),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
