from rest_framework import serializers
from .models import Job, JobRun, JobTemplate
from croniter import croniter
from datetime import datetime


class JobRunSerializer(serializers.ModelSerializer):
    """Serializer for JobRun model."""
    
    duration = serializers.ReadOnlyField()
    
    class Meta:
        model = JobRun
        fields = '__all__'
        read_only_fields = ('job', 'start_time', 'end_time', 'execution_time_seconds')


class JobRunSummarySerializer(serializers.ModelSerializer):
    """Lightweight serializer for job run summaries."""
    
    duration = serializers.ReadOnlyField()
    
    class Meta:
        model = JobRun
        fields = ('id', 'start_time', 'end_time', 'status', 'duration', 'records_processed', 'records_failed')


class JobSerializer(serializers.ModelSerializer):
    """Serializer for Job model with full details."""
    
    last_run_status = serializers.ReadOnlyField()
    run_history = JobRunSummarySerializer(many=True, read_only=True)
    
    class Meta:
        model = Job
        fields = '__all__'
        read_only_fields = ('last_run_timestamp', 'next_run_timestamp', 'created_at', 'updated_at')
    
    def validate_schedule(self, value):
        """Validate schedule format."""
        predefined_schedules = ['daily', 'hourly', 'weekly']
        
        if value.lower() in predefined_schedules:
            return value.lower()
        
        # Validate cron expression
        try:
            croniter(value)
            return value
        except Exception as e:
            raise serializers.ValidationError(f"Invalid schedule format: {str(e)}")
    
    def validate_configuration_parameters(self, value):
        """Validate configuration parameters based on job type."""
        job_type = self.initial_data.get('job_type', 'data_pipeline')
        
        if job_type == 'data_pipeline':
            required_fields = ['raw_data_path']
            for field in required_fields:
                if field not in value:
                    raise serializers.ValidationError(f"Missing required field for data_pipeline: {field}")
        
        return value
    
    def create(self, validated_data):
        """Create job with proper next_run_timestamp calculation."""
        job = Job(**validated_data)
        job.calculate_next_run()
        job.save()
        return job


class JobListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for job lists."""
    
    last_run_status = serializers.ReadOnlyField()
    
    class Meta:
        model = Job
        fields = (
            'id', 'name', 'job_type', 'schedule', 'status',
            'last_run_timestamp', 'next_run_timestamp',
            'last_run_status', 'created_at'
        )


class JobCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating new jobs."""
    
    class Meta:
        model = Job
        fields = (
            'name', 'job_type', 'schedule', 'status',
            'configuration_parameters', 'description', 'owner'
        )
    
    def validate_name(self, value):
        """Ensure job name is unique."""
        if Job.objects.filter(name=value).exists():
            raise serializers.ValidationError("A job with this name already exists.")
        return value
    
    def validate_schedule(self, value):
        """Validate schedule format."""
        predefined_schedules = ['daily', 'hourly', 'weekly']
        
        if value.lower() in predefined_schedules:
            return value.lower()
        
        # Validate cron expression
        try:
            croniter(value)
            return value
        except Exception as e:
            raise serializers.ValidationError(f"Invalid schedule format: {str(e)}")


class JobUpdateSerializer(serializers.ModelSerializer):
    """Serializer for updating existing jobs."""
    
    class Meta:
        model = Job
        fields = (
            'name', 'job_type', 'schedule', 'status',
            'configuration_parameters', 'description', 'owner'
        )
    
    def validate_name(self, value):
        """Ensure job name is unique (excluding current instance)."""
        if self.instance and self.instance.name == value:
            return value
        if Job.objects.filter(name=value).exists():
            raise serializers.ValidationError("A job with this name already exists.")
        return value
    
    def validate_schedule(self, value):
        """Validate schedule format."""
        predefined_schedules = ['daily', 'hourly', 'weekly']
        
        if value.lower() in predefined_schedules:
            return value.lower()
        
        # Validate cron expression
        try:
            croniter(value)
            return value
        except Exception as e:
            raise serializers.ValidationError(f"Invalid schedule format: {str(e)}")
    
    def update(self, instance, validated_data):
        """Update job and recalculate next_run_timestamp if schedule changed."""
        if 'schedule' in validated_data and validated_data['schedule'] != instance.schedule:
            instance.schedule = validated_data['schedule']
            instance.calculate_next_run()
            validated_data.pop('schedule')  # Remove from validated_data to avoid double-setting
        
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        
        instance.save()
        return instance


class JobTemplateSerializer(serializers.ModelSerializer):
    """Serializer for JobTemplate model."""
    
    class Meta:
        model = JobTemplate
        fields = '__all__'
        read_only_fields = ('created_at', 'updated_at')


class JobExecutionRequestSerializer(serializers.Serializer):
    """Serializer for manual job execution requests."""
    
    override_parameters = serializers.JSONField(required=False, default=dict)
    priority = serializers.ChoiceField(
        choices=[('low', 'Low'), ('normal', 'Normal'), ('high', 'High')],
        default='normal'
    )
    
    def validate_override_parameters(self, value):
        """Validate override parameters."""
        if not isinstance(value, dict):
            raise serializers.ValidationError("Override parameters must be a JSON object.")
        return value


class JobStatsSerializer(serializers.Serializer):
    """Serializer for job statistics."""
    
    total_jobs = serializers.IntegerField()
    active_jobs = serializers.IntegerField()
    inactive_jobs = serializers.IntegerField()
    jobs_by_type = serializers.DictField()
    recent_runs = serializers.IntegerField()
    success_rate = serializers.FloatField()
    average_execution_time = serializers.FloatField()
    
    
class JobRunStatsSerializer(serializers.Serializer):
    """Serializer for job run statistics."""
    
    total_runs = serializers.IntegerField()
    successful_runs = serializers.IntegerField()
    failed_runs = serializers.IntegerField()
    running_jobs = serializers.IntegerField()
    queued_jobs = serializers.IntegerField()
    success_rate = serializers.FloatField()
    average_execution_time = serializers.FloatField()
    runs_last_24h = serializers.IntegerField()
    runs_last_7d = serializers.IntegerField()