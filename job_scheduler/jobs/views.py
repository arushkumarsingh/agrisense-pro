from django.shortcuts import render

from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.utils import timezone
from django.db.models import Q, Count, Avg
from datetime import timedelta
import logging

from .models import Job, JobRun, JobTemplate
from .serializers import (
    JobSerializer, JobListSerializer, JobCreateSerializer, JobUpdateSerializer,
    JobRunSerializer, JobTemplateSerializer, JobExecutionRequestSerializer,
    JobStatsSerializer, JobRunStatsSerializer
)
from .tasks import execute_job_async

logger = logging.getLogger(__name__)


class JobViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing jobs.
    Provides CRUD operations and additional actions for job management.
    """
    queryset = Job.objects.all()
    # permission_classes = [IsAuthenticated]  # Uncomment in production
    
    def get_serializer_class(self):
        """Return appropriate serializer based on action."""
        if self.action == 'list':
            return JobListSerializer
        elif self.action == 'create':
            return JobCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return JobUpdateSerializer
        return JobSerializer
    
    def get_queryset(self):
        """Filter queryset based on query parameters."""
        queryset = Job.objects.all()
        
        # Filter by job type
        job_type = self.request.query_params.get('job_type')
        if job_type:
            queryset = queryset.filter(job_type=job_type)
        
        # Filter by status
        status_filter = self.request.query_params.get('status')
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        # Filter by owner
        owner = self.request.query_params.get('owner')
        if owner:
            queryset = queryset.filter(owner=owner)
        
        # Search by name
        search = self.request.query_params.get('search')
        if search:
            queryset = queryset.filter(
                Q(name__icontains=search) | Q(description__icontains=search)
            )
        
        return queryset.select_related().prefetch_related('job_runs')
    
    @action(detail=True, methods=['post'])
    def run(self, request, pk=None):
        """
        Manually trigger a job execution.
        """
        job = self.get_object()
        
        if job.status != 'active':
            return Response(
                {'error': 'Cannot run inactive job'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        serializer = JobExecutionRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            # Create job run record
            job_run = JobRun.objects.create(
                job=job,
                status='queued',
                log_summary=f"Manual execution requested by user"
            )
            
            # Execute job asynchronously
            task = execute_job_async.delay(
                job_id=job.id,
                job_run_id=job_run.id,
                override_parameters=serializer.validated_data.get('override_parameters', {}),
                priority=serializer.validated_data.get('priority', 'normal')
            )
            
            job_run.celery_task_id = task.id
            job_run.save(update_fields=['celery_task_id'])
            
            logger.info(f"Job {job.name} queued for execution with task ID: {task.id}")
            
            return Response({
                'message': 'Job queued for execution',
                'job_run_id': job_run.id,
                'task_id': task.id
            }, status=status.HTTP_202_ACCEPTED)
            
        except Exception as e:
            logger.error(f"Failed to queue job {job.name}: {str(e)}")
            return Response(
                {'error': f'Failed to queue job: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=True, methods=['post'])
    def pause(self, request, pk=None):
        """Pause a job (set status to paused)."""
        job = self.get_object()
        job.status = 'paused'
        job.save(update_fields=['status'])
        
        logger.info(f"Job {job.name} paused")
        return Response({'message': 'Job paused successfully'})
    
    @action(detail=True, methods=['post'])
    def resume(self, request, pk=None):
        """Resume a paused job (set status to active)."""
        job = self.get_object()
        if job.status == 'paused':
            job.status = 'active'
            job.calculate_next_run()
            job.save(update_fields=['status', 'next_run_timestamp'])
            
            logger.info(f"Job {job.name} resumed")
            return Response({'message': 'Job resumed successfully'})
        else:
            return Response(
                {'error': 'Job is not paused'},
                status=status.HTTP_400_BAD_REQUEST
            )
    
    @action(detail=True, methods=['get'])
    def runs(self, request, pk=None):
        """Get run history for a specific job."""
        job = self.get_object()
        runs = JobRun.objects.filter(job=job).order_by('-start_time')
        
        # Pagination
        page_size = int(request.query_params.get('page_size', 20))
        page = int(request.query_params.get('page', 1))
        start = (page - 1) * page_size
        end = start + page_size
        
        paginated_runs = runs[start:end]
        serializer = JobRunSerializer(paginated_runs, many=True)
        
        return Response({
            'results': serializer.data,
            'total': runs.count(),
            'page': page,
            'page_size': page_size
        })
    
    @action(detail=False, methods=['get'])
    def stats(self, request):
        """Get overall job statistics."""
        total_jobs = Job.objects.count()
        active_jobs = Job.objects.filter(status='active').count()
        inactive_jobs = Job.objects.filter(status='inactive').count()
        
        # Jobs by type
        jobs_by_type = dict(Job.objects.values('job_type').annotate(count=Count('id')).values_list('job_type', 'count'))
        
        # Recent runs (last 24 hours)
        recent_runs = JobRun.objects.filter(
            start_time__gte=timezone.now() - timedelta(hours=24)
        ).count()
        
        # Success rate
        total_runs = JobRun.objects.count()
        successful_runs = JobRun.objects.filter(status='success').count()
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
        
        # Average execution time
        avg_execution_time = JobRun.objects.filter(
            status='success',
            execution_time_seconds__isnull=False
        ).aggregate(avg=Avg('execution_time_seconds'))['avg'] or 0
        
        stats_data = {
            'total_jobs': total_jobs,
            'active_jobs': active_jobs,
            'inactive_jobs': inactive_jobs,
            'jobs_by_type': jobs_by_type,
            'recent_runs': recent_runs,
            'success_rate': round(success_rate, 2),
            'average_execution_time': round(avg_execution_time, 2)
        }
        
        serializer = JobStatsSerializer(stats_data)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'])
    def due_jobs(self, request):
        """Get jobs that are due to run."""
        due_jobs = Job.objects.filter(
            status='active',
            next_run_timestamp__lte=timezone.now()
        ).order_by('next_run_timestamp')
        
        serializer = JobListSerializer(due_jobs, many=True)
        return Response(serializer.data)


class JobRunViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing job runs.
    Provides read-only access to job execution history.
    """
    queryset = JobRun.objects.all()
    serializer_class = JobRunSerializer
    # permission_classes = [IsAuthenticated]  # Uncomment in production
    
    def get_queryset(self):
        """Filter queryset based on query parameters."""
        queryset = JobRun.objects.select_related('job')
        
        # Filter by job
        job_id = self.request.query_params.get('job_id')
        if job_id:
            queryset = queryset.filter(job_id=job_id)
        
        # Filter by status
        status_filter = self.request.query_params.get('status')
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        # Filter by date range
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')
        
        if start_date:
            queryset = queryset.filter(start_time__gte=start_date)
        if end_date:
            queryset = queryset.filter(start_time__lte=end_date)
        
        return queryset.order_by('-start_time')
    
    @action(detail=False, methods=['get'])
    def stats(self, request):
        """Get job run statistics."""
        now = timezone.now()
        
        total_runs = JobRun.objects.count()
        successful_runs = JobRun.objects.filter(status='success').count()
        failed_runs = JobRun.objects.filter(status='failed').count()
        running_jobs = JobRun.objects.filter(status='running').count()
        queued_jobs = JobRun.objects.filter(status='queued').count()
        
        # Success rate
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
        
        # Average execution time
        avg_execution_time = JobRun.objects.filter(
            status='success',
            execution_time_seconds__isnull=False
        ).aggregate(avg=Avg('execution_time_seconds'))['avg'] or 0
        
        # Runs in last 24 hours and 7 days
        runs_last_24h = JobRun.objects.filter(
            start_time__gte=now - timedelta(hours=24)
        ).count()
        
        runs_last_7d = JobRun.objects.filter(
            start_time__gte=now - timedelta(days=7)
        ).count()
        
        stats_data = {
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'failed_runs': failed_runs,
            'running_jobs': running_jobs,
            'queued_jobs': queued_jobs,
            'success_rate': round(success_rate, 2),
            'average_execution_time': round(avg_execution_time, 2),
            'runs_last_24h': runs_last_24h,
            'runs_last_7d': runs_last_7d
        }
        
        serializer = JobRunStatsSerializer(stats_data)
        return Response(serializer.data)
    
    @action(detail=True, methods=['post'])
    def cancel(self, request, pk=None):
        """Cancel a running job."""
        job_run = self.get_object()
        
        if job_run.status not in ['queued', 'running']:
            return Response(
                {'error': 'Job run cannot be cancelled'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Cancel the Celery task if it exists
        if job_run.celery_task_id:
            from celery import current_app
            current_app.control.revoke(job_run.celery_task_id, terminate=True)
        
        job_run.status = 'cancelled'
        job_run.end_time = timezone.now()
        job_run.save(update_fields=['status', 'end_time'])
        
        logger.info(f"Job run {job_run.id} cancelled")
        return Response({'message': 'Job run cancelled successfully'})


class JobTemplateViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing job templates.
    """
    queryset = JobTemplate.objects.all()
    serializer_class = JobTemplateSerializer
    # permission_classes = [IsAuthenticated]  # Uncomment in production
    
    def get_queryset(self):
        """Filter queryset based on query parameters."""
        queryset = JobTemplate.objects.all()
        
        # Filter by job type
        job_type = self.request.query_params.get('job_type')
        if job_type:
            queryset = queryset.filter(job_type=job_type)
        
        # Filter by active status
        is_active = self.request.query_params.get('is_active')
        if is_active is not None:
            queryset = queryset.filter(is_active=is_active.lower() == 'true')
        
        return queryset
    
    @action(detail=True, methods=['post'])
    def create_job(self, request, pk=None):
        """Create a job from this template."""
        template = self.get_object()
        
        # Validate required fields
        job_name = request.data.get('name')
        if not job_name:
            return Response(
                {'error': 'Job name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Check if job name already exists
        if Job.objects.filter(name=job_name).exists():
            return Response(
                {'error': 'A job with this name already exists'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            job = template.create_job(
                name=job_name,
                schedule=request.data.get('schedule'),
                configuration=request.data.get('configuration_parameters')
            )
            
            serializer = JobSerializer(job)
            return Response(serializer.data, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Failed to create job from template {template.name}: {str(e)}")
            return Response(
                {'error': f'Failed to create job: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


def health_check(request):
    """Health check endpoint for the application."""
    from django.db import connection
    from django.core.cache import cache
    
    # Check database connection
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    # Check cache connection
    try:
        cache.set("health_check", "ok", 10)
        cache_status = "healthy"
    except Exception as e:
        cache_status = f"unhealthy: {str(e)}"
    
    # Check Celery connection
    try:
        from celery import current_app
        current_app.control.inspect().active()
        celery_status = "healthy"
    except Exception as e:
        celery_status = f"unhealthy: {str(e)}"
    
    overall_status = "healthy" if all(
        status == "healthy" for status in [db_status, cache_status, celery_status]
    ) else "unhealthy"
    
    return Response({
        'status': overall_status,
        'timestamp': timezone.now().isoformat(),
        'services': {
            'database': db_status,
            'cache': cache_status,
            'celery': celery_status
        }
    })


def dashboard_stats(request):
    """Get comprehensive dashboard statistics."""
    now = timezone.now()
    
    # Job statistics
    total_jobs = Job.objects.count()
    active_jobs = Job.objects.filter(status='active').count()
    due_jobs = Job.objects.filter(
        status='active',
        next_run_timestamp__lte=now
    ).count()
    
    # Run statistics
    total_runs = JobRun.objects.count()
    successful_runs = JobRun.objects.filter(status='success').count()
    failed_runs = JobRun.objects.filter(status='failed').count()
    running_runs = JobRun.objects.filter(status='running').count()
    
    # Success rate
    success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
    
    # Recent activity (last 24 hours)
    recent_runs_24h = JobRun.objects.filter(
        start_time__gte=now - timedelta(hours=24)
    ).count()
    
    # Average execution time
    avg_execution_time = JobRun.objects.filter(
        status='success',
        execution_time_seconds__isnull=False
    ).aggregate(avg=Avg('execution_time_seconds'))['avg'] or 0
    
    # Jobs by type
    jobs_by_type = dict(
        Job.objects.values('job_type').annotate(count=Count('id')).values_list('job_type', 'count')
    )
    
    return Response({
        'jobs': {
            'total': total_jobs,
            'active': active_jobs,
            'due': due_jobs,
            'by_type': jobs_by_type
        },
        'runs': {
            'total': total_runs,
            'successful': successful_runs,
            'failed': failed_runs,
            'running': running_runs,
            'success_rate': round(success_rate, 2),
            'recent_24h': recent_runs_24h,
            'avg_execution_time': round(avg_execution_time, 2)
        },
        'system': {
            'timestamp': now.isoformat(),
            'uptime': 'N/A'  # Could be implemented with process start time
        }
    })


def recent_activity(request):
    """Get recent job activity."""
    # Get recent job runs
    recent_runs = JobRun.objects.select_related('job').order_by('-start_time')[:20]
    
    # Get recent job creations/updates
    recent_jobs = Job.objects.order_by('-updated_at')[:10]
    
    # Get due jobs
    due_jobs = Job.objects.filter(
        status='active',
        next_run_timestamp__lte=timezone.now()
    ).order_by('next_run_timestamp')[:5]
    
    return Response({
        'recent_runs': JobRunSummarySerializer(recent_runs, many=True).data,
        'recent_jobs': JobListSerializer(recent_jobs, many=True).data,
        'due_jobs': JobListSerializer(due_jobs, many=True).data,
        'timestamp': timezone.now().isoformat()
    })