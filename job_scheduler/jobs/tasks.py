from celery import shared_task
from django.utils import timezone
from django.conf import settings
import logging
import json
import subprocess
import os
from typing import Dict, Any

from .models import Job, JobRun

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def execute_job_async(self, job_id: int, job_run_id: int, override_parameters: Dict[str, Any] = None, priority: str = 'normal'):
    """
    Execute a job asynchronously.
    
    Args:
        job_id: ID of the job to execute
        job_run_id: ID of the job run record
        override_parameters: Optional parameters to override job configuration
        priority: Execution priority (low, normal, high)
    """
    try:
        # Get job and job run instances
        job = Job.objects.get(id=job_id)
        job_run = JobRun.objects.get(id=job_run_id)
        
        logger.info(f"Starting execution of job: {job.name} (ID: {job_id})")
        
        # Mark job run as running
        job_run.mark_as_running()
        
        # Prepare execution parameters
        execution_params = job.configuration_parameters.copy()
        if override_parameters:
            execution_params.update(override_parameters)
        
        # Execute based on job type
        if job.job_type == 'data_pipeline':
            result = execute_data_pipeline(job, job_run, execution_params)
        elif job.job_type == 'email_notification':
            result = execute_email_notification(job, job_run, execution_params)
        elif job.job_type == 'system_maintenance':
            result = execute_system_maintenance(job, job_run, execution_params)
        else:
            raise ValueError(f"Unknown job type: {job.job_type}")
        
        # Mark job run as completed
        job_run.mark_as_completed(
            success=result['success'],
            error_message=result.get('error_message', ''),
            execution_details=result.get('details', {})
        )
        
        # Update job timestamps
        job.update_after_run(success=result['success'])
        
        logger.info(f"Job {job.name} completed successfully")
        return result
        
    except Job.DoesNotExist:
        error_msg = f"Job with ID {job_id} not found"
        logger.error(error_msg)
        return {'success': False, 'error_message': error_msg}
        
    except JobRun.DoesNotExist:
        error_msg = f"JobRun with ID {job_run_id} not found"
        logger.error(error_msg)
        return {'success': False, 'error_message': error_msg}
        
    except Exception as exc:
        error_msg = f"Job execution failed: {str(exc)}"
        logger.error(error_msg)
        
        # Mark job run as failed if it exists
        try:
            job_run = JobRun.objects.get(id=job_run_id)
            job_run.mark_as_completed(success=False, error_message=error_msg)
        except JobRun.DoesNotExist:
            pass
        
        # Retry the task if retries are available
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying job {job_id} (attempt {self.request.retries + 1})")
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        
        return {'success': False, 'error_message': error_msg}


def execute_data_pipeline(job: Job, job_run: JobRun, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute data pipeline job.
    
    Args:
        job: Job instance
        job_run: JobRun instance
        params: Execution parameters
        
    Returns:
        Dictionary with execution results
    """
    try:
        logger.info(f"Executing data pipeline for job: {job.name}")
        
        # Prepare command to run data pipeline
        pipeline_cmd = [
            'python', '/app/data_pipeline/main.py',
            '--job-id', str(job.id),
            '--job-run-id', str(job_run.id),
            '--config', json.dumps(params)
        ]
        
        # Set environment variables
        env = os.environ.copy()
        env['PYTHONPATH'] = '/app/data_pipeline'
        
        # Execute the pipeline
        result = subprocess.run(
            pipeline_cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
            env=env
        )
        
        if result.returncode == 0:
            # Parse output for execution details
            execution_details = {
                'stdout': result.stdout,
                'stderr': result.stderr,
                'return_code': result.returncode
            }
            
            # Try to parse pipeline statistics from stdout
            try:
                # Look for JSON output in the last lines
                lines = result.stdout.strip().split('\n')
                for line in reversed(lines):
                    if line.startswith('{') and line.endswith('}'):
                        stats = json.loads(line)
                        execution_details.update(stats)
                        
                        # Update job run with statistics
                        if 'records_processed' in stats:
                            job_run.records_processed = stats['records_processed']
                        if 'records_failed' in stats:
                            job_run.records_failed = stats['records_failed']
                        job_run.save(update_fields=['records_processed', 'records_failed'])
                        break
            except (json.JSONDecodeError, KeyError):
                pass
            
            job_run.log_summary = f"Pipeline executed successfully. Processed files from {params.get('raw_data_path', 'default path')}"
            job_run.save(update_fields=['log_summary'])
            
            return {
                'success': True,
                'details': execution_details,
                'message': 'Data pipeline executed successfully'
            }
        else:
            error_message = f"Pipeline failed with return code {result.returncode}: {result.stderr}"
            return {
                'success': False,
                'error_message': error_message,
                'details': {
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'return_code': result.returncode
                }
            }
            
    except subprocess.TimeoutExpired:
        error_message = "Pipeline execution timed out"
        logger.error(error_message)
        return {
            'success': False,
            'error_message': error_message
        }
    except Exception as e:
        error_message = f"Failed to execute data pipeline: {str(e)}"
        logger.error(error_message)
        return {
            'success': False,
            'error_message': error_message
        }


def execute_email_notification(job: Job, job_run: JobRun, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute email notification job (placeholder implementation).
    
    Args:
        job: Job instance
        job_run: JobRun instance
        params: Execution parameters
        
    Returns:
        Dictionary with execution results
    """
    try:
        logger.info(f"Executing email notification for job: {job.name}")
        
        # Placeholder implementation
        recipient = params.get('recipient', 'admin@agrisense.com')
        subject = params.get('subject', f'AgriSense Pro Notification - {job.name}')
        message = params.get('message', 'This is a test notification from AgriSense Pro.')
        
        # Simulate email sending
        logger.info(f"Sending email to {recipient} with subject: {subject}")
        
        job_run.log_summary = f"Email notification sent to {recipient}"
        job_run.save(update_fields=['log_summary'])
        
        return {
            'success': True,
            'details': {
                'recipient': recipient,
                'subject': subject,
                'message': message
            },
            'message': 'Email notification sent successfully'
        }
        
    except Exception as e:
        error_message = f"Failed to send email notification: {str(e)}"
        logger.error(error_message)
        return {
            'success': False,
            'error_message': error_message
        }


def execute_system_maintenance(job: Job, job_run: JobRun, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute system maintenance job (placeholder implementation).
    
    Args:
        job: Job instance
        job_run: JobRun instance
        params: Execution parameters
        
    Returns:
        Dictionary with execution results
    """
    try:
        logger.info(f"Executing system maintenance for job: {job.name}")
        
        # Placeholder implementation
        maintenance_type = params.get('type', 'cleanup')
        target = params.get('target', 'logs')
        
        logger.info(f"Performing {maintenance_type} on {target}")
        
        # Simulate maintenance task
        if maintenance_type == 'cleanup':
            if target == 'logs':
                # Simulate log cleanup
                logger.info("Cleaning up old log files")
            elif target == 'temp_files':
                # Simulate temp file cleanup
                logger.info("Cleaning up temporary files")
        
        job_run.log_summary = f"System maintenance completed: {maintenance_type} on {target}"
        job_run.save(update_fields=['log_summary'])
        
        return {
            'success': True,
            'details': {
                'maintenance_type': maintenance_type,
                'target': target
            },
            'message': 'System maintenance completed successfully'
        }
        
    except Exception as e:
        error_message = f"Failed to execute system maintenance: {str(e)}"
        logger.error(error_message)
        return {
            'success': False,
            'error_message': error_message
        }


@shared_task
def check_and_schedule_due_jobs():
    """
    Check for due jobs and schedule them for execution.
    This task should be run periodically by Celery Beat.
    """
    try:
        logger.info("Checking for due jobs...")
        
        # Get all due jobs
        due_jobs = Job.objects.filter(
            status='active',
            next_run_timestamp__lte=timezone.now()
        ).order_by('next_run_timestamp')
        
        scheduled_count = 0
        for job in due_jobs:
            try:
                # Create job run record
                job_run = JobRun.objects.create(
                    job=job,
                    status='queued',
                    log_summary=f"Scheduled execution at {timezone.now()}"
                )
                
                # Schedule job execution
                task = execute_job_async.delay(
                    job_id=job.id,
                    job_run_id=job_run.id
                )
                
                job_run.celery_task_id = task.id
                job_run.save(update_fields=['celery_task_id'])
                
                scheduled_count += 1
                logger.info(f"Scheduled job: {job.name} (task ID: {task.id})")
                
            except Exception as e:
                logger.error(f"Failed to schedule job {job.name}: {str(e)}")
        
        logger.info(f"Scheduled {scheduled_count} jobs for execution")
        return {
            'scheduled_jobs': scheduled_count,
            'message': f'Successfully scheduled {scheduled_count} jobs'
        }
        
    except Exception as e:
        error_message = f"Failed to check and schedule due jobs: {str(e)}"
        logger.error(error_message)
        return {
            'scheduled_jobs': 0,
            'error_message': error_message
        }


@shared_task
def cleanup_old_job_runs():
    """
    Clean up old job run records to prevent database bloat.
    Keeps job runs from the last 30 days.
    """
    try:
        logger.info("Cleaning up old job runs...")
        
        cutoff_date = timezone.now() - timezone.timedelta(days=30)
        deleted_count = JobRun.objects.filter(
            start_time__lt=cutoff_date
        ).delete()[0]
        
        logger.info(f"Deleted {deleted_count} old job run records")
        return {
            'deleted_count': deleted_count,
            'message': f'Successfully deleted {deleted_count} old job run records'
        }
        
    except Exception as e:
        error_message = f"Failed to cleanup old job runs: {str(e)}"
        logger.error(error_message)
        return {
            'deleted_count': 0,
            'error_message': error_message
        }