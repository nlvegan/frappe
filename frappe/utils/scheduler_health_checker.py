"""
Scheduler Health Checker
=======================

Component for detecting stuck jobs in the scheduler.
Separated for maintainability and focused responsibility.
"""

from datetime import timedelta
from typing import Dict, List, Optional
import time

import frappe
from frappe.utils import now_datetime, get_datetime, flt
from rq.job import Job

from frappe.utils.scheduler_monitor_types import StuckJobAlert, HealthCheckResult, SecurityError, MonitoringError


class SchedulerHealthChecker:
    """
    Component for detecting stuck jobs and scheduler health issues.
    
    Responsibility: Analyze job states and identify problems.
    """
    
    def __init__(self, site: str):
        self.site = site
        self._validate_site_access()
        
    def _validate_site_access(self):
        """Validate access to site for security isolation"""
        if not self.site:
            raise SecurityError("Site context required for health checking")
        
        # Validate site exists and is accessible
        if frappe.local.site and frappe.local.site != self.site:
            raise SecurityError(f"Cross-site access denied: current={frappe.local.site}, requested={self.site}")
        
    def check_job_health(self, running_jobs: List[Job], config: Dict) -> HealthCheckResult:
        """
        Analyze running jobs to identify stuck ones.
        
        Args:
            running_jobs: List of currently running jobs
            config: Monitoring configuration
            
        Returns:
            HealthCheckResult with healthy and stuck jobs
        """
        try:
            check_start = time.perf_counter()
            
            stuck_jobs = []
            healthy_jobs = []
            errors = []
            
            # Validate inputs
            if not isinstance(running_jobs, list):
                raise MonitoringError(f"Expected list of jobs, got {type(running_jobs)}")
            
            if not config:
                raise MonitoringError("Configuration required for health checking")
            
            for job in running_jobs:
                try:
                    # Validate job site matches our site for security
                    self._validate_job_site_access(job)
                    
                    if self._is_job_stuck(job, config):
                        alert = self._create_stuck_job_alert(job, config)
                        stuck_jobs.append(alert)
                    else:
                        healthy_jobs.append(job)
                        
                except Exception as e:
                    error_msg = f"Error analyzing job {getattr(job, 'id', 'unknown')}: {str(e)}"
                    frappe.log_error(error_msg, "SchedulerHealthChecker")
                    errors.append(error_msg)
            
            check_duration = (time.perf_counter() - check_start) * 1000.0
            
            return HealthCheckResult(
                total_jobs_checked=len(running_jobs),
                healthy_jobs=[job.__dict__ if hasattr(job, '__dict__') else str(job) for job in healthy_jobs],
                stuck_jobs=stuck_jobs,
                check_duration_ms=check_duration,
                errors=errors
            )
            
        except Exception as e:
            frappe.log_error(f"Health check failed: {str(e)}", "SchedulerHealthChecker")
            return HealthCheckResult(
                total_jobs_checked=len(running_jobs) if isinstance(running_jobs, list) else 0,
                healthy_jobs=[],
                stuck_jobs=[],
                check_duration_ms=0.0,
                errors=[f"Health check failed: {str(e)}"]
            )
    
    def _validate_job_site_access(self, job: Job):
        """Validate job belongs to current site for security isolation"""
        if not job:
            return
            
        # Check job metadata for site information
        job_site = None
        if hasattr(job, 'meta') and job.meta:
            job_site = job.meta.get('site')
        
        # If job has site info and it doesn't match, deny access
        if job_site and job_site != self.site:
            raise SecurityError(f"Cross-site job access denied: job_site={job_site}, current_site={self.site}")
    
    def _is_job_stuck(self, job: Job, config: Dict) -> bool:
        """Determine if a job appears to be stuck with validation"""
        try:
            if not job or not hasattr(job, 'started_at') or not job.started_at:
                return False
                
            # Calculate runtime with error handling
            try:
                runtime_minutes = (now_datetime() - get_datetime(job.started_at)).total_seconds() / 60
            except Exception as e:
                frappe.log_error(f"Error calculating job runtime: {str(e)}", "SchedulerHealthChecker")
                return False
            
            if runtime_minutes < 0:
                # Job in future - clock sync issues
                frappe.log_error(f"Job started_at in future: {job.started_at}", "SchedulerHealthChecker")
                return False
            
            # Get configured timeout for this job type
            job_name = self._get_safe_job_name(job)
            job_timeout_minutes = self._get_job_timeout_minutes(job_name, config, job)
            
            # Primary check: exceeded configured timeout
            if runtime_minutes > job_timeout_minutes:
                return True
                
            # Secondary check: exceeded global maximum runtime
            global_max_hours = config.get('maximum_job_runtime_hours', 2.0)  # Default 2 hours
            global_max_minutes = flt(global_max_hours) * 60
            if runtime_minutes > global_max_minutes:
                return True
                
            return False
            
        except Exception as e:
            frappe.log_error(f"Error checking if job stuck: {str(e)}", "SchedulerHealthChecker")
            return False  # Fail safe - don't consider job stuck if we can't determine
    
    def _get_safe_job_name(self, job: Job) -> str:
        """Safely extract job name with fallbacks"""
        try:
            if hasattr(job, 'func_name') and job.func_name:
                return str(job.func_name)
            elif hasattr(job, 'description') and job.description:
                return str(job.description)
            elif hasattr(job, 'id') and job.id:
                return f"job_{job.id}"
            else:
                return "unknown_job"
        except Exception:
            return "unknown_job"
    
    def _get_job_timeout_minutes(self, job_name: str, config: Dict, job: Job = None) -> float:
        """
        Get timeout in minutes for specific job using business-friendly patterns
        
        Priority order:
        1. Job-declared timeout (if job provides it)
        2. Exact pattern match
        3. Wildcard pattern match  
        4. Default timeout
        """
        try:
            # Check if job itself declares a timeout
            if job and hasattr(job, 'meta') and job.meta:
                job_meta = job.meta
                if 'expected_runtime_minutes' in job_meta:
                    return flt(job_meta['expected_runtime_minutes'])
                elif 'timeout_minutes' in job_meta:
                    return flt(job_meta['timeout_minutes'])
            
            job_patterns = config.get('job_timeout_patterns', {})
            
            # Check for exact match first
            if job_name in job_patterns:
                return flt(job_patterns[job_name])
                
            # Check for wildcard pattern matches
            job_name_lower = job_name.lower()
            for pattern, timeout_minutes in job_patterns.items():
                if pattern.startswith('*') and pattern.endswith('*'):
                    # Pattern like '*dues*' - check if keyword is in job name
                    keyword = pattern.strip('*')
                    if keyword and keyword in job_name_lower:
                        return flt(timeout_minutes)
                elif pattern.startswith('*'):
                    # Pattern like '*_report' - check if job name ends with this
                    suffix = pattern[1:]
                    if job_name_lower.endswith(suffix):
                        return flt(timeout_minutes)
                elif pattern.endswith('*'):
                    # Pattern like 'frappe.*' - check if job name starts with this
                    prefix = pattern[:-1]
                    if job_name_lower.startswith(prefix):
                        return flt(timeout_minutes)
                elif pattern in job_name_lower:
                    # Simple substring match for backwards compatibility
                    return flt(timeout_minutes)
                    
            # Default timeout
            return flt(config.get('standard_job_timeout_minutes', 30))
            
        except Exception as e:
            frappe.log_error(f"Error getting job timeout: {str(e)}", "SchedulerHealthChecker")
            return 30.0  # Safe default
    
    def _create_stuck_job_alert(self, job: Job, config: Dict) -> StuckJobAlert:
        """Create a structured alert for a stuck job with error handling"""
        try:
            runtime_minutes = (now_datetime() - get_datetime(job.started_at)).total_seconds() / 60
            job_name = self._get_safe_job_name(job)
            configured_timeout = self._get_job_timeout_minutes(job_name, config, job)
            
            # Determine alert level based on how long it's been stuck
            multiplier = runtime_minutes / configured_timeout if configured_timeout > 0 else 1
            if multiplier >= 3:
                alert_level = "emergency"
                recommended_action = "terminate_immediately"
            elif multiplier >= 2:
                alert_level = "critical" 
                recommended_action = "terminate_with_grace"
            else:
                alert_level = "warning"
                recommended_action = "monitor_closely"
            
            # Safe queue extraction
            queue = "unknown"
            try:
                if hasattr(job, 'origin') and job.origin:
                    queue = str(job.origin).split(':')[-1]
            except Exception:
                pass
                
            return StuckJobAlert(
                job_id=getattr(job, 'id', 'unknown'),
                job_name=job_name,
                queue=queue,
                runtime_minutes=runtime_minutes,
                configured_timeout_minutes=configured_timeout,
                alert_level=alert_level,
                recommended_action=recommended_action
            )
            
        except Exception as e:
            frappe.log_error(f"Error creating stuck job alert: {str(e)}", "SchedulerHealthChecker")
            # Return minimal alert
            return StuckJobAlert(
                job_id=getattr(job, 'id', 'error'),
                job_name=f"error_creating_alert_{str(e)[:50]}",
                queue="unknown",
                runtime_minutes=0.0,
                configured_timeout_minutes=30.0,
                alert_level="warning",
                recommended_action="monitor_closely"
            )