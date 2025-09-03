"""
Scheduler Recovery Manager
=========================

Component for handling stuck jobs and recovery operations.
Manages job termination, security validation, and audit logging.
"""

import json
from typing import Dict, List, Optional, Any
import time

import frappe
from frappe.utils import now_datetime
from rq.job import Job

from frappe.utils.scheduler_monitor_types import StuckJobAlert, SecurityError, MonitoringError


class SchedulerRecoveryManager:
    """
    Component for job termination and recovery operations.
    
    Responsibility: Handle stuck jobs and recovery actions.
    """
    
    def __init__(self, site: str):
        self.site = site
        self._validate_site_access()
        
    def _validate_site_access(self):
        """Validate access to site for security isolation"""
        if not self.site:
            raise SecurityError("Site context required for recovery operations")
        
        # Validate site exists and is accessible
        if frappe.local.site and frappe.local.site != self.site:
            raise SecurityError(f"Cross-site access denied: current={frappe.local.site}, requested={self.site}")
        
    def handle_stuck_jobs(self, stuck_jobs: List[StuckJobAlert], config: Dict) -> List[Dict]:
        """
        Handle stuck jobs based on configuration and alert level with error handling.
        
        Args:
            stuck_jobs: List of stuck job alerts
            config: Monitoring configuration
            
        Returns:
            List of actions taken
        """
        try:
            if not isinstance(stuck_jobs, list):
                raise MonitoringError(f"Expected list of alerts, got {type(stuck_jobs)}")
            
            if not config:
                raise MonitoringError("Configuration required for recovery operations")
            
            actions_taken = []
            
            for alert in stuck_jobs:
                try:
                    if not isinstance(alert, StuckJobAlert):
                        frappe.log_error(f"Invalid alert type: {type(alert)}", "SchedulerRecoveryManager")
                        continue
                        
                    action = self._determine_action(alert, config)
                    
                    if action == 'terminate_immediately':
                        if self._should_allow_termination(alert, config) and self._terminate_job_secure(alert.job_id):
                            actions_taken.append({
                                'job_id': alert.job_id,
                                'job_name': alert.job_name,
                                'action': 'terminated_immediately',
                                'reason': f'Emergency termination after {alert.runtime_minutes:.1f} minutes',
                                'timestamp': now_datetime().isoformat(),
                                'site': self.site
                            })
                            
                    elif action == 'terminate_with_grace':
                        if config.get('enable_graceful_termination', False):
                            if self._should_allow_termination(alert, config) and self._terminate_job_secure(alert.job_id):
                                actions_taken.append({
                                    'job_id': alert.job_id,
                                    'job_name': alert.job_name,
                                    'action': 'terminated_gracefully',
                                    'reason': f'Critical timeout after {alert.runtime_minutes:.1f} minutes',
                                    'timestamp': now_datetime().isoformat(),
                                    'site': self.site
                                })
                                
                    elif action == 'monitor_closely':
                        actions_taken.append({
                            'job_id': alert.job_id,
                            'job_name': alert.job_name,
                            'action': 'monitoring',
                            'reason': f'Warning threshold exceeded at {alert.runtime_minutes:.1f} minutes',
                            'timestamp': now_datetime().isoformat(),
                            'site': self.site
                        })
                        
                except Exception as e:
                    error_msg = f"Error handling stuck job {getattr(alert, 'job_id', 'unknown')}: {str(e)}"
                    frappe.log_error(error_msg, "SchedulerRecoveryManager")
                    continue
                        
            return actions_taken
            
        except Exception as e:
            frappe.log_error(f"Recovery operation failed: {str(e)}", "SchedulerRecoveryManager")
            return []
    
    def _should_allow_termination(self, alert: StuckJobAlert, config: Dict) -> bool:
        """Additional safety checks before job termination"""
        try:
            # Check for termination-exempt patterns
            exempt_patterns = config.get('termination_exempt_patterns', [])
            job_name = alert.job_name.lower()
            
            for pattern in exempt_patterns:
                if isinstance(pattern, str) and pattern.lower() in job_name:
                    frappe.logger("scheduler_monitor").info(
                        f"Job {alert.job_id} exempt from termination (pattern: {pattern})"
                    )
                    return False
            
            # Check minimum runtime threshold
            min_runtime = config.get('minimum_runtime_before_termination_minutes', 10)
            if alert.runtime_minutes < min_runtime:
                frappe.logger("scheduler_monitor").info(
                    f"Job {alert.job_id} runtime {alert.runtime_minutes:.1f}m below minimum {min_runtime}m"
                )
                return False
            
            return True
            
        except Exception as e:
            frappe.log_error(f"Error validating termination permission: {str(e)}", "SchedulerRecoveryManager")
            return False  # Fail safe - don't terminate if we can't validate
    
    def _determine_action(self, alert: StuckJobAlert, config: Dict) -> str:
        """Determine what action to take for a stuck job with validation"""
        try:
            protection_level = config.get('protection_level', 'monitor_only')
            
            # Validate protection level
            valid_levels = ['monitor_only', 'safe_protection', 'full_protection']
            if protection_level not in valid_levels:
                frappe.log_error(f"Invalid protection level: {protection_level}", "SchedulerRecoveryManager")
                protection_level = 'monitor_only'
            
            if protection_level == 'monitor_only':
                return 'monitor_closely'
            elif protection_level == 'safe_protection':
                return alert.recommended_action if alert.alert_level == 'emergency' else 'monitor_closely'
            elif protection_level == 'full_protection':
                return alert.recommended_action
            else:
                return 'monitor_closely'
                
        except Exception as e:
            frappe.log_error(f"Error determining action: {str(e)}", "SchedulerRecoveryManager")
            return 'monitor_closely'  # Safe fallback
    
    def _terminate_job_secure(self, job_id: str) -> bool:
        """
        Securely terminate a stuck job with validation and audit logging.
        
        Args:
            job_id: ID of the job to terminate
            
        Returns:
            bool: True if termination successful, False otherwise
        """
        try:
            if not job_id or not isinstance(job_id, str):
                frappe.log_error(f"Invalid job_id: {job_id}", "SchedulerRecoveryManager")
                return False
            
            # Enhanced security validation using direct RQ job access
            try:
                from frappe.utils.background_jobs import get_redis_conn
                from rq import Queue
                from rq.registry import StartedJobRegistry
                
                # Get Redis connection with error handling
                try:
                    redis_conn = get_redis_conn()
                    if not redis_conn:
                        raise MonitoringError("Redis connection not available")
                except Exception as e:
                    frappe.log_error(f"Redis connection error: {str(e)}", "SchedulerRecoveryManager")
                    return False
                
                # Find job across all queues for this site
                job = None
                queues = ['default', 'short', 'long']
                
                for queue_name in queues:
                    try:
                        queue = Queue(queue_name, connection=redis_conn)
                        registry = queue.get_started_job_registry()
                        
                        if job_id in registry.get_job_ids():
                            job = Job.fetch(job_id, connection=redis_conn)
                            break
                    except Exception as e:
                        continue  # Try next queue
                
                if not job:
                    frappe.logger("scheduler_monitor").warning(
                        f"Job {job_id} not found in started registries, may have completed"
                    )
                    return False
                
                # Validate job site ownership for security
                self._validate_job_site_ownership(job, job_id)
                
                # Perform termination with timeout
                termination_start = time.time()
                try:
                    job.cancel()
                    termination_duration = time.time() - termination_start
                    
                    frappe.logger("scheduler_monitor").warning(
                        f"Successfully terminated stuck job: {job_id} (site: {self.site}, duration: {termination_duration:.3f}s)"
                    )
                except Exception as e:
                    frappe.log_error(f"Job termination failed for {job_id}: {str(e)}", "SchedulerRecoveryManager")
                    return False
                
                # Enhanced audit logging
                self._create_termination_audit_log(job_id, job, termination_duration)
                
                return True
                
            except SecurityError:
                # Re-raise security errors
                raise
            except Exception as e:
                frappe.log_error(f"Job termination error for {job_id}: {str(e)}", "SchedulerRecoveryManager")
                return False
            
        except SecurityError as e:
            frappe.log_error(f"Security violation during job termination: {str(e)}", "SchedulerRecoveryManager")
            return False
        except Exception as e:
            frappe.log_error(f"Unexpected error terminating job {job_id}: {str(e)}", "SchedulerRecoveryManager")
            return False
    
    def _validate_job_site_ownership(self, job: Job, job_id: str):
        """Validate job belongs to current site for security"""
        try:
            # Check job metadata for site information
            job_site = None
            if hasattr(job, 'meta') and job.meta:
                job_site = job.meta.get('site')
            
            # Also check job kwargs
            if not job_site and hasattr(job, 'kwargs') and job.kwargs:
                job_site = job.kwargs.get('site')
            
            # Also check job args for site context
            if not job_site and hasattr(job, 'args') and job.args:
                for arg in job.args:
                    if isinstance(arg, dict) and 'site' in arg:
                        job_site = arg['site']
                        break
            
            if job_site and job_site != self.site:
                error_msg = f"Cross-site job termination blocked: job {job_id} belongs to site '{job_site}', not '{self.site}'"
                frappe.log_error(
                    title="Scheduler Monitor: Security Violation",
                    message=error_msg,
                    reference_doctype="RQ Job",
                    reference_name=job_id
                )
                raise SecurityError(error_msg)
                
            if not job_site:
                frappe.logger("scheduler_monitor").warning(
                    f"Job {job_id} has no site context - proceeding with caution"
                )
                
        except SecurityError:
            raise
        except Exception as e:
            frappe.log_error(f"Error validating job site ownership: {str(e)}", "SchedulerRecoveryManager")
            # Don't raise - log and continue with termination
    
    def _create_termination_audit_log(self, job_id: str, job: Optional[Job], duration: float):
        """Create audit log for job termination"""
        try:
            # Safely extract job details
            job_name = "unknown"
            job_queue = "unknown"
            job_args = None
            job_kwargs = None
            
            if job:
                try:
                    if hasattr(job, 'func_name'):
                        job_name = str(job.func_name)
                    elif hasattr(job, 'description'):
                        job_name = str(job.description)
                        
                    if hasattr(job, 'origin'):
                        job_queue = str(job.origin)
                        
                    if hasattr(job, 'args'):
                        job_args = job.args
                        
                    if hasattr(job, 'kwargs'):
                        job_kwargs = job.kwargs
                except Exception as e:
                    frappe.log_error(f"Error extracting job details: {str(e)}", "SchedulerRecoveryManager")
            
            audit_details = {
                'event': 'automated_job_termination',
                'job_id': job_id,
                'job_name': job_name,
                'job_queue': job_queue,
                'site': self.site,
                'termination_duration_seconds': round(duration, 3),
                'timestamp': now_datetime().isoformat(),
                'monitoring_system': 'frappe_scheduler_monitor',
                'job_args_count': len(job_args) if job_args else 0,
                'job_kwargs_keys': list(job_kwargs.keys()) if job_kwargs else [],
                'frappe_version': frappe.__version__ if hasattr(frappe, '__version__') else 'unknown'
            }
            
            frappe.log_error(
                title=f"Scheduler Monitor: Job Termination Audit",
                message=json.dumps(audit_details, indent=2, default=str),
                reference_doctype="RQ Job",
                reference_name=job_id
            )
            
        except Exception as e:
            frappe.log_error(f"Error creating audit log: {str(e)}", "SchedulerRecoveryManager")
    
    def log_stuck_jobs(self, stuck_jobs: List[StuckJobAlert]) -> None:
        """Log stuck job alerts for administrative visibility with error handling"""
        try:
            if not isinstance(stuck_jobs, list):
                return
                
            for alert in stuck_jobs:
                try:
                    if not isinstance(alert, StuckJobAlert):
                        continue
                        
                    log_level = {
                        'warning': 'warning',
                        'critical': 'error',
                        'emergency': 'critical'
                    }.get(alert.alert_level, 'warning')
                    
                    message = (
                        f"🔴 STUCK JOB: {alert.job_name} "
                        f"(ID: {alert.job_id}, Runtime: {alert.runtime_minutes:.1f}m, "
                        f"Timeout: {alert.configured_timeout_minutes:.1f}m, "
                        f"Queue: {alert.queue}, Alert: {alert.alert_level})"
                    )
                    
                    getattr(frappe.logger("scheduler_monitor"), log_level)(message)
                    
                    # Also create Error Log for admin dashboard visibility
                    frappe.log_error(
                        title=f"Stuck Job Alert: {alert.job_name}",
                        message=json.dumps(self._serialize_stuck_job(alert), indent=2, default=str),
                        reference_doctype="RQ Job",
                        reference_name=alert.job_id
                    )
                    
                except Exception as e:
                    frappe.log_error(f"Error logging stuck job alert: {str(e)}", "SchedulerRecoveryManager")
                    continue
                    
        except Exception as e:
            frappe.log_error(f"Error logging stuck jobs: {str(e)}", "SchedulerRecoveryManager")
    
    def _serialize_stuck_job(self, alert: StuckJobAlert) -> Dict[str, Any]:
        """Convert StuckJobAlert to dictionary for JSON serialization"""
        try:
            return {
                'job_id': alert.job_id,
                'job_name': alert.job_name,
                'queue': alert.queue,
                'runtime_minutes': round(alert.runtime_minutes, 2),
                'configured_timeout_minutes': round(alert.configured_timeout_minutes, 2),
                'timeout_exceeded_by_minutes': round(alert.runtime_minutes - alert.configured_timeout_minutes, 2),
                'timeout_multiplier': round(alert.runtime_minutes / alert.configured_timeout_minutes, 2) if alert.configured_timeout_minutes > 0 else 0,
                'alert_level': alert.alert_level,
                'recommended_action': alert.recommended_action,
                'site': self.site,
                'detection_timestamp': now_datetime().isoformat()
            }
        except Exception as e:
            frappe.log_error(f"Error serializing stuck job: {str(e)}", "SchedulerRecoveryManager")
            return {
                'job_id': getattr(alert, 'job_id', 'unknown'),
                'error': f"Serialization failed: {str(e)}",
                'site': self.site
            }