"""
Frappe Scheduler Monitor
========================

Proactive monitoring and protection system for Frappe's RQ-based scheduler.
Builds on existing RQ Job infrastructure to detect and resolve stuck jobs
before they can block the scheduler.

Key features:
- Integration with existing RQ Job system
- Configurable timeout monitoring beyond job-level timeouts
- Graduated response system (log -> alert -> terminate)
- Site-specific configuration with sensible defaults
- Comprehensive metrics and alerting
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

import frappe
from frappe.utils import now_datetime, get_datetime, cint, flt
from frappe.core.doctype.rq_job.rq_job import get_all_queued_jobs, serialize_job
from frappe.utils.background_jobs import get_redis_conn
from rq.job import Job
from rq.queue import Queue


@dataclass
class StuckJobAlert:
    """Data class for stuck job alert information"""
    job_id: str
    job_name: str
    queue: str
    runtime_minutes: float
    configured_timeout_minutes: float
    alert_level: str  # warning, critical, emergency
    recommended_action: str


class SchedulerMonitor:
    """
    Main scheduler monitoring service that integrates with Frappe's RQ infrastructure.
    Provides proactive detection and handling of stuck jobs.
    """
    
    def __init__(self, site: str = None):
        self.site = site or frappe.local.site
        self.redis_conn = get_redis_conn()
        
    def run_monitoring_cycle(self) -> Dict[str, Any]:
        """
        Execute one complete monitoring cycle.
        
        Returns:
            dict: Monitoring cycle results including any actions taken
        """
        if not self._is_monitoring_enabled():
            return {
                'status': 'disabled',
                'message': f'Scheduler monitoring disabled for site {self.site}'
            }
            
        try:
            cycle_start = time.time()
            
            # Get current job states from RQ
            running_jobs = self._get_running_jobs()
            
            # Analyze for stuck jobs
            stuck_jobs = self._identify_stuck_jobs(running_jobs)
            
            # Take appropriate actions
            actions_taken = self._handle_stuck_jobs(stuck_jobs)
            
            # Record metrics
            cycle_metrics = {
                'timestamp': now_datetime(),
                'site': self.site,
                'total_running_jobs': len(running_jobs),
                'stuck_jobs_detected': len(stuck_jobs),
                'actions_taken': len(actions_taken),
                'cycle_duration_seconds': round(time.time() - cycle_start, 3)
            }
            
            self._record_metrics(cycle_metrics)
            
            # Log significant findings
            if stuck_jobs:
                self._log_stuck_jobs(stuck_jobs)
                
            return {
                'status': 'completed',
                'metrics': cycle_metrics,
                'stuck_jobs': [self._serialize_stuck_job(job) for job in stuck_jobs],
                'actions': actions_taken
            }
            
        except Exception as e:
            frappe.log_error(
                f"Scheduler monitoring cycle failed: {str(e)}", 
                "Scheduler Monitor"
            )
            return {
                'status': 'error',
                'error': str(e),
                'site': self.site
            }
    
    def _get_running_jobs(self) -> List[Job]:
        """Get all currently running jobs from RQ"""
        running_jobs = []
        
        try:
            # Use existing Frappe infrastructure to get jobs
            all_jobs = get_all_queued_jobs()
            
            for job in all_jobs:
                if job.get_status() == 'started':
                    running_jobs.append(job)
                    
        except Exception as e:
            frappe.logger("scheduler_monitor").error(
                f"Failed to get running jobs: {str(e)}"
            )
            
        return running_jobs
    
    def _identify_stuck_jobs(self, running_jobs: List[Job]) -> List[StuckJobAlert]:
        """Analyze running jobs to identify stuck ones"""
        stuck_jobs = []
        config = self._get_monitor_config()
        
        for job in running_jobs:
            if self._is_job_stuck(job, config):
                alert = self._create_stuck_job_alert(job, config)
                stuck_jobs.append(alert)
                
        return stuck_jobs
    
    def _is_job_stuck(self, job: Job, config: Dict) -> bool:
        """Determine if a job appears to be stuck"""
        if not job.started_at:
            return False
            
        # Calculate runtime
        runtime_minutes = (now_datetime() - get_datetime(job.started_at)).total_seconds() / 60
        
        # Get configured timeout for this job type
        job_name = str(job.func_name) if hasattr(job, 'func_name') else str(job.description)
        job_timeout_minutes = self._get_job_timeout_minutes(job_name, config)
        
        # Primary check: exceeded configured timeout
        if runtime_minutes > job_timeout_minutes:
            return True
            
        # Secondary check: exceeded global maximum runtime
        global_max_minutes = config.get('global_max_runtime_minutes', 120)  # 2 hours default
        if runtime_minutes > global_max_minutes:
            return True
            
        return False
    
    def _get_job_timeout_minutes(self, job_name: str, config: Dict) -> float:
        """Get timeout in minutes for specific job"""
        job_timeouts = config.get('job_timeouts', {})
        
        # Check for exact match first
        if job_name in job_timeouts:
            return flt(job_timeouts[job_name]) / 60  # Convert seconds to minutes
            
        # Check for partial matches (for dynamic job names)
        for pattern, timeout_seconds in job_timeouts.items():
            if pattern in job_name:
                return flt(timeout_seconds) / 60
                
        # Default timeout
        return flt(config.get('default_timeout_seconds', 1800)) / 60  # 30 minutes default
    
    def _create_stuck_job_alert(self, job: Job, config: Dict) -> StuckJobAlert:
        """Create a structured alert for a stuck job"""
        runtime_minutes = (now_datetime() - get_datetime(job.started_at)).total_seconds() / 60
        job_name = str(job.func_name) if hasattr(job, 'func_name') else str(job.description)
        configured_timeout = self._get_job_timeout_minutes(job_name, config)
        
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
            
        return StuckJobAlert(
            job_id=job.id,
            job_name=job_name,
            queue=getattr(job, 'origin', 'unknown').split(':')[-1] if hasattr(job, 'origin') else 'unknown',
            runtime_minutes=runtime_minutes,
            configured_timeout_minutes=configured_timeout,
            alert_level=alert_level,
            recommended_action=recommended_action
        )
    
    def _handle_stuck_jobs(self, stuck_jobs: List[StuckJobAlert]) -> List[Dict]:
        """Handle stuck jobs based on configuration and alert level"""
        actions_taken = []
        config = self._get_monitor_config()
        
        for alert in stuck_jobs:
            action = self._determine_action(alert, config)
            
            if action == 'terminate_immediately':
                if self._terminate_job(alert.job_id):
                    actions_taken.append({
                        'job_id': alert.job_id,
                        'action': 'terminated',
                        'reason': f'Emergency termination after {alert.runtime_minutes:.1f} minutes'
                    })
                    
            elif action == 'terminate_with_grace':
                if config.get('enable_graceful_termination', False):
                    if self._terminate_job(alert.job_id):
                        actions_taken.append({
                            'job_id': alert.job_id,
                            'action': 'terminated_gracefully',
                            'reason': f'Critical timeout after {alert.runtime_minutes:.1f} minutes'
                        })
                        
            elif action == 'monitor_closely':
                actions_taken.append({
                    'job_id': alert.job_id,
                    'action': 'monitoring',
                    'reason': f'Warning threshold exceeded at {alert.runtime_minutes:.1f} minutes'
                })
                
        return actions_taken
    
    def _determine_action(self, alert: StuckJobAlert, config: Dict) -> str:
        """Determine what action to take for a stuck job"""
        monitor_mode = config.get('monitor_mode', 'observe_only')
        
        if monitor_mode == 'observe_only':
            return 'monitor_closely'
        elif monitor_mode == 'conservative':
            return alert.recommended_action if alert.alert_level == 'emergency' else 'monitor_closely'
        elif monitor_mode == 'active':
            return alert.recommended_action
        else:
            return 'monitor_closely'
    
    def _terminate_job(self, job_id: str) -> bool:
        """Terminate a stuck job using existing RQ infrastructure"""
        try:
            # Use existing Frappe RQ Job functionality
            from frappe.core.doctype.rq_job.rq_job import stop_job
            stop_job(job_id)
            
            frappe.logger("scheduler_monitor").warning(
                f"Terminated stuck job: {job_id}"
            )
            return True
            
        except Exception as e:
            frappe.logger("scheduler_monitor").error(
                f"Failed to terminate job {job_id}: {str(e)}"
            )
            return False
    
    def _log_stuck_jobs(self, stuck_jobs: List[StuckJobAlert]) -> None:
        """Log stuck job alerts for administrative visibility"""
        for alert in stuck_jobs:
            log_level = {
                'warning': 'warning',
                'critical': 'error',
                'emergency': 'critical'
            }.get(alert.alert_level, 'warning')
            
            message = (
                f"🔴 STUCK JOB: {alert.job_name} "
                f"(ID: {alert.job_id}, Runtime: {alert.runtime_minutes:.1f}m, "
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
    
    def _serialize_stuck_job(self, alert: StuckJobAlert) -> Dict:
        """Convert StuckJobAlert to dictionary for JSON serialization"""
        return {
            'job_id': alert.job_id,
            'job_name': alert.job_name,
            'queue': alert.queue,
            'runtime_minutes': round(alert.runtime_minutes, 2),
            'configured_timeout_minutes': round(alert.configured_timeout_minutes, 2),
            'alert_level': alert.alert_level,
            'recommended_action': alert.recommended_action,
            'site': self.site
        }
    
    def _record_metrics(self, metrics: Dict) -> None:
        """Record monitoring metrics for analysis and alerting"""
        try:
            cache_key = f"scheduler_monitor_metrics:{self.site}"
            
            # Get existing metrics
            existing_metrics = frappe.cache().get(cache_key) or []
            existing_metrics.append(metrics)
            
            # Keep only last 200 entries (about 16 hours at 5-minute intervals)
            if len(existing_metrics) > 200:
                existing_metrics = existing_metrics[-200:]
                
            # Store with 24 hour expiration
            frappe.cache().set(cache_key, existing_metrics, expires_in_sec=86400)
            
            # Log summary for external monitoring
            if metrics['stuck_jobs_detected'] > 0:
                frappe.logger("scheduler_monitor").info(
                    f"Monitor cycle: {metrics['stuck_jobs_detected']} stuck jobs, "
                    f"{metrics['actions_taken']} actions taken"
                )
                
        except Exception as e:
            frappe.logger("scheduler_monitor").error(
                f"Failed to record metrics: {str(e)}"
            )
    
    def _is_monitoring_enabled(self) -> bool:
        """Check if monitoring is enabled for this site"""
        config = self._get_monitor_config()
        return config.get('enabled', False)
    
    def _get_monitor_config(self) -> Dict:
        """Get monitoring configuration with defaults"""
        try:
            site_config = frappe.get_site_config()
            monitor_config = site_config.get('scheduler_monitor', {})
            
            # Default configuration
            defaults = {
                'enabled': False,  # Must be explicitly enabled
                'monitor_mode': 'observe_only',  # observe_only, conservative, active
                'default_timeout_seconds': 1800,  # 30 minutes
                'global_max_runtime_minutes': 120,  # 2 hours absolute maximum
                'enable_graceful_termination': False,
                'job_timeouts': {
                    'frappe.twofactor.delete_all_barcodes_for_users': 3600,  # 1 hour
                    'frappe.utils.global_search.sync_global_search': 1800,  # 30 minutes
                    'frappe.email.queue.flush': 300,  # 5 minutes
                }
            }
            
            # Merge with site-specific config
            for key, value in defaults.items():
                if key not in monitor_config:
                    monitor_config[key] = value
                elif key == 'job_timeouts':
                    # Merge job timeouts
                    defaults[key].update(monitor_config[key])
                    monitor_config[key] = defaults[key]
                    
            return monitor_config
            
        except Exception as e:
            frappe.logger("scheduler_monitor").debug(
                f"Could not load monitor config: {str(e)}"
            )
            return {'enabled': False}


# Utility functions for external integration

def run_scheduler_monitoring_cycle(site: str = None) -> Dict[str, Any]:
    """Run a single monitoring cycle and return results"""
    monitor = SchedulerMonitor(site)
    return monitor.run_monitoring_cycle()


def is_monitoring_enabled(site: str = None) -> bool:
    """Check if scheduler monitoring is enabled for site"""
    monitor = SchedulerMonitor(site)
    return monitor._is_monitoring_enabled()


def get_monitoring_status(site: str = None) -> Dict[str, Any]:
    """Get current monitoring system status and recent metrics"""
    monitor = SchedulerMonitor(site)
    
    if not monitor._is_monitoring_enabled():
        return {
            'enabled': False,
            'site': monitor.site,
            'message': 'Scheduler monitoring not enabled'
        }
        
    try:
        # Get recent metrics
        cache_key = f"scheduler_monitor_metrics:{monitor.site}"
        recent_metrics = frappe.cache().get(cache_key) or []
        
        # Calculate summary statistics
        total_cycles = len(recent_metrics)
        if total_cycles == 0:
            return {
                'enabled': True,
                'site': monitor.site,
                'message': 'No monitoring cycles recorded yet'
            }
            
        stuck_jobs_total = sum(m.get('stuck_jobs_detected', 0) for m in recent_metrics)
        actions_total = sum(m.get('actions_taken', 0) for m in recent_metrics)
        avg_duration = sum(m.get('cycle_duration_seconds', 0) for m in recent_metrics) / total_cycles
        
        return {
            'enabled': True,
            'site': monitor.site,
            'total_monitoring_cycles': total_cycles,
            'stuck_jobs_detected': stuck_jobs_total,
            'total_actions_taken': actions_total,
            'average_cycle_duration': round(avg_duration, 3),
            'last_cycle': recent_metrics[-1] if recent_metrics else None,
            'config': monitor._get_monitor_config()
        }
        
    except Exception as e:
        return {
            'enabled': False,
            'site': monitor.site,
            'error': str(e)
        }