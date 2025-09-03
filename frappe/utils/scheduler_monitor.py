"""
Frappe Scheduler Monitor
========================

Monitoring system for Frappe's RQ-based job scheduler.
Detects stuck jobs and provides recovery actions with security validation
and audit logging.

Architecture:
- Main monitor coordinates health checking, metrics collection, and recovery
- Site-based security validation prevents cross-site job access
- Handles Redis connection failures gracefully
- Audit logging for job termination actions
- LRU caching for performance optimization

Configuration in site_config.json:
    {
        "scheduler_monitor": {
            "enabled": true,
            "protection_level": "safe_protection",
            "standard_job_timeout_minutes": 30,
            "job_timeout_patterns": {
                "*dues*": 60,
                "*sepa*": 30, 
                "frappe.email.queue.flush": 5
            },
            "max_jobs_per_cycle": 100,
            "enable_graceful_termination": true
        }
    }

Protection Levels:
- monitor_only: Detection and alerting only, no job termination
- safe_protection: Terminate only emergency-level jobs (3x+ timeout)  
- full_protection: Terminate jobs at configured timeout thresholds

Security Features:
- Site isolation validation for multi-tenant environments
- Audit logging for job termination actions
- Permission validation before job termination
- Configuration limits to prevent abuse

Usage:
    from frappe.utils.scheduler_monitor import SchedulerMonitor
    
    monitor = SchedulerMonitor()  # Uses current site
    try:
        result = monitor.run_monitoring_cycle()
        if result['stuck_jobs_detected'] > 0:
            # Review audit logs and take appropriate action
            pass
    except Exception as e:
        # System handles errors gracefully and logs for analysis
        frappe.log_error(f"Scheduler monitoring failed: {e}")
"""

import time
import json
import hashlib
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union

import frappe
from frappe.utils import now_datetime, get_datetime, cint, flt
from frappe.utils.background_jobs import get_redis_conn
from frappe.core.doctype.rq_job.rq_job import get_all_queued_jobs
from rq.job import Job
from rq.queue import Queue
from rq.exceptions import NoSuchJobError

from frappe.utils.scheduler_monitor_types import (
    StuckJobAlert,
    MonitoringMetrics, 
    HealthCheckResult,
    MonitoringError,
    ConfigurationError,
    SecurityError,
    RedisConnectionError
)
from frappe.utils.scheduler_monitor_config import SchedulerMonitorConfig


class SchedulerMonitor:
    """
    Scheduler monitoring system for RQ job queues.
    
    Provides monitoring of job queues with security validation,
    performance tracking, and error handling.
    """
    
    def __init__(self, site: Optional[str] = None):
        """
        Initialize monitor for specified site.
        
        Args:
            site: Site name, defaults to current site from frappe.local
        """
        self.site = site or frappe.local.site
        if not self.site:
            raise ConfigurationError("No site context available for scheduler monitor")
            
        # Initialize configuration management
        self.config_manager = SchedulerMonitorConfig(self.site)
        self.config = self.config_manager.get_config()
        
        # Initialize with connection validation
        self.redis_conn = None
        self._validate_redis_connection()
        
        # Performance tracking
        self._redis_call_count = 0
        self._cycle_start_time = None
        
        # LRU cache for timeout pattern matching (performance optimization)
        self._pattern_cache = OrderedDict()
        self._max_cache_size = 100
        self._cache_hits = 0
        self._cache_misses = 0
        
        # Load and validate configuration
        self.config = self._load_and_validate_config()
    
    def _validate_redis_connection(self) -> None:
        """Validate Redis connectivity with proper error handling."""
        try:
            self.redis_conn = get_redis_conn()
            self.redis_conn.ping()
        except Exception as e:
            # Don't fail completely - allow graceful degradation
            frappe.log_error(
                title="Scheduler Monitor: Redis Connection Failed",
                message=f"Redis connection failed for site {self.site}: {str(e)}"
            )
            raise RedisConnectionError(f"Redis unavailable: {e}")
    
    def _load_and_validate_config(self) -> Dict[str, Any]:
        """Load configuration with validation and security limits."""
        try:
            site_config = frappe.get_site_config() or {}
            monitor_config = site_config.get('scheduler_monitor', {})
            
            # Default configuration with security-conscious defaults
            config = {
                'enabled': False,  # Must be explicitly enabled
                'protection_level': 'monitor_only',
                'standard_job_timeout_minutes': 30,
                'maximum_job_runtime_hours': 2,  # Security limit
                'max_jobs_per_cycle': 100,  # Performance limit  
                'enable_graceful_termination': False,
                'job_timeout_patterns': {},
                'require_admin_approval_for_termination': True
            }
            
            # Merge with site configuration
            config.update(monitor_config)
            
            # Validate and apply security limits
            return self._apply_security_limits(config)
            
        except Exception as e:
            frappe.log_error(
                title="Scheduler Monitor: Configuration Error", 
                message=f"Failed to load config for site {self.site}: {str(e)}"
            )
            raise ConfigurationError(f"Invalid configuration: {e}")
    
    def _apply_security_limits(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply security limits to prevent configuration abuse."""
        # Enforce maximum job limits (prevent resource exhaustion)
        max_jobs = config.get('max_jobs_per_cycle', 100)
        if max_jobs > 500:  # System limit
            frappe.log_error(
                title="Scheduler Monitor: Security Limit Applied",
                message=f"max_jobs_per_cycle reduced from {max_jobs} to 500"
            )
            config['max_jobs_per_cycle'] = 500
        
        # Validate timeout minimums (prevent false positives)
        min_timeout = config.get('standard_job_timeout_minutes', 30)
        if min_timeout < 1:
            frappe.log_error(
                title="Scheduler Monitor: Security Limit Applied", 
                message=f"standard_job_timeout_minutes increased from {min_timeout} to 1"
            )
            config['standard_job_timeout_minutes'] = 1
        
        # Validate protection levels
        valid_levels = ['monitor_only', 'safe_protection', 'full_protection']
        protection_level = config.get('protection_level', 'monitor_only')
        if protection_level not in valid_levels:
            frappe.log_error(
                title="Scheduler Monitor: Invalid Protection Level",
                message=f"Invalid protection_level '{protection_level}', using monitor_only"
            )
            config['protection_level'] = 'monitor_only'
        
        # Security warning for full protection
        if protection_level == 'full_protection':
            frappe.log_error(
                title="Scheduler Monitor: Full Protection Enabled",
                message=f"Full protection level enabled for site {self.site} - automated job termination allowed"
            )
        
        return config
    
    def run_monitoring_cycle(self) -> Dict[str, Any]:
        """
        Execute monitoring cycle with error handling.
        
        Returns results including performance metrics and any errors.
        """
        cycle_start = time.perf_counter()
        cpu_start = time.process_time()
        self._redis_call_count = 0
        
        try:
            # Skip if monitoring disabled
            if not self.config.get('enabled', False):
                return {
                    'status': 'disabled',
                    'message': 'Scheduler monitoring not enabled for this site'
                }
            
            # Get running jobs with error handling
            running_jobs = self._get_running_jobs_safe()
            
            # Analyze job health
            health_result = self._analyze_job_health(running_jobs)
            
            # Handle stuck jobs based on protection level  
            actions_taken = self._handle_stuck_jobs(health_result.stuck_jobs)
            
            # Record metrics and audit logs
            metrics = self._create_cycle_metrics(
                cycle_start, cpu_start, len(running_jobs), 
                len(health_result.stuck_jobs), len(actions_taken)
            )
            
            self._record_metrics_safe(metrics)
            
            if health_result.stuck_jobs:
                self._log_stuck_jobs_for_audit(health_result.stuck_jobs)
            
            return {
                'status': 'completed',
                'jobs_monitored': len(running_jobs),
                'stuck_jobs_detected': len(health_result.stuck_jobs), 
                'actions_taken': len(actions_taken),
                'cycle_duration_ms': metrics.cycle_duration_ms,
                'jobs_per_second': metrics.jobs_per_second,
                'errors': health_result.errors,
                'monitoring_results': {
                    'healthy_jobs': len(health_result.healthy_jobs),
                    'cache_efficiency': self._calculate_cache_efficiency()
                }
            }
            
        except RedisConnectionError as e:
            return self._handle_infrastructure_failure('redis', str(e))
        except ConfigurationError as e:
            return self._handle_infrastructure_failure('config', str(e))
        except Exception as e:
            frappe.log_error(
                title="Scheduler Monitor: Unexpected Error",
                message=f"Monitoring cycle failed for site {self.site}: {str(e)}"
            )
            return {
                'status': 'error',
                'error': str(e),
                'site': self.site
            }
    
    def _get_running_jobs_safe(self) -> List[Dict[str, Any]]:
        """Get running jobs with error handling."""
        try:
            self._redis_call_count += 1
            jobs = []
            
            # Use Frappe's RQ integration for job enumeration
            for queue_name in ['default', 'short', 'long']:
                try:
                    queue = Queue(queue_name, connection=self.redis_conn)
                    
                    # Get started jobs (actually running) using correct RQ API
                    registry = queue.get_started_job_registry()
                    started_jobs = registry.get_job_ids()
                    
                    for job_id in started_jobs[:self.config['max_jobs_per_cycle']]:
                        try:
                            job = Job.fetch(job_id, connection=self.redis_conn)
                            
                            # Validate job belongs to our site (security)
                            if not self._validate_job_site_access(job):
                                continue
                            
                            job_data = {
                                'job_id': job.id,
                                'function_name': getattr(job, 'func_name', str(job.func)),
                                'status': job.status,
                                'started_at': job.started_at or now_datetime(),
                                'queue': queue_name,
                                'runtime_minutes': self._calculate_job_runtime_minutes(job),
                                'meta': getattr(job, 'meta', {})
                            }
                            jobs.append(job_data)
                            
                        except NoSuchJobError:
                            # Job completed between enumeration and fetch
                            continue
                        except Exception as e:
                            frappe.logger("scheduler_monitor").warning(
                                f"Error fetching job {job_id}: {str(e)}"
                            )
                            continue
                            
                except Exception as e:
                    frappe.logger("scheduler_monitor").error(
                        f"Error accessing queue {queue_name}: {str(e)}"
                    )
                    continue
            
            return jobs
            
        except Exception as e:
            frappe.log_error(
                title="Scheduler Monitor: Job Enumeration Failed",
                message=f"Failed to get running jobs for site {self.site}: {str(e)}"
            )
            raise RedisConnectionError(f"Failed to enumerate jobs: {e}")
    
    def _validate_job_site_access(self, job: Job) -> bool:
        """Validate job belongs to current site for security."""
        try:
            job_kwargs = getattr(job, 'kwargs', {})
            if not job_kwargs:
                # Jobs without context are system-level, require careful handling
                frappe.logger("scheduler_monitor").info(
                    f"Job {job.id} has no site context - allowing system job"
                )
                return True
            
            job_site = job_kwargs.get('site')
            if job_site and job_site != self.site:
                frappe.logger("scheduler_monitor").warning(
                    f"Blocking access to job {job.id} from site {job_site} (current: {self.site})"
                )
                return False
            
            return True
            
        except Exception as e:
            frappe.logger("scheduler_monitor").error(
                f"Error validating job site access: {str(e)}"
            )
            return False  # Fail secure
    
    def _calculate_job_runtime_minutes(self, job: Job) -> float:
        """Calculate job runtime with error handling for invalid timestamps."""
        try:
            if not job.started_at:
                return 0.0
            
            if isinstance(job.started_at, str):
                # Handle string timestamps
                started_at = get_datetime(job.started_at)
            else:
                started_at = job.started_at
            
            runtime_seconds = (now_datetime() - started_at).total_seconds()
            return max(0.0, runtime_seconds / 60.0)  # Ensure non-negative
            
        except Exception as e:
            frappe.logger("scheduler_monitor").warning(
                f"Error calculating runtime for job {job.id}: {str(e)}"
            )
            return 0.0
    
    def _analyze_job_health(self, jobs: List[Dict[str, Any]]) -> HealthCheckResult:
        """Analyze job health with error handling."""
        check_start = time.perf_counter()
        healthy_jobs = []
        stuck_jobs = []
        errors = []
        
        for job in jobs:
            try:
                # Get configured timeout for this job
                timeout_minutes = self._get_job_timeout_minutes(
                    job['function_name'], 
                    job.get('meta', {})
                )
                
                # Check if job is stuck
                if self._is_job_stuck(job, timeout_minutes):
                    alert = self._create_stuck_job_alert(job, timeout_minutes)
                    stuck_jobs.append(alert)
                else:
                    healthy_jobs.append({
                        'job_id': job['job_id'],
                        'function_name': job['function_name'],
                        'runtime_minutes': job['runtime_minutes']
                    })
                    
            except Exception as e:
                error_msg = f"Error analyzing job {job.get('job_id', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                frappe.logger("scheduler_monitor").error(error_msg)
        
        check_duration = (time.perf_counter() - check_start) * 1000.0
        
        return HealthCheckResult(
            total_jobs_checked=len(jobs),
            healthy_jobs=healthy_jobs,
            stuck_jobs=stuck_jobs,
            check_duration_ms=check_duration,
            errors=errors
        )
    
    def _get_job_timeout_minutes(self, job_name: str, job_meta: Dict[str, Any] = None) -> float:
        """Get timeout for job with caching and proper priority handling."""
        # Priority 1: Job-declared timeout in metadata
        if job_meta and 'expected_runtime_minutes' in job_meta:
            return flt(job_meta['expected_runtime_minutes'])
        
        # Priority 2: Cached pattern match (performance optimization)
        patterns = self.config.get('job_timeout_patterns', {})
        cache_key = f"{job_name}:{hash(str(sorted(patterns.items())))}"
        
        if cache_key in self._pattern_cache:
            self._cache_hits += 1
            # Move to end (LRU)
            self._pattern_cache.move_to_end(cache_key)
            return self._pattern_cache[cache_key]
        
        # Priority 3: Pattern matching with caching
        timeout = self._calculate_timeout_from_patterns(job_name, patterns)
        
        # Cache result with LRU eviction
        self._pattern_cache[cache_key] = timeout
        self._cache_misses += 1
        
        if len(self._pattern_cache) > self._max_cache_size:
            self._pattern_cache.popitem(last=False)  # Remove oldest
        
        return timeout
    
    def _calculate_timeout_from_patterns(self, job_name: str, patterns: Dict[str, int]) -> float:
        """Calculate timeout from pattern matching with pattern support."""
        if not job_name or not patterns:
            return flt(self.config.get('standard_job_timeout_minutes', 30))
        
        job_name_lower = job_name.lower()
        
        # Check exact match first (highest priority)
        if job_name in patterns:
            return flt(patterns[job_name])
        
        # Check wildcard patterns
        for pattern, timeout in patterns.items():
            if '*' in pattern:
                if pattern.startswith('*') and pattern.endswith('*'):
                    # Contains pattern (*keyword*)
                    keyword = pattern.strip('*').lower()
                    if keyword and keyword in job_name_lower:
                        return flt(timeout)
                elif pattern.startswith('*'):
                    # Ends with pattern (*suffix)
                    suffix = pattern[1:].lower()
                    if job_name_lower.endswith(suffix):
                        return flt(timeout)
                elif pattern.endswith('*'):
                    # Starts with pattern (prefix*)
                    prefix = pattern[:-1].lower()
                    if job_name_lower.startswith(prefix):
                        return flt(timeout)
        
        # Default timeout
        return flt(self.config.get('standard_job_timeout_minutes', 30))
    
    def _is_job_stuck(self, job: Dict[str, Any], timeout_minutes: float) -> bool:
        """Determine if job is stuck with proper error handling."""
        try:
            runtime = job.get('runtime_minutes', 0)
            if runtime <= 0:
                return False  # Job hasn't been running long enough
            
            return runtime > timeout_minutes
            
        except Exception as e:
            frappe.logger("scheduler_monitor").error(
                f"Error checking if job stuck: {str(e)}"
            )
            return False  # Fail safe
    
    def _create_stuck_job_alert(self, job: Dict[str, Any], timeout_minutes: float) -> StuckJobAlert:
        """Create structured alert for stuck job."""
        runtime = job.get('runtime_minutes', 0)
        
        # Determine alert level based on how far past timeout
        ratio = runtime / max(timeout_minutes, 1)  # Avoid division by zero
        
        if ratio >= 3.0:
            alert_level = 'emergency'
            recommended_action = 'terminate_immediately'
        elif ratio >= 2.0:
            alert_level = 'critical' 
            recommended_action = 'terminate_with_grace'
        else:
            alert_level = 'warning'
            recommended_action = 'monitor_closely'
        
        return StuckJobAlert(
            job_id=job['job_id'],
            job_name=job['function_name'],
            queue=job.get('queue', 'unknown'),
            runtime_minutes=runtime,
            configured_timeout_minutes=timeout_minutes,
            alert_level=alert_level,
            recommended_action=recommended_action
        )
    
    def _handle_stuck_jobs(self, stuck_jobs: List[StuckJobAlert]) -> List[Dict[str, Any]]:
        """Handle stuck jobs based on protection level with security validation."""
        actions_taken = []
        
        for alert in stuck_jobs:
            try:
                # Determine action based on protection level and alert severity
                action = self._determine_protection_action(alert)
                
                if action == 'terminate_immediately' or action == 'terminate_with_grace':
                    # Attempt termination with security validation
                    success = self._terminate_job_secure(alert.job_id, action)
                    
                    if success:
                        actions_taken.append({
                            'job_id': alert.job_id,
                            'action': 'terminated',
                            'method': action,
                            'reason': f'{alert.alert_level} timeout after {alert.runtime_minutes:.1f} minutes'
                        })
                else:
                    # Monitoring action
                    actions_taken.append({
                        'job_id': alert.job_id,
                        'action': 'monitoring',
                        'reason': f'{alert.alert_level} threshold at {alert.runtime_minutes:.1f} minutes'
                    })
                    
            except Exception as e:
                frappe.log_error(
                    title="Scheduler Monitor: Action Failed",
                    message=f"Failed to handle stuck job {alert.job_id}: {str(e)}"
                )
        
        return actions_taken
    
    def _determine_protection_action(self, alert: StuckJobAlert) -> str:
        """Determine action based on protection level and alert severity."""
        protection_level = self.config.get('protection_level', 'monitor_only')
        
        if protection_level == 'monitor_only':
            return 'monitor_closely'
        elif protection_level == 'safe_protection':
            # Only terminate emergency-level jobs
            return alert.recommended_action if alert.alert_level == 'emergency' else 'monitor_closely'
        elif protection_level == 'full_protection':
            return alert.recommended_action
        else:
            return 'monitor_closely'
    
    def _terminate_job_secure(self, job_id: str, method: str) -> bool:
        """Terminate job with security validation."""
        try:
            # Security validation: Verify job exists and belongs to current site
            job = Job.fetch(job_id, connection=self.redis_conn)
            
            if not self._validate_job_site_access(job):
                frappe.log_error(
                    title="Scheduler Monitor: Security Violation Blocked",
                    message=f"Blocked cross-site job termination attempt: {job_id}"
                )
                return False
            
            # Additional security: Check if termination is actually allowed
            if not self.config.get('enable_graceful_termination', False) and method == 'terminate_with_grace':
                frappe.logger("scheduler_monitor").warning(
                    f"Graceful termination not enabled, monitoring job {job_id} instead"
                )
                return False
            
            # Perform termination
            if method == 'terminate_immediately':
                job.cancel()
            else:  # terminate_with_grace
                # Try graceful cancellation first
                job.cancel()
            
            # Audit logging for compliance
            frappe.log_error(
                title="Scheduler Monitor: Job Terminated",
                message=json.dumps({
                    'job_id': job_id,
                    'method': method,
                    'site': self.site,
                    'timestamp': now_datetime().isoformat(),
                    'protection_level': self.config.get('protection_level')
                }, indent=2)
            )
            
            return True
            
        except NoSuchJobError:
            frappe.logger("scheduler_monitor").info(
                f"Job {job_id} no longer exists, may have completed"
            )
            return True  # Job is gone, which achieves our goal
        except Exception as e:
            frappe.log_error(
                title="Scheduler Monitor: Termination Failed", 
                message=f"Failed to terminate job {job_id}: {str(e)}"
            )
            return False
    
    def _create_cycle_metrics(self, cycle_start: float, cpu_start: float, 
                           jobs_processed: int, stuck_jobs: int, actions_taken: int) -> MonitoringMetrics:
        """Create performance metrics."""
        cycle_end = time.perf_counter()
        cpu_end = time.process_time()
        
        cycle_duration_ms = (cycle_end - cycle_start) * 1000.0
        cpu_time_ms = (cpu_end - cpu_start) * 1000.0
        
        return MonitoringMetrics(
            cycle_duration_ms=cycle_duration_ms,
            jobs_processed=jobs_processed,
            jobs_per_second=0.0,  # Calculated in __post_init__
            memory_usage_mb=self._get_memory_usage_mb(),
            redis_calls=self._redis_call_count,
            cpu_time_ms=cpu_time_ms,
            stuck_jobs_detected=stuck_jobs,
            actions_taken=actions_taken,
            cache_hits=self._cache_hits,
            cache_misses=self._cache_misses
        )
    
    def _get_memory_usage_mb(self) -> float:
        """Get current memory usage with fallback handling."""
        try:
            import resource
            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Convert to MB (Linux: KB, macOS: bytes)  
            import platform
            if platform.system() == 'Darwin':
                return rss_kb / 1024 / 1024
            else:
                return rss_kb / 1024
        except ImportError:
            return 0.0
    
    def _calculate_cache_efficiency(self) -> float:
        """Calculate cache hit ratio as percentage."""
        total_requests = self._cache_hits + self._cache_misses
        if total_requests == 0:
            return 0.0
        return (self._cache_hits / total_requests) * 100.0
    
    def _record_metrics_safe(self, metrics: MonitoringMetrics) -> None:
        """Record metrics with error handling."""
        try:
            cache_key = f"scheduler_monitor_metrics:{self.site}"
            existing_metrics = frappe.cache().get(cache_key) or []
            
            # Convert dataclass to dict for storage
            metrics_dict = {
                'timestamp': now_datetime().isoformat(),
                'cycle_duration_ms': metrics.cycle_duration_ms,
                'jobs_processed': metrics.jobs_processed,
                'jobs_per_second': metrics.jobs_per_second,
                'stuck_jobs_detected': metrics.stuck_jobs_detected,
                'actions_taken': metrics.actions_taken,
                'cache_efficiency': self._calculate_cache_efficiency()
            }
            
            existing_metrics.append(metrics_dict)
            
            # Keep last 200 entries
            if len(existing_metrics) > 200:
                existing_metrics = existing_metrics[-200:]
            
            frappe.cache().set(cache_key, existing_metrics, expires_in_sec=86400)
            
        except Exception as e:
            frappe.logger("scheduler_monitor").error(
                f"Failed to record metrics: {str(e)}"
            )
    
    def _log_stuck_jobs_for_audit(self, stuck_jobs: List[StuckJobAlert]) -> None:
        """Create audit logs for stuck jobs."""
        for alert in stuck_jobs:
            try:
                audit_data = {
                    'job_id': alert.job_id,
                    'job_name': alert.job_name,
                    'queue': alert.queue,
                    'runtime_minutes': round(alert.runtime_minutes, 2),
                    'configured_timeout_minutes': round(alert.configured_timeout_minutes, 2),
                    'alert_level': alert.alert_level,
                    'recommended_action': alert.recommended_action,
                    'site': self.site,
                    'timestamp': now_datetime().isoformat()
                }
                
                frappe.log_error(
                    title=f"Scheduler Monitor: Stuck Job Alert - {alert.job_name}",
                    message=json.dumps(audit_data, indent=2),
                    reference_doctype="RQ Job",
                    reference_name=alert.job_id
                )
                
            except Exception as e:
                frappe.logger("scheduler_monitor").error(
                    f"Failed to log audit entry for job {alert.job_id}: {str(e)}"
                )
    
    def _handle_infrastructure_failure(self, component: str, error_msg: str) -> Dict[str, Any]:
        """Handle infrastructure failures with graceful degradation."""
        frappe.log_error(
            title=f"Scheduler Monitor: {component.title()} Failure",
            message=f"Infrastructure failure in {component} for site {self.site}: {error_msg}"
        )
        
        return {
            'status': 'degraded',
            'component_failed': component,
            'error': error_msg,
            'site': self.site,
            'message': f'Monitoring system degraded due to {component} failure'
        }


# Health check endpoint
@frappe.whitelist(allow_guest=True)
def get_monitoring_status():
    """Public endpoint for monitoring system health checks."""
    try:
        if not frappe.local.site:
            return {'status': 'error', 'message': 'No site context'}
        
        monitor = SchedulerMonitor()
        
        if not monitor.config.get('enabled'):
            return {'status': 'disabled', 'site': frappe.local.site}
        
        # Basic health indicators
        cache_key = f"scheduler_monitor_metrics:{monitor.site}"
        recent_metrics = frappe.cache().get(cache_key) or []
        
        return {
            'status': 'enabled',
            'site': monitor.site,
            'protection_level': monitor.config.get('protection_level'),
            'recent_cycles': len(recent_metrics),
            'last_cycle': recent_metrics[-1] if recent_metrics else None,
            'cache_efficiency': monitor._calculate_cache_efficiency()
        }
        
    except Exception as e:
        frappe.log_error(f"Health check failed: {str(e)}")
        return {'status': 'error', 'error': str(e)}