"""
Scheduler Monitor Types and Data Classes
========================================

Shared data structures and types for the scheduler monitor system.
Avoids circular imports and provides clean interfaces between components.
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime
from collections import OrderedDict


@dataclass
class StuckJobAlert:
    """Alert for a job that has exceeded its configured timeout."""
    job_id: str
    job_name: str
    queue: str
    runtime_minutes: float
    configured_timeout_minutes: float
    alert_level: str  # warning, critical, emergency
    recommended_action: str  # monitor_closely, terminate_with_grace, terminate_immediately
    
    def __post_init__(self):
        """Validate alert data on creation."""
        if self.runtime_minutes <= 0:
            raise ValueError("Runtime minutes must be positive")
        if self.configured_timeout_minutes <= 0:
            raise ValueError("Configured timeout must be positive")
        if self.alert_level not in ['warning', 'critical', 'emergency']:
            raise ValueError(f"Invalid alert level: {self.alert_level}")


@dataclass 
class MonitoringMetrics:
    """Performance and operational metrics for a monitoring cycle."""
    cycle_duration_ms: float
    jobs_processed: int
    jobs_per_second: float
    memory_usage_mb: float
    redis_calls: int
    cpu_time_ms: float
    stuck_jobs_detected: int
    actions_taken: int
    cache_hits: int
    cache_misses: int
    
    def __post_init__(self):
        """Calculate derived metrics."""
        if self.cycle_duration_ms > 0:
            self.jobs_per_second = (self.jobs_processed / self.cycle_duration_ms) * 1000.0
        else:
            self.jobs_per_second = 0.0


@dataclass
class HealthCheckResult:
    """Result of health checking a set of jobs."""
    total_jobs_checked: int
    healthy_jobs: List[Dict[str, Any]]
    stuck_jobs: List[StuckJobAlert]
    check_duration_ms: float
    errors: List[str]
    
    def __post_init__(self):
        """Validate health check results."""
        if self.total_jobs_checked != len(self.healthy_jobs) + len(self.stuck_jobs):
            # Allow for jobs that couldn't be classified due to errors
            pass


class MonitoringError(Exception):
    """Base exception for scheduler monitoring errors."""
    pass


class ConfigurationError(MonitoringError):
    """Configuration validation or access errors."""
    pass


class SecurityError(MonitoringError):
    """Security validation failures."""
    pass


class RedisConnectionError(MonitoringError):
    """Redis connectivity issues."""
    pass