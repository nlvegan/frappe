"""
Scheduler Metrics Collector
===========================

Component for collecting and managing scheduler monitoring metrics.
Handles performance tracking, caching, and metrics storage.
"""

import time
import resource
import platform
import tracemalloc
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import OrderedDict

import frappe
from frappe.utils import now_datetime, flt

from frappe.utils.scheduler_monitor_types import MonitoringMetrics, SecurityError, MonitoringError


class SchedulerMetricsCollector:
    """
    Component for metrics collection and performance tracking.
    
    Responsibility: Collect, cache, and manage monitoring metrics.
    """
    
    def __init__(self, site: str):
        self.site = site
        self._validate_site_access()
        
        # LRU cache for pattern matching results
        self._pattern_cache = OrderedDict()
        self._max_cache_size = 100  # Prevent unbounded growth
        
        # Performance tracking counters
        self._redis_call_count = 0
        self._cache_hits = 0
        self._cache_misses = 0
        
        # Cache pattern hashes to avoid recalculation
        self._patterns_hash = None
        self._last_patterns = None
        
    def _validate_site_access(self):
        """Validate access to site for security isolation"""
        if not self.site:
            raise SecurityError("Site context required for metrics collection")
        
        # Validate site exists and is accessible
        if frappe.local.site and frappe.local.site != self.site:
            raise SecurityError(f"Cross-site access denied: current={frappe.local.site}, requested={self.site}")
        
    def start_performance_tracking(self) -> Dict[str, Any]:
        """Initialize performance tracking for a monitoring cycle"""
        try:
            return {
                'cycle_start': time.perf_counter(),
                'cpu_start': time.process_time(),
                'redis_calls_start': self._redis_call_count,
                'cache_hits_start': self._cache_hits,
                'cache_misses_start': self._cache_misses,
                'memory_start_mb': self._get_memory_usage_mb()
            }
        except Exception as e:
            frappe.log_error(f"Error starting performance tracking: {str(e)}", "SchedulerMetricsCollector")
            # Return minimal tracking data
            return {
                'cycle_start': time.perf_counter(),
                'cpu_start': time.process_time(),
                'redis_calls_start': 0,
                'cache_hits_start': 0,
                'cache_misses_start': 0,
                'memory_start_mb': 0.0
            }
    
    def create_monitoring_metrics(self, 
                                 tracking_data: Dict[str, Any],
                                 jobs_processed: int,
                                 stuck_jobs_detected: int,
                                 actions_taken: int) -> MonitoringMetrics:
        """
        Create performance metrics from tracking data.
        
        Args:
            tracking_data: Data from start_performance_tracking()
            jobs_processed: Number of jobs analyzed
            stuck_jobs_detected: Number of stuck jobs found
            actions_taken: Number of actions taken
            
        Returns:
            MonitoringMetrics with performance data
        """
        try:
            if not isinstance(tracking_data, dict):
                raise MonitoringError(f"Expected tracking data dict, got {type(tracking_data)}")
                
            cycle_end = time.perf_counter()
            cpu_end = time.process_time()
            
            cycle_duration_ms = (cycle_end - tracking_data.get('cycle_start', cycle_end)) * 1000.0
            cpu_time_ms = (cpu_end - tracking_data.get('cpu_start', cpu_end)) * 1000.0
            
            # Calculate incremental counters
            redis_calls = self._redis_call_count - tracking_data.get('redis_calls_start', 0)
            cache_hits = self._cache_hits - tracking_data.get('cache_hits_start', 0)
            cache_misses = self._cache_misses - tracking_data.get('cache_misses_start', 0)
            
            return MonitoringMetrics(
                cycle_duration_ms=cycle_duration_ms,
                jobs_processed=max(0, jobs_processed),  # Ensure non-negative
                jobs_per_second=0.0,  # Calculated in __post_init__
                memory_usage_mb=self._get_memory_usage_mb(),
                redis_calls=max(0, redis_calls),
                cpu_time_ms=cpu_time_ms,
                stuck_jobs_detected=max(0, stuck_jobs_detected),
                actions_taken=max(0, actions_taken),
                cache_hits=max(0, cache_hits),
                cache_misses=max(0, cache_misses)
            )
            
        except Exception as e:
            frappe.log_error(f"Error creating monitoring metrics: {str(e)}", "SchedulerMetricsCollector")
            # Return minimal valid metrics
            return MonitoringMetrics(
                cycle_duration_ms=0.0,
                jobs_processed=0,
                jobs_per_second=0.0,
                memory_usage_mb=0.0,
                redis_calls=0,
                cpu_time_ms=0.0,
                stuck_jobs_detected=0,
                actions_taken=0,
                cache_hits=0,
                cache_misses=0
            )
    
    def record_metrics(self, metrics: MonitoringMetrics) -> None:
        """Record monitoring metrics for analysis and alerting with error handling"""
        try:
            if not isinstance(metrics, MonitoringMetrics):
                raise MonitoringError(f"Expected MonitoringMetrics, got {type(metrics)}")
                
            cache_key = f"scheduler_monitor_metrics:{self.site}"
            
            # Convert metrics to serializable dict
            metrics_dict = {
                'timestamp': now_datetime().isoformat(),
                'site': self.site,
                'cycle_duration_ms': metrics.cycle_duration_ms,
                'jobs_processed': metrics.jobs_processed,
                'jobs_per_second': metrics.jobs_per_second,
                'memory_usage_mb': metrics.memory_usage_mb,
                'redis_calls': metrics.redis_calls,
                'cpu_time_ms': metrics.cpu_time_ms,
                'stuck_jobs_detected': metrics.stuck_jobs_detected,
                'actions_taken': metrics.actions_taken,
                'cache_hits': metrics.cache_hits,
                'cache_misses': metrics.cache_misses
            }
            
            # Get existing metrics with error handling
            try:
                existing_metrics = frappe.cache().get(cache_key) or []
                if not isinstance(existing_metrics, list):
                    existing_metrics = []
            except Exception as e:
                frappe.log_error(f"Error reading existing metrics: {str(e)}", "SchedulerMetricsCollector")
                existing_metrics = []
            
            existing_metrics.append(metrics_dict)
            
            # Keep only last 200 entries (about 16 hours at 5-minute intervals)
            if len(existing_metrics) > 200:
                existing_metrics = existing_metrics[-200:]
                
            # Store with 24 hour expiration
            try:
                frappe.cache().set(cache_key, existing_metrics, expires_in_sec=86400)
            except Exception as e:
                frappe.log_error(f"Error storing metrics cache: {str(e)}", "SchedulerMetricsCollector")
            
            # Log summary for external monitoring
            if metrics.stuck_jobs_detected > 0:
                frappe.logger("scheduler_monitor").info(
                    f"Monitor cycle: {metrics.stuck_jobs_detected} stuck jobs, "
                    f"{metrics.actions_taken} actions taken, "
                    f"duration: {metrics.cycle_duration_ms:.1f}ms"
                )
            
            # Check performance alerting thresholds
            self._check_performance_alerts(metrics)
                
        except Exception as e:
            frappe.log_error(f"Failed to record metrics: {str(e)}", "SchedulerMetricsCollector")
    
    def get_timeout_with_cache(self, job_name: str, patterns: Dict[str, float]) -> float:
        """
        Get job timeout with LRU caching for performance.
        
        Returns timeout in minutes for the specified job name.
        """
        try:
            if not isinstance(job_name, str):
                job_name = str(job_name)
            
            if not isinstance(patterns, dict):
                return 30.0  # Safe default
            
            # Cache patterns hash to eliminate recalculation
            try:
                if patterns != self._last_patterns:
                    self._patterns_hash = hash(frozenset(patterns.items()))
                    self._last_patterns = patterns.copy()
                cache_key = f"{job_name}:{self._patterns_hash}"
            except TypeError:
                # Patterns dict contains unhashable values
                cache_key = f"{job_name}:unhashable"
            
            if cache_key in self._pattern_cache:
                self._cache_hits += 1
                # Use move_to_end for LRU behavior (single operation)
                self._pattern_cache.move_to_end(cache_key)
                return self._pattern_cache[cache_key]
            
            # Calculate timeout (cache miss)
            timeout_minutes = self._calculate_job_timeout(job_name, patterns)
            self._cache_timeout_result(cache_key, timeout_minutes)
            
            return timeout_minutes
            
        except Exception as e:
            frappe.log_error(f"Error getting cached timeout: {str(e)}", "SchedulerMetricsCollector")
            return 30.0  # Safe default
    
    def _calculate_job_timeout(self, job_name: str, patterns: Dict[str, float]) -> float:
        """Calculate job timeout based on patterns with error handling"""
        try:
            # Check for exact match first
            if job_name in patterns:
                return flt(patterns[job_name])
                
            # Check for wildcard pattern matches
            job_name_lower = job_name.lower()
            for pattern, timeout_minutes in patterns.items():
                try:
                    if not isinstance(pattern, str):
                        continue
                        
                    if pattern.startswith('*') and pattern.endswith('*'):
                        keyword = pattern.strip('*')
                        if keyword and keyword in job_name_lower:
                            return flt(timeout_minutes)
                    elif pattern.startswith('*'):
                        suffix = pattern[1:]
                        if job_name_lower.endswith(suffix):
                            return flt(timeout_minutes)
                    elif pattern.endswith('*'):
                        prefix = pattern[:-1]
                        if job_name_lower.startswith(prefix):
                            return flt(timeout_minutes)
                    elif pattern in job_name_lower:
                        return flt(timeout_minutes)
                except Exception as e:
                    frappe.log_error(f"Error matching pattern {pattern}: {str(e)}", "SchedulerMetricsCollector")
                    continue
                    
            # Default timeout
            return 30.0
            
        except Exception as e:
            frappe.log_error(f"Error calculating job timeout: {str(e)}", "SchedulerMetricsCollector")
            return 30.0  # Safe default
    
    def _cache_timeout_result(self, cache_key: str, result: float) -> None:
        """Cache a timeout result with LRU eviction"""
        try:
            self._pattern_cache[cache_key] = result
            self._cache_misses += 1
            
            # Enforce cache size limit with LRU eviction
            while len(self._pattern_cache) > self._max_cache_size:
                # Remove least recently used item (first item)
                self._pattern_cache.popitem(last=False)
                
        except Exception as e:
            frappe.log_error(f"Error caching timeout result: {str(e)}", "SchedulerMetricsCollector")
    
    def _get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB with fallbacks"""
        try:
            # Try tracemalloc first (most accurate for Python memory)
            if tracemalloc.is_tracing():
                current, peak = tracemalloc.get_traced_memory()
                return current / 1024 / 1024
        except (ImportError, RuntimeError, AttributeError):
            pass
        
        try:
            # Fallback: use resource module for RSS memory
            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            
            # Handle platform differences in RSS reporting
            if platform.system() == 'Darwin':  # macOS reports in bytes
                return rss_kb / 1024 / 1024
            else:  # Linux/Unix reports in KB
                return rss_kb / 1024
                
        except (ImportError, AttributeError, OSError):
            pass
        
        # Final fallback
        return 0.0
    
    def calculate_cache_efficiency(self) -> float:
        """Calculate cache hit ratio as percentage"""
        try:
            total_requests = self._cache_hits + self._cache_misses
            if total_requests == 0:
                return 0.0
            return (self._cache_hits / total_requests) * 100.0
        except Exception:
            return 0.0
    
    def increment_redis_calls(self, count: int = 1) -> None:
        """Track Redis operations for performance monitoring"""
        try:
            if isinstance(count, int) and count > 0:
                self._redis_call_count += count
        except Exception:
            pass  # Non-critical operation
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring system status and recent metrics"""
        try:
            # Check if monitoring is enabled
            try:
                monitoring_enabled = frappe.db.get_single_value("System Settings", "scheduler_monitor_enabled")
            except Exception:
                monitoring_enabled = False
                
            if not monitoring_enabled:
                return {
                    'enabled': False,
                    'site': self.site,
                    'message': 'Scheduler monitoring not enabled'
                }
                
            # Get recent metrics
            cache_key = f"scheduler_monitor_metrics:{self.site}"
            try:
                recent_metrics = frappe.cache().get(cache_key) or []
                if not isinstance(recent_metrics, list):
                    recent_metrics = []
            except Exception as e:
                frappe.log_error(f"Error reading metrics cache: {str(e)}", "SchedulerMetricsCollector")
                recent_metrics = []
            
            # Calculate summary statistics
            total_cycles = len(recent_metrics)
            if total_cycles == 0:
                return {
                    'enabled': True,
                    'site': self.site,
                    'message': 'No monitoring cycles recorded yet',
                    'cache_efficiency': self.calculate_cache_efficiency()
                }
            
            try:
                stuck_jobs_total = sum(m.get('stuck_jobs_detected', 0) for m in recent_metrics)
                actions_total = sum(m.get('actions_taken', 0) for m in recent_metrics)
                avg_duration_ms = sum(m.get('cycle_duration_ms', 0) for m in recent_metrics) / total_cycles
                
                return {
                    'enabled': True,
                    'site': self.site,
                    'total_monitoring_cycles': total_cycles,
                    'stuck_jobs_detected': stuck_jobs_total,
                    'total_actions_taken': actions_total,
                    'average_cycle_duration_ms': round(avg_duration_ms, 3),
                    'last_cycle': recent_metrics[-1] if recent_metrics else None,
                    'cache_efficiency': self.calculate_cache_efficiency(),
                    'cache_size': len(self._pattern_cache),
                    'redis_calls_total': self._redis_call_count
                }
            except Exception as e:
                return {
                    'enabled': True,
                    'site': self.site,
                    'error': f"Error calculating statistics: {str(e)}",
                    'total_cycles': total_cycles
                }
                
        except Exception as e:
            frappe.log_error(f"Error getting monitoring status: {str(e)}", "SchedulerMetricsCollector")
            return {
                'enabled': False,
                'site': self.site,
                'error': f"Status check failed: {str(e)}"
            }
    
    def _check_performance_alerts(self, metrics: MonitoringMetrics):
        """Check for performance issues and generate alerts"""
        try:
            # Alert on low cache efficiency (< 50%)
            cache_efficiency = self.calculate_cache_efficiency()
            if cache_efficiency < 50 and self._cache_hits + self._cache_misses > 10:
                frappe.log_error(
                    title="Scheduler Monitor: Low Cache Efficiency Alert",
                    message=f"Cache efficiency at {cache_efficiency:.1f}% is below 50% threshold. "
                           f"Consider optimizing job timeout patterns. "
                           f"Hits: {self._cache_hits}, Misses: {self._cache_misses}",
                    reference_doctype="System Settings",
                    reference_name="System Settings"
                )
            
            # Alert on excessive cycle time (> 5 seconds)
            if metrics.cycle_duration_ms > 5000:
                frappe.log_error(
                    title="Scheduler Monitor: High Cycle Time Alert",
                    message=f"Monitoring cycle took {metrics.cycle_duration_ms:.0f}ms (>{5000}ms threshold). "
                           f"Jobs processed: {metrics.jobs_processed}. "
                           f"Consider reducing max_jobs_per_cycle or investigating bottlenecks.",
                    reference_doctype="System Settings", 
                    reference_name="System Settings"
                )
            
            # Alert on unexpected memory growth
            if metrics.memory_usage_mb > 100:  # 100MB threshold
                frappe.log_error(
                    title="Scheduler Monitor: High Memory Usage Alert",
                    message=f"Scheduler monitor memory usage at {metrics.memory_usage_mb:.1f}MB. "
                           f"Monitor for potential memory leaks or excessive cache growth.",
                    reference_doctype="System Settings",
                    reference_name="System Settings" 
                )
                
        except Exception as e:
            frappe.log_error(f"Error checking performance alerts: {str(e)}", "SchedulerMetricsCollector")