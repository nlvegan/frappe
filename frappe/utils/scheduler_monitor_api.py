"""
Scheduler Monitor API Endpoints
==============================

API endpoints for monitoring the scheduler monitor system.
Provides health checks and performance metrics.
"""

import frappe
from frappe.utils import now_datetime
from typing import Dict, Any, List
import time

from frappe.utils.scheduler_monitor import SchedulerMonitor
from frappe.utils.scheduler_monitor_types import SecurityError, MonitoringError


@frappe.whitelist()
def monitor_health() -> Dict[str, Any]:
    """
    Health check endpoint for the scheduler monitoring system.
    
    Returns:
        Dict containing health status, performance metrics, and recommendations
    """
    try:
        # Verify permissions
        if not frappe.has_permission("System Settings", "read"):
            frappe.throw("Insufficient permissions to access scheduler monitor health")
        
        site = frappe.local.site
        
        # Initialize monitor components for health check
        from frappe.utils.scheduler_metrics_collector import SchedulerMetricsCollector
        metrics_collector = SchedulerMetricsCollector(site)
        
        # Get recent monitoring status
        status = metrics_collector.get_monitoring_status()
        
        # Calculate performance indicators
        cache_efficiency = metrics_collector.calculate_cache_efficiency()
        
        # Get recent metrics for trend analysis
        recent_metrics = get_recent_monitoring_metrics(site, limit=10)
        avg_cycle_time_ms = calculate_average_cycle_time(recent_metrics)
        memory_trend = analyze_memory_trend(recent_metrics)
        
        # Health assessment
        health_score = calculate_health_score(cache_efficiency, avg_cycle_time_ms, status)
        
        return {
            "status": "healthy" if health_score >= 80 else "degraded" if health_score >= 60 else "unhealthy",
            "health_score": health_score,
            "timestamp": now_datetime().isoformat(),
            "site": site,
            "performance_metrics": {
                "cache_efficiency_percent": round(cache_efficiency, 1),
                "average_cycle_time_ms": round(avg_cycle_time_ms, 1),
                "memory_usage_trend": memory_trend,
                "cache_size": len(metrics_collector._pattern_cache),
                "redis_calls_total": metrics_collector._redis_call_count
            },
            "monitoring_status": {
                "enabled": status.get("enabled", False),
                "total_cycles": status.get("total_monitoring_cycles", 0),
                "stuck_jobs_detected": status.get("stuck_jobs_detected", 0),
                "actions_taken": status.get("total_actions_taken", 0)
            },
            "alerts": generate_performance_alerts(cache_efficiency, avg_cycle_time_ms, memory_trend),
            "recommendations": generate_health_recommendations(cache_efficiency, avg_cycle_time_ms, status)
        }
        
    except Exception as e:
        frappe.log_error(f"Error in monitor health check: {str(e)}", "SchedulerMonitorAPI")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": now_datetime().isoformat(),
            "site": frappe.local.site
        }


@frappe.whitelist()
def monitor_performance_metrics() -> Dict[str, Any]:
    """
    Detailed performance metrics endpoint for the scheduler monitoring system.
    
    Returns:
        Dict containing detailed performance data and trends
    """
    try:
        if not frappe.has_permission("System Settings", "read"):
            frappe.throw("Insufficient permissions to access performance metrics")
        
        site = frappe.local.site
        
        # Get metrics
        recent_metrics = get_recent_monitoring_metrics(site, limit=50)
        
        return {
            "timestamp": now_datetime().isoformat(),
            "site": site,
            "metrics_summary": {
                "total_data_points": len(recent_metrics),
                "time_range_hours": calculate_time_range_hours(recent_metrics),
                "average_cycle_time_ms": calculate_average_cycle_time(recent_metrics),
                "peak_cycle_time_ms": calculate_peak_cycle_time(recent_metrics),
                "total_jobs_processed": sum(m.get("jobs_processed", 0) for m in recent_metrics),
                "total_stuck_jobs": sum(m.get("stuck_jobs_detected", 0) for m in recent_metrics),
                "total_actions_taken": sum(m.get("actions_taken", 0) for m in recent_metrics)
            },
            "performance_trends": {
                "cycle_time_trend": analyze_cycle_time_trend(recent_metrics),
                "memory_trend": analyze_memory_trend(recent_metrics),
                "job_processing_trend": analyze_job_processing_trend(recent_metrics)
            },
            "recent_metrics": recent_metrics[-10:]  # Last 10 data points
        }
        
    except Exception as e:
        frappe.log_error(f"Error getting performance metrics: {str(e)}", "SchedulerMonitorAPI")
        return {
            "error": str(e),
            "timestamp": now_datetime().isoformat()
        }


def get_recent_monitoring_metrics(site: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent monitoring metrics from cache"""
    try:
        cache_key = f"scheduler_monitor_metrics:{site}"
        metrics = frappe.cache().get(cache_key) or []
        return metrics[-limit:] if isinstance(metrics, list) else []
    except Exception:
        return []


def calculate_average_cycle_time(metrics: List[Dict[str, Any]]) -> float:
    """Calculate average cycle time from metrics"""
    if not metrics:
        return 0.0
    
    cycle_times = [m.get("cycle_duration_ms", 0) for m in metrics]
    valid_times = [t for t in cycle_times if t > 0]
    
    return sum(valid_times) / len(valid_times) if valid_times else 0.0


def calculate_peak_cycle_time(metrics: List[Dict[str, Any]]) -> float:
    """Calculate peak cycle time from metrics"""
    if not metrics:
        return 0.0
    
    cycle_times = [m.get("cycle_duration_ms", 0) for m in metrics]
    return max(cycle_times) if cycle_times else 0.0


def calculate_time_range_hours(metrics: List[Dict[str, Any]]) -> float:
    """Calculate time range covered by metrics in hours"""
    if len(metrics) < 2:
        return 0.0
    
    try:
        from dateutil import parser
        first_time = parser.parse(metrics[0].get("timestamp", ""))
        last_time = parser.parse(metrics[-1].get("timestamp", ""))
        return (last_time - first_time).total_seconds() / 3600
    except Exception:
        return 0.0


def analyze_memory_trend(metrics: List[Dict[str, Any]]) -> str:
    """Analyze memory usage trend"""
    if len(metrics) < 3:
        return "insufficient_data"
    
    memory_usage = [m.get("memory_usage_mb", 0) for m in metrics[-5:]]
    
    # Simple trend analysis
    increasing = sum(1 for i in range(1, len(memory_usage)) if memory_usage[i] > memory_usage[i-1])
    
    if increasing >= len(memory_usage) - 1:
        return "increasing"
    elif increasing <= 1:
        return "decreasing"
    else:
        return "stable"


def analyze_cycle_time_trend(metrics: List[Dict[str, Any]]) -> str:
    """Analyze cycle time trend"""
    if len(metrics) < 3:
        return "insufficient_data"
    
    cycle_times = [m.get("cycle_duration_ms", 0) for m in metrics[-5:]]
    
    # Calculate trend
    avg_first_half = sum(cycle_times[:len(cycle_times)//2]) / (len(cycle_times)//2)
    avg_second_half = sum(cycle_times[len(cycle_times)//2:]) / (len(cycle_times) - len(cycle_times)//2)
    
    if avg_second_half > avg_first_half * 1.2:
        return "degrading"
    elif avg_second_half < avg_first_half * 0.8:
        return "improving"
    else:
        return "stable"


def analyze_job_processing_trend(metrics: List[Dict[str, Any]]) -> str:
    """Analyze job processing volume trend"""
    if len(metrics) < 3:
        return "insufficient_data"
    
    job_counts = [m.get("jobs_processed", 0) for m in metrics[-5:]]
    avg_jobs = sum(job_counts) / len(job_counts)
    
    if avg_jobs > 50:
        return "high_volume"
    elif avg_jobs > 20:
        return "moderate_volume"
    else:
        return "low_volume"


def calculate_health_score(cache_efficiency: float, avg_cycle_time_ms: float, status: Dict[str, Any]) -> int:
    """Calculate overall health score (0-100)"""
    score = 100
    
    # Cache efficiency scoring (30 points)
    if cache_efficiency < 30:
        score -= 20
    elif cache_efficiency < 50:
        score -= 10
    elif cache_efficiency < 70:
        score -= 5
    
    # Cycle time scoring (30 points)
    if avg_cycle_time_ms > 10000:  # > 10 seconds
        score -= 20
    elif avg_cycle_time_ms > 5000:  # > 5 seconds
        score -= 10
    elif avg_cycle_time_ms > 2000:  # > 2 seconds
        score -= 5
    
    # System status scoring (40 points)
    if not status.get("enabled", True):
        score -= 40
    elif status.get("error"):
        score -= 20
    
    return max(0, min(100, score))


def generate_performance_alerts(cache_efficiency: float, avg_cycle_time_ms: float, memory_trend: str) -> List[Dict[str, str]]:
    """Generate performance alerts based on Carmack's recommendations"""
    alerts = []
    
    # Carmack's alert thresholds
    if cache_efficiency < 50:
        alerts.append({
            "level": "warning",
            "message": f"Cache efficiency at {cache_efficiency:.1f}% is below recommended 50% threshold",
            "recommendation": "Review job pattern configuration or increase cache size"
        })
    
    if avg_cycle_time_ms > 5000:
        alerts.append({
            "level": "critical", 
            "message": f"Average cycle time {avg_cycle_time_ms:.0f}ms exceeds 5 second threshold",
            "recommendation": "Investigate job processing bottlenecks or reduce max_jobs_per_cycle"
        })
    
    if memory_trend == "increasing":
        alerts.append({
            "level": "warning",
            "message": "Memory usage trend is increasing",
            "recommendation": "Monitor for potential memory leaks in job processing"
        })
    
    return alerts


def generate_health_recommendations(cache_efficiency: float, avg_cycle_time_ms: float, status: Dict[str, Any]) -> List[str]:
    """Generate health improvement recommendations"""
    recommendations = []
    
    if cache_efficiency < 70:
        recommendations.append("Consider optimizing job timeout patterns to improve cache hit rate")
    
    if avg_cycle_time_ms > 2000:
        recommendations.append("Review max_jobs_per_cycle setting to optimize processing time")
    
    if status.get("stuck_jobs_detected", 0) > 0:
        recommendations.append("Review job timeout configurations for frequently stuck jobs")
    
    total_cycles = status.get("total_monitoring_cycles", 0)
    if total_cycles < 10:
        recommendations.append("Allow more monitoring cycles to accumulate for better trend analysis")
    
    return recommendations