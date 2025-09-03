# Frappe Scheduler Monitor

## Overview

The Frappe Scheduler Monitor is a proactive monitoring and protection system designed to detect and handle stuck jobs in Frappe's RQ-based scheduler before they can impact system reliability.

## Problem Statement

Frappe's scheduler can become unresponsive when individual jobs get stuck or run longer than expected. Common scenarios include:

- Jobs that exceed their configured timeout but continue running
- Background jobs that hang due to external service failures
- Long-running maintenance tasks that block other scheduled operations
- Resource contention causing jobs to run slower than expected

This can lead to:
- Scheduler becoming completely disabled
- Critical maintenance tasks not running (backups, cleanup, etc.)
- System performance degradation
- Manual intervention required to restore scheduler functionality

## Solution Architecture

The Scheduler Monitor builds on Frappe v16's existing RQ infrastructure to provide:

### 1. Proactive Monitoring
- **5-minute monitoring cycles** that inspect all running jobs
- **Integration with existing RQ Job system** for job state tracking
- **Configurable timeout detection** beyond job-level timeouts
- **Multi-level alerting** (warning, critical, emergency)

### 2. Graduated Response System
- **Observe-only mode**: Monitoring and alerting only (default)
- **Conservative mode**: Terminate only emergency-level stuck jobs
- **Active mode**: Full automated recovery including job termination

### 3. Site-Specific Configuration
```json
{
  "scheduler_monitor": {
    "enabled": true,
    "monitor_mode": "observe_only",
    "default_timeout_seconds": 1800,
    "global_max_runtime_minutes": 120,
    "enable_graceful_termination": true,
    "job_timeouts": {
      "frappe.twofactor.delete_all_barcodes_for_users": 3600,
      "frappe.utils.global_search.sync_global_search": 1800,
      "frappe.email.queue.flush": 300
    }
  }
}
```

### 4. Comprehensive Metrics and Alerting
- **Metrics collection** with 24-hour retention
- **Error Log integration** for admin dashboard visibility
- **Structured logging** for external monitoring systems
- **Performance tracking** of monitoring cycles themselves

## Key Features

### Smart Job Detection
- **Configurable per-job timeouts** with pattern matching
- **Global maximum runtime** as safety net
- **Alert escalation** based on severity (1x, 2x, 3x timeout exceeded)
- **False positive reduction** through multiple validation layers

### Safe Recovery Actions
- **Uses existing RQ infrastructure** for job termination
- **Respects monitoring mode** configuration
- **Comprehensive audit trail** of all actions taken
- **Error handling** to prevent monitor failures from affecting scheduler

### Integration Points
- **Scheduled every 5 minutes** via existing cron infrastructure
- **Leverages RQ Job DocType** for job state management
- **Integrates with Frappe's Error Log** system
- **Uses standard site configuration** patterns

## Implementation Details

### Core Components

1. **SchedulerMonitor Class**
   - Main monitoring service
   - Configurable timeout detection
   - Graduated response handling

2. **StuckJobAlert Dataclass**
   - Structured alert information
   - Severity classification
   - Recommended actions

3. **Utility Functions**
   - `run_scheduler_monitoring_cycle()` - Main entry point
   - `is_monitoring_enabled()` - Configuration check
   - `get_monitoring_status()` - System status and metrics

### Configuration Hierarchy

1. **Built-in defaults** for common Frappe jobs
2. **Site configuration** via `site_config.json`
3. **Runtime overrides** for specific environments

### Monitoring Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `observe_only` | Monitor and alert only | Production rollout, initial deployment |
| `conservative` | Terminate only emergency-level jobs | Balanced approach for stable systems |
| `active` | Full automated recovery | High-confidence environments |

## Testing Strategy

### Unit Tests
- Configuration loading and merging
- Job timeout calculation and detection
- Alert classification and escalation
- Recovery action determination

### Integration Tests
- Real RQ job fetching and processing
- Frappe cache integration
- Error logging and metrics collection
- Site configuration integration

### Mock-Based Tests
- Job termination scenarios
- Error handling and recovery
- Different monitoring modes
- Metrics recording and retrieval

## Deployment Considerations

### Backward Compatibility
- **Disabled by default** - requires explicit site configuration
- **No changes to existing scheduler behavior** when disabled
- **Leverages existing RQ infrastructure** - no new dependencies

### Performance Impact
- **Minimal overhead** - 5-minute monitoring cycles
- **Efficient RQ queries** using existing Frappe patterns
- **Capped metrics storage** with automatic cleanup
- **Fail-safe design** - monitor failures don't affect scheduler

### Monitoring and Alerting
- **Error Log integration** for admin visibility
- **Structured logging** for external monitoring systems
- **Metrics API** for dashboard integration
- **Configuration validation** with helpful error messages

## Usage Examples

### Basic Configuration
```json
{
  "scheduler_monitor": {
    "enabled": true
  }
}
```

### Production Configuration
```json
{
  "scheduler_monitor": {
    "enabled": true,
    "monitor_mode": "conservative",
    "global_max_runtime_minutes": 60,
    "job_timeouts": {
      "custom.long_running_report": 7200,
      "custom.data_sync": 3600
    }
  }
}
```

### Checking System Status
```python
from frappe.utils.scheduler_monitor import get_monitoring_status

status = get_monitoring_status()
print(f"Monitor enabled: {status['enabled']}")
print(f"Stuck jobs detected: {status['stuck_jobs_detected']}")
```

## Migration Path

1. **Phase 1**: Deploy with `monitor_mode: "observe_only"`
2. **Phase 2**: Monitor alerts and adjust job timeouts as needed
3. **Phase 3**: Graduate to `monitor_mode: "conservative"`
4. **Phase 4**: Consider `monitor_mode: "active"` for stable environments

## Benefits

### For Site Administrators
- **Proactive alerting** before scheduler failures occur
- **Detailed job runtime insights** for capacity planning
- **Automated recovery** options to reduce manual intervention
- **Configuration flexibility** for different environments

### For Frappe Ecosystem
- **Improved scheduler reliability** across all installations
- **Standardized monitoring patterns** for job management
- **Foundation for future scheduler enhancements**
- **Better visibility** into scheduler performance characteristics

## Related Issues

- [#33335](https://github.com/frappe/frappe/issues/33335) - Scheduler getting disabled due to stuck jobs
- [#32413](https://github.com/frappe/frappe/pull/32413) - Split maintenance tasks into separate queue
- [#32226](https://github.com/frappe/frappe/pull/32226) - LIFO ordering when queue is starved

## Future Enhancements

- **Predictive analytics** for job runtime patterns
- **Resource usage monitoring** for job optimization
- **Integration with system metrics** (CPU, memory, disk)
- **Automated job priority adjustment** based on performance
- **Scheduler health scoring** and trending

---

This implementation provides a robust foundation for scheduler reliability while maintaining backward compatibility and following Frappe's architectural patterns.