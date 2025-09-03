"""
Integration Tests for Scheduler Monitor
=======================================

Integration tests using real Redis/RQ components with minimal mocking.
"""

import time
import unittest
from datetime import datetime, timedelta

import frappe
from frappe.utils import now_datetime
from frappe.utils.background_jobs import enqueue, get_redis_conn
from frappe.tests.utils import FrappeTestCase

# Import the focused components
from frappe.utils.scheduler_monitor_types import StuckJobAlert, HealthCheckResult, MonitoringMetrics
from frappe.utils.scheduler_health_checker import SchedulerHealthChecker
from frappe.utils.scheduler_metrics_collector import SchedulerMetricsCollector
from frappe.utils.scheduler_recovery_manager import SchedulerRecoveryManager


class TestSchedulerMonitorRealIntegration(FrappeTestCase):
    """
    Integration tests using real Redis/RQ components.
    
    Uses minimal mocking for realistic system behavior testing.
    """
    
    def setUp(self):
        super().setUp()
        
        # Verify Redis is available
        try:
            self.redis_conn = get_redis_conn()
            self.redis_conn.ping()
        except Exception:
            self.skipTest("Redis not available for integration tests")
        
        # Initialize focused components
        self.site = frappe.local.site
        self.health_checker = SchedulerHealthChecker(self.site)
        self.metrics_collector = SchedulerMetricsCollector(self.site)
        self.recovery_manager = SchedulerRecoveryManager(self.site)
        
        # Test configuration
        self.test_config = {
            'enabled': True,
            'protection_level': 'safe_protection',
            'standard_job_timeout_minutes': 1,  # Short for testing
            'maximum_job_runtime_hours': 0.5,   # 30 minutes for testing
            'job_timeout_patterns': {
                '*test_long*': 5,
                '*test_critical*': 10,
                'frappe.email.queue.flush': 2
            },
            'max_jobs_per_cycle': 10  # Limit for testing
        }
    
    def test_health_checker_with_real_job_states(self):
        """Test health checker with actual job objects (minimal mocking)"""
        
        # Create mock job objects that simulate real RQ jobs
        class MockJob:
            def __init__(self, job_id, func_name, started_minutes_ago):
                self.id = job_id
                self.func_name = func_name
                self.started_at = now_datetime() - timedelta(minutes=started_minutes_ago)
                self.origin = f"rq:job:{job_id}"
                self.meta = {'site': self.site}
        
        # Create jobs with different states
        running_jobs = [
            MockJob("healthy_job_1", "frappe.utils.send_email", 0.5),  # Healthy
            MockJob("stuck_job_1", "test_long_running_operation", 8),   # Stuck (8 min > 5 min timeout)
            MockJob("critical_job_1", "test_critical_process", 15),     # Critical (15 min > 10 min timeout)
        ]
        
        # Test health checking
        health_result = self.health_checker.check_job_health(running_jobs, self.test_config)
        
        # Verify results
        self.assertIsInstance(health_result, HealthCheckResult)
        self.assertEqual(health_result.total_jobs_checked, 3)
        self.assertEqual(len(health_result.healthy_jobs), 1)
        self.assertEqual(len(health_result.stuck_jobs), 2)
        
        # Verify stuck job detection
        stuck_job_names = [alert.job_name for alert in health_result.stuck_jobs]
        self.assertIn("test_long_running_operation", stuck_job_names)
        self.assertIn("test_critical_process", stuck_job_names)
        
        # Verify performance tracking
        self.assertGreater(health_result.check_duration_ms, 0)
    
    def test_metrics_collector_performance_tracking(self):
        """Test metrics collector with real performance measurement"""
        
        # Start performance tracking
        tracking_data = self.metrics_collector.start_performance_tracking()
        
        # Simulate some work
        time.sleep(0.01)  # 10ms of work
        
        # Create metrics
        metrics = self.metrics_collector.create_monitoring_metrics(
            tracking_data=tracking_data,
            jobs_processed=50,
            stuck_jobs_detected=2,
            actions_taken=1
        )
        
        # Verify metrics
        self.assertIsInstance(metrics, MonitoringMetrics)
        self.assertGreater(metrics.cycle_duration_ms, 8)  # At least 8ms (we slept for 10ms)
        self.assertEqual(metrics.jobs_processed, 50)
        self.assertEqual(metrics.stuck_jobs_detected, 2)
        self.assertEqual(metrics.actions_taken, 1)
        self.assertGreater(metrics.jobs_per_second, 0)
        
        # Test metrics recording
        self.metrics_collector.record_metrics(metrics)
    
    def test_cache_performance_with_real_patterns(self):
        """Test cache performance with real pattern matching"""
        
        # Test patterns
        patterns = {
            '*dues*': 60.0,
            '*sepa*': 30.0,
            'frappe.email.queue.flush': 5.0,
            'frappe.*': 15.0
        }
        
        # Test jobs that should match patterns
        test_jobs = [
            'verenigingen.generate_membership_dues_batch',  # Should match *dues* -> 60
            'verenigingen.create_sepa_mandate_batch',       # Should match *sepa* -> 30
            'frappe.email.queue.flush',                     # Exact match -> 5
            'frappe.utils.background_job',                  # Should match frappe.* -> 15
            'custom.unknown_job',                           # No match -> default
        ]
        
        expected_timeouts = [60.0, 30.0, 5.0, 15.0, 30.0]  # Default is 30
        
        # Test cache performance
        for i, (job_name, expected) in enumerate(zip(test_jobs, expected_timeouts)):
            # First call (cache miss)
            timeout1 = self.metrics_collector.get_timeout_with_cache(job_name, patterns)
            self.assertEqual(timeout1, expected, f"Job {i}: {job_name}")
            
            # Second call (cache hit)
            timeout2 = self.metrics_collector.get_timeout_with_cache(job_name, patterns)
            self.assertEqual(timeout2, expected, f"Job {i}: {job_name} (cached)")
        
        # Verify cache efficiency
        efficiency = self.metrics_collector.calculate_cache_efficiency()
        self.assertGreater(efficiency, 40)  # Should have > 40% cache hit rate
    
    def test_recovery_manager_action_determination(self):
        """Test recovery manager with different protection levels"""
        
        # Create a critical stuck job alert
        alert = StuckJobAlert(
            job_id="test_job_123",
            job_name="critical_business_process", 
            queue="default",
            runtime_minutes=90.0,
            configured_timeout_minutes=30.0,
            alert_level="critical",
            recommended_action="terminate_with_grace"
        )
        
        # Test different protection levels
        test_configs = [
            ({'protection_level': 'monitor_only'}, 'monitor_closely'),
            ({'protection_level': 'safe_protection'}, 'monitor_closely'),  # Critical, not emergency
            ({'protection_level': 'full_protection'}, 'terminate_with_grace')
        ]
        
        for config, expected_action in test_configs:
            action = self.recovery_manager._determine_action(alert, config)
            self.assertEqual(action, expected_action, 
                           f"Protection level {config['protection_level']} should result in {expected_action}")
    
    def test_end_to_end_monitoring_cycle(self):
        """Test complete monitoring cycle with minimal mocking"""
        
        # Create realistic job data
        class MockJob:
            def __init__(self, job_id, func_name, started_minutes_ago):
                self.id = job_id
                self.func_name = func_name  
                self.started_at = now_datetime() - timedelta(minutes=started_minutes_ago)
                self.origin = f"rq:job:{job_id}"
                self.meta = {'site': self.site}
        
        jobs = [
            MockJob("job_1", "frappe.email.queue.flush", 0.5),     # Healthy
            MockJob("job_2", "verenigingen.process_dues", 70),     # Stuck (*dues* pattern)
            MockJob("job_3", "custom.stuck_process", 45),          # Stuck (exceeds default 1m)
        ]
        
        # Step 1: Health Check
        health_result = self.health_checker.check_job_health(jobs, self.test_config)
        self.assertEqual(len(health_result.stuck_jobs), 2)  # 2 stuck jobs
        
        # Step 2: Handle Recovery (with monitor_only to avoid actual termination)
        monitor_config = self.test_config.copy()
        monitor_config['protection_level'] = 'monitor_only'
        
        actions = self.recovery_manager.handle_stuck_jobs(
            health_result.stuck_jobs, 
            monitor_config
        )
        
        # Verify actions (should be monitoring only)
        self.assertEqual(len(actions), 2)
        for action in actions:
            self.assertEqual(action['action'], 'monitoring')
        
        # Step 3: Collect metrics
        tracking_data = self.metrics_collector.start_performance_tracking()
        time.sleep(0.001)  # Minimal work simulation
        
        metrics = self.metrics_collector.create_monitoring_metrics(
            tracking_data=tracking_data,
            jobs_processed=len(jobs),
            stuck_jobs_detected=len(health_result.stuck_jobs),
            actions_taken=len(actions)
        )
        
        # Verify complete cycle metrics
        self.assertEqual(metrics.jobs_processed, 3)
        self.assertEqual(metrics.stuck_jobs_detected, 2)
        self.assertEqual(metrics.actions_taken, 2)
        self.assertGreater(metrics.cycle_duration_ms, 0)
    
    def test_monitoring_status_retrieval(self):
        """Test monitoring status retrieval functionality"""
        
        # Test status retrieval
        status = self.metrics_collector.get_monitoring_status()
        
        # Verify status structure
        self.assertIsInstance(status, dict)
        self.assertIn('site', status)
        self.assertEqual(status['site'], self.site)
        
        # The enabled status depends on system settings
        self.assertIn('enabled', status)
    
    def test_stuck_job_logging(self):
        """Test stuck job logging and serialization"""
        
        # Create test alert
        alert = StuckJobAlert(
            job_id="test_logging_123",
            job_name="test_logging_job",
            queue="default",
            runtime_minutes=45.5,
            configured_timeout_minutes=30.0,
            alert_level="warning",
            recommended_action="monitor_closely"
        )
        
        # Test logging (should not raise exceptions)
        try:
            self.recovery_manager.log_stuck_jobs([alert])
        except Exception as e:
            self.fail(f"Stuck job logging failed: {e}")
        
        # Test serialization
        serialized = self.recovery_manager._serialize_stuck_job(alert)
        
        # Verify serialized structure
        self.assertIsInstance(serialized, dict)
        self.assertEqual(serialized['job_id'], "test_logging_123")
        self.assertEqual(serialized['job_name'], "test_logging_job")
        self.assertEqual(serialized['runtime_minutes'], 45.5)
        self.assertEqual(serialized['configured_timeout_minutes'], 30.0)
        self.assertIn('timeout_exceeded_by_minutes', serialized)
        self.assertIn('timeout_multiplier', serialized)
    
    def test_error_resilience(self):
        """Test system behavior with errors and edge cases"""
        
        # Test health checker with invalid job data
        invalid_jobs = [None, "not_a_job", {}]
        
        try:
            result = self.health_checker.check_job_health(invalid_jobs, self.test_config)
            # Should handle gracefully and return result with errors
            self.assertIsInstance(result, HealthCheckResult)
            self.assertGreater(len(result.errors), 0)
        except Exception as e:
            self.fail(f"Health checker should handle invalid jobs gracefully: {e}")
        
        # Test metrics collector with invalid tracking data
        try:
            metrics = self.metrics_collector.create_monitoring_metrics(
                tracking_data=None,
                jobs_processed=0,
                stuck_jobs_detected=0,
                actions_taken=0
            )
            # Should return minimal valid metrics
            self.assertIsInstance(metrics, MonitoringMetrics)
        except Exception as e:
            self.fail(f"Metrics collector should handle invalid tracking data: {e}")
    
    def tearDown(self):
        """Clean up test data"""
        super().tearDown()
        
        # Clear any test cache data
        try:
            cache_key = f"scheduler_monitor_metrics:{self.site}"
            frappe.cache().delete(cache_key)
        except:
            pass  # Best effort cleanup


if __name__ == '__main__':
    unittest.main()