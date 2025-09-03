"""
Core Logic Tests for Scheduler Monitor
======================================

Tests scheduler monitor algorithms with minimal mocking.
Focuses on business logic validation.
"""

import unittest
from datetime import datetime, timedelta
from collections import OrderedDict

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import now_datetime, add_to_date

from frappe.utils.scheduler_monitor_types import StuckJobAlert, MonitoringMetrics, HealthCheckResult
from frappe.utils.scheduler_health_checker import SchedulerHealthChecker
from frappe.utils.scheduler_metrics_collector import SchedulerMetricsCollector
from frappe.utils.scheduler_recovery_manager import SchedulerRecoveryManager


class TestSchedulerMonitorCoreLogic(FrappeTestCase):
    """
    Algorithm testing with minimal mocking.
    Tests core business logic.
    """
    
    def setUp(self):
        super().setUp()
        self.site = frappe.local.site
        self.test_config = {
            'enabled': True,
            'protection_level': 'safe_protection',
            'standard_job_timeout_minutes': 30,
            'maximum_job_runtime_hours': 2.0,
            'job_timeout_patterns': {
                '*dues*': 60,
                '*sepa*': 30,
                'frappe.email.queue.flush': 5,
                'frappe.*': 15,
                'verenigingen.*': 45
            }
        }
    
    def test_timeout_calculation_algorithm(self):
        """Test the core timeout calculation algorithm directly"""
        metrics_collector = SchedulerMetricsCollector(self.site)
        
        # Test exact pattern matches
        timeout = metrics_collector.get_timeout_with_cache('frappe.email.queue.flush', self.test_config['job_timeout_patterns'])
        self.assertEqual(timeout, 5)
        
        # Test wildcard pattern matches
        timeout = metrics_collector.get_timeout_with_cache('verenigingen.process_membership_dues_batch', self.test_config['job_timeout_patterns'])
        self.assertEqual(timeout, 60)  # Should match *dues* pattern
        
        timeout = metrics_collector.get_timeout_with_cache('verenigingen.create_sepa_mandate_batch', self.test_config['job_timeout_patterns'])
        self.assertEqual(timeout, 30)  # Should match *sepa* pattern
        
        # Test prefix patterns
        timeout = metrics_collector.get_timeout_with_cache('frappe.utils.background_job', self.test_config['job_timeout_patterns'])
        self.assertEqual(timeout, 15)  # Should match frappe.* pattern
        
        # Test default timeout
        timeout = metrics_collector.get_timeout_with_cache('unknown.custom_job', self.test_config['job_timeout_patterns'])
        self.assertEqual(timeout, 30)  # Should use safe default
    
    def test_lru_cache_behavior(self):
        """Test LRU cache implementation directly"""
        metrics_collector = SchedulerMetricsCollector(self.site)
        metrics_collector._max_cache_size = 3  # Small cache for testing
        
        # Fill cache
        metrics_collector.get_timeout_with_cache('job1', {'job1': 10})
        metrics_collector.get_timeout_with_cache('job2', {'job2': 20})
        metrics_collector.get_timeout_with_cache('job3', {'job3': 30})
        
        # Verify cache size limit
        self.assertLessEqual(len(metrics_collector._pattern_cache), 3)
        
        # Access job1 to make it recently used
        metrics_collector.get_timeout_with_cache('job1', {'job1': 10})
        
        # Add job4, should evict least recently used (job2 or job3, not job1)
        metrics_collector.get_timeout_with_cache('job4', {'job4': 40})
        
        # Verify cache efficiency calculation
        efficiency = metrics_collector.calculate_cache_efficiency()
        self.assertGreater(efficiency, 0)  # Should have some cache hits
    
    def test_stuck_job_detection_algorithm(self):
        """Test stuck job detection logic directly"""
        health_checker = SchedulerHealthChecker(self.site)
        
        # Create mock job that represents RQ job interface
        class MockJob:
            def __init__(self, job_id, func_name, started_minutes_ago):
                self.id = job_id
                self.func_name = func_name
                self.started_at = now_datetime() - timedelta(minutes=started_minutes_ago)
                self.meta = {'site': frappe.local.site}
        
        # Test normal job (not stuck)
        normal_job = MockJob('job1', 'frappe.email.queue.flush', 2)  # 2 min runtime, 5 min timeout
        self.assertFalse(health_checker._is_job_stuck(normal_job, self.test_config))
        
        # Test stuck job
        stuck_job = MockJob('job2', 'frappe.email.queue.flush', 10)  # 10 min runtime, 5 min timeout
        self.assertTrue(health_checker._is_job_stuck(stuck_job, self.test_config))
        
        # Test job exceeding global maximum
        very_long_job = MockJob('job3', 'some.quick.job', 150)  # 150 min runtime, exceeds 2 hour max
        self.assertTrue(health_checker._is_job_stuck(very_long_job, self.test_config))
    
    def test_alert_level_determination(self):
        """Test alert level calculation algorithm"""
        health_checker = SchedulerHealthChecker(self.site)
        
        class MockJob:
            def __init__(self, job_id, func_name, started_minutes_ago):
                self.id = job_id
                self.func_name = func_name
                self.started_at = now_datetime() - timedelta(minutes=started_minutes_ago)
                self.meta = {'site': frappe.local.site}
        
        # Test warning level (1.5x timeout)
        job = MockJob('job1', 'frappe.email.queue.flush', 7.5)  # 7.5 min runtime, 5 min timeout = 1.5x
        alert = health_checker._create_stuck_job_alert(job, self.test_config)
        self.assertEqual(alert.alert_level, 'warning')
        self.assertEqual(alert.recommended_action, 'monitor_closely')
        
        # Test critical level (2.5x timeout)
        job = MockJob('job2', 'frappe.email.queue.flush', 12.5)  # 12.5 min runtime, 5 min timeout = 2.5x
        alert = health_checker._create_stuck_job_alert(job, self.test_config)
        self.assertEqual(alert.alert_level, 'critical')
        self.assertEqual(alert.recommended_action, 'terminate_with_grace')
        
        # Test emergency level (3+ timeout)
        job = MockJob('job3', 'frappe.email.queue.flush', 20)  # 20 min runtime, 5 min timeout = 4x
        alert = health_checker._create_stuck_job_alert(job, self.test_config)
        self.assertEqual(alert.alert_level, 'emergency')
        self.assertEqual(alert.recommended_action, 'terminate_immediately')
    
    def test_protection_level_logic(self):
        """Test protection level decision logic"""
        recovery_manager = SchedulerRecoveryManager(self.site)
        
        # Create test alert
        alert = StuckJobAlert(
            job_id='test123',
            job_name='test_job',
            queue='default',
            runtime_minutes=90.0,
            configured_timeout_minutes=30.0,
            alert_level='critical',
            recommended_action='terminate_with_grace'
        )
        
        # Test monitor_only level
        config = {'protection_level': 'monitor_only'}
        action = recovery_manager._determine_action(alert, config)
        self.assertEqual(action, 'monitor_closely')
        
        # Test safe_protection level with critical alert
        config = {'protection_level': 'safe_protection'}
        action = recovery_manager._determine_action(alert, config)
        self.assertEqual(action, 'monitor_closely')  # Critical, not emergency
        
        # Test safe_protection level with emergency alert
        alert.alert_level = 'emergency'
        alert.recommended_action = 'terminate_immediately'
        action = recovery_manager._determine_action(alert, config)
        self.assertEqual(action, 'terminate_immediately')  # Emergency allowed
        
        # Test full_protection level
        config = {'protection_level': 'full_protection'}
        action = recovery_manager._determine_action(alert, config)
        self.assertEqual(action, 'terminate_immediately')  # Follows recommendation
    
    def test_monitoring_metrics_calculation(self):
        """Test monitoring metrics calculation directly"""
        metrics_collector = SchedulerMetricsCollector(self.site)
        
        # Start tracking
        tracking_data = metrics_collector.start_performance_tracking()
        
        # Simulate work
        import time
        time.sleep(0.001)  # 1ms of work
        
        # Create metrics
        metrics = metrics_collector.create_monitoring_metrics(
            tracking_data=tracking_data,
            jobs_processed=10,
            stuck_jobs_detected=2,
            actions_taken=1
        )
        
        # Verify metrics structure
        self.assertIsInstance(metrics, MonitoringMetrics)
        self.assertEqual(metrics.jobs_processed, 10)
        self.assertEqual(metrics.stuck_jobs_detected, 2)
        self.assertEqual(metrics.actions_taken, 1)
        self.assertGreater(metrics.cycle_duration_ms, 0)
        self.assertGreater(metrics.jobs_per_second, 0)  # Calculated in __post_init__
    
    def test_health_check_result_structure(self):
        """Test health check result data structure"""
        health_checker = SchedulerHealthChecker(self.site)
        
        class MockJob:
            def __init__(self, job_id, func_name, started_minutes_ago):
                self.id = job_id
                self.func_name = func_name
                self.started_at = now_datetime() - timedelta(minutes=started_minutes_ago)
                self.meta = {'site': frappe.local.site}
        
        jobs = [
            MockJob('healthy1', 'frappe.email.queue.flush', 2),  # Healthy
            MockJob('stuck1', 'frappe.email.queue.flush', 10),   # Stuck
            MockJob('healthy2', 'verenigingen.process_dues', 30), # Healthy (60 min timeout)
            MockJob('stuck2', 'verenigingen.process_dues', 90),   # Stuck
        ]
        
        result = health_checker.check_job_health(jobs, self.test_config)
        
        # Verify result structure
        self.assertIsInstance(result, HealthCheckResult)
        self.assertEqual(result.total_jobs_checked, 4)
        self.assertEqual(len(result.healthy_jobs), 2)
        self.assertEqual(len(result.stuck_jobs), 2)
        self.assertGreater(result.check_duration_ms, 0)
        self.assertIsInstance(result.errors, list)
        
        # Verify stuck jobs have correct alert levels
        for stuck_alert in result.stuck_jobs:
            self.assertIsInstance(stuck_alert, StuckJobAlert)
            self.assertIn(stuck_alert.alert_level, ['warning', 'critical', 'emergency'])
    
    def test_security_validation_logic(self):
        """Test cross-site security validation"""
        # Test with valid site
        health_checker = SchedulerHealthChecker(self.site)
        
        class MockJob:
            def __init__(self, job_id, site):
                self.id = job_id
                self.meta = {'site': site}
        
        # Valid job for current site
        valid_job = MockJob('job1', self.site)
        try:
            health_checker._validate_job_site_access(valid_job)
            # Should not raise exception
        except Exception as e:
            self.fail(f"Valid job validation failed: {e}")
        
        # Invalid job for different site
        invalid_job = MockJob('job2', 'different_site')
        with self.assertRaises(Exception):
            health_checker._validate_job_site_access(invalid_job)
    
    def test_pattern_matching_edge_cases(self):
        """Test pattern matching edge cases"""
        metrics_collector = SchedulerMetricsCollector(self.site)
        
        patterns = {
            '*dues*': 60,
            'frappe.*': 15,
            '*_report': 20,
            'exact.job.name': 10
        }
        
        # Test case sensitivity
        timeout = metrics_collector.get_timeout_with_cache('MEMBERSHIP_DUES_PROCESS', patterns)
        self.assertEqual(timeout, 60)  # Should match *dues* despite case
        
        # Test multiple pattern matches (first match wins)
        timeout = metrics_collector.get_timeout_with_cache('frappe.dues.processor', patterns)
        self.assertEqual(timeout, 15)  # frappe.* should match before *dues*
        
        # Test suffix patterns
        timeout = metrics_collector.get_timeout_with_cache('financial_report', patterns)
        self.assertEqual(timeout, 20)  # Should match *_report
        
        # Test exact matches take priority
        timeout = metrics_collector.get_timeout_with_cache('exact.job.name', patterns)
        self.assertEqual(timeout, 10)  # Exact match
    
    def test_error_handling_resilience(self):
        """Test system behavior with invalid inputs"""
        metrics_collector = SchedulerMetricsCollector(self.site)
        
        # Test with invalid patterns
        timeout = metrics_collector.get_timeout_with_cache('some.job', None)
        self.assertEqual(timeout, 30.0)  # Should return default
        
        timeout = metrics_collector.get_timeout_with_cache('some.job', {})
        self.assertEqual(timeout, 30.0)  # Should return default
        
        # Test with invalid job names
        timeout = metrics_collector.get_timeout_with_cache(None, {'job': 10})
        self.assertEqual(timeout, 30.0)  # Should handle gracefully
        
        timeout = metrics_collector.get_timeout_with_cache('', {'job': 10})
        self.assertEqual(timeout, 30.0)  # Should handle gracefully


if __name__ == '__main__':
    unittest.main()