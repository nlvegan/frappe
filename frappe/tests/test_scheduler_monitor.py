"""
Test suite for Frappe Scheduler Monitor

Tests the monitoring and protection capabilities for stuck jobs
in the Frappe scheduler system.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
from unittest.mock import Mock, patch, MagicMock

import frappe
from frappe.tests import IntegrationTestCase
from frappe.utils import now_datetime, add_to_date
from frappe.core.doctype.rq_job.rq_job import get_all_queued_jobs
from frappe.utils.scheduler_monitor import (
    SchedulerMonitor,
    StuckJobAlert,
    run_scheduler_monitoring_cycle,
    is_monitoring_enabled,
    get_monitoring_status
)


class TestSchedulerMonitor(IntegrationTestCase):
    def setUp(self):
        super().setUp()
        self.monitor = SchedulerMonitor("test_site")
        
        # Clear any existing cache
        cache_key = f"scheduler_monitor_metrics:test_site"
        frappe.cache().delete(cache_key)
        
        # Mock site config for testing with business-friendly language
        self.test_config = {
            'enabled': True,
            'protection_level': 'monitor_only',
            'standard_job_timeout_minutes': 5,  # 5 minutes for testing
            'maximum_job_runtime_hours': 0.5,  # 30 minutes for testing
            'enable_graceful_termination': True,
            'job_timeout_patterns': {
                'test.long_running_job': 10,  # 10 minutes
                'test.quick_job': 1,  # 1 minute
                '*dues*': 60,  # Test the pattern matching
                'frappe.twofactor.delete_all_barcodes_for_users': 30  # 30 minutes
            }
        }
        
    def tearDown(self):
        super().tearDown()
        # Clean up cache
        cache_key = f"scheduler_monitor_metrics:test_site"
        frappe.cache().delete(cache_key)
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    @patch('frappe.core.doctype.rq_job.rq_job.get_all_queued_jobs')
    def test_performance_limit_large_job_queue(self, mock_get_jobs, mock_config):
        """Test that performance limits are enforced with large job queues"""
        mock_config.return_value = {
            'enabled': True,
            'max_jobs_per_cycle': 50,  # Low limit for testing  
            'default_timeout_seconds': 300
        }
        
        # Create 1000 mock jobs (exceeds limit)
        large_job_queue = []
        for i in range(1000):
            job = Mock()
            job.id = f"job_{i}"
            job.get_status.return_value = "started" if i < 50 else "queued"
            job.started_at = now_datetime() - timedelta(minutes=10)
            large_job_queue.append(job)
        
        mock_get_jobs.return_value = large_job_queue
        
        result = self.monitor.run_monitoring_cycle()
        
        # Should complete successfully despite large queue
        self.assertEqual(result['status'], 'completed')
        
        # Should have limited the jobs checked
        self.assertLessEqual(result['metrics']['total_running_jobs'], 50)  # Max running jobs found
        
    @patch.object(SchedulerMonitor, '_get_monitor_config') 
    @patch('frappe.core.doctype.rq_job.rq_job.RQJob')
    @patch('frappe.core.doctype.rq_job.rq_job.stop_job')
    def test_job_termination_security_validation(self, mock_stop_job, mock_rq_job_class, mock_config):
        """Test that job termination includes proper security validation"""
        mock_config.return_value = {
            'enabled': True,
            'protection_level': 'full_protection',
            'standard_job_timeout_minutes': 5
        }
        
        # Mock RQJob instance
        mock_rq_job = Mock()
        mock_rq_job.job.kwargs = {'site': 'test_site'}  # Same site - should allow
        mock_rq_job_class.return_value = mock_rq_job
        
        # Test successful termination with proper site
        result = self.monitor._terminate_job("valid_job_123")
        self.assertTrue(result)
        mock_stop_job.assert_called_once_with("valid_job_123")
        
        # Reset mocks
        mock_stop_job.reset_mock()
        
        # Test blocked termination with different site
        mock_rq_job.job.kwargs = {'site': 'different_site'}
        result = self.monitor._terminate_job("invalid_job_456")
        self.assertFalse(result)
        mock_stop_job.assert_not_called()  # Should not call stop_job
        
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    @patch('frappe.core.doctype.rq_job.rq_job.get_all_queued_jobs')
    def test_type_safety_invalid_job_objects(self, mock_get_jobs, mock_config):
        """Test resilience to invalid job objects without expected methods"""
        mock_config.return_value = {
            'enabled': True,
            'default_timeout_seconds': 300
        }
        
        # Mix of valid and invalid job objects
        mixed_job_queue = [
            Mock(spec=['get_status', 'id']),  # Valid job object
            {'invalid': 'dict_object'},      # Invalid - dict instead of job object  
            Mock(spec=['id']),               # Invalid - missing get_status method
            None,                            # Invalid - None object
        ]
        
        # Set up the valid job
        mixed_job_queue[0].get_status.return_value = "started"
        mixed_job_queue[0].id = "valid_job"
        mixed_job_queue[0].started_at = now_datetime() - timedelta(minutes=10)
        
        mock_get_jobs.return_value = mixed_job_queue
        
        # Should handle invalid objects gracefully
        result = self.monitor.run_monitoring_cycle()
        
        self.assertEqual(result['status'], 'completed')
        # Should only process the 1 valid job
        self.assertEqual(result['metrics']['total_running_jobs'], 1)
    
    def test_configuration_security_limits(self):
        """Test that configuration validation applies reasonable security limits"""
        monitor = SchedulerMonitor("test_site")
        
        # Test excessive job limit gets capped
        dangerous_config = {
            'max_jobs_per_cycle': 10000,  # Excessive
            'standard_job_timeout_minutes': 0.1,  # Too low (6 seconds)
            'protection_level': 'invalid_mode'  # Invalid
        }
        
        # Apply security limits
        safe_config = monitor._apply_security_limits(dangerous_config)
        
        # Should cap excessive values
        self.assertLessEqual(safe_config['max_jobs_per_cycle'], 500)
        self.assertGreaterEqual(safe_config['standard_job_timeout_minutes'], 1)
        self.assertEqual(safe_config['protection_level'], 'monitor_only')
    
    def test_full_protection_mode_audit_logging(self):
        """Test that full protection mode activation is properly audited"""
        monitor = SchedulerMonitor("test_site")
        
        with patch('frappe.log_error') as mock_log_error:
            # Enable full protection mode
            config = {
                'protection_level': 'full_protection',
                'require_admin_approval_for_termination': True
            }
            
            # Apply security validation
            monitor._apply_security_limits(config)
            
            # Should create audit log for active mode
            mock_log_error.assert_called()
            call_args = mock_log_error.call_args
            self.assertIn("Active Mode Enabled", call_args[1]['title'])
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    def test_monitoring_disabled_by_default(self, mock_config):
        """Test that monitoring is disabled by default"""
        mock_config.return_value = {'enabled': False}
        
        result = self.monitor.run_monitoring_cycle()
        
        self.assertEqual(result['status'], 'disabled')
        self.assertIn('disabled', result['message'])
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    @patch.object(SchedulerMonitor, '_get_running_jobs')
    def test_monitoring_cycle_no_stuck_jobs(self, mock_get_jobs, mock_config):
        """Test successful monitoring cycle with no stuck jobs"""
        mock_config.return_value = self.test_config
        mock_get_jobs.return_value = []
        
        result = self.monitor.run_monitoring_cycle()
        
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['metrics']['stuck_jobs_detected'], 0)
        self.assertEqual(result['metrics']['actions_taken'], 0)
        self.assertGreater(result['metrics']['cycle_duration_seconds'], 0)
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    @patch.object(SchedulerMonitor, '_get_running_jobs')
    def test_stuck_job_detection(self, mock_get_jobs, mock_config):
        """Test detection of stuck jobs"""
        mock_config.return_value = self.test_config
        
        # Create a mock stuck job
        stuck_job = Mock()
        stuck_job.id = "test_job_123"
        stuck_job.func_name = "test.long_running_job"
        stuck_job.started_at = add_to_date(now_datetime(), minutes=-15)  # Running for 15 minutes
        stuck_job.get_status.return_value = "started"
        
        mock_get_jobs.return_value = [stuck_job]
        
        result = self.monitor.run_monitoring_cycle()
        
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['metrics']['stuck_jobs_detected'], 1)
        self.assertEqual(len(result['stuck_jobs']), 1)
        
        stuck_job_info = result['stuck_jobs'][0]
        self.assertEqual(stuck_job_info['job_id'], 'test_job_123')
        self.assertEqual(stuck_job_info['job_name'], 'test.long_running_job')
        self.assertEqual(stuck_job_info['alert_level'], 'warning')  # 15m > 10m configured timeout
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    @patch.object(SchedulerMonitor, '_get_running_jobs')
    def test_critical_stuck_job_detection(self, mock_get_jobs, mock_config):
        """Test detection of critically stuck jobs"""
        mock_config.return_value = self.test_config
        
        # Create a mock critically stuck job (running 3x longer than timeout)
        stuck_job = Mock()
        stuck_job.id = "critical_job_456"
        stuck_job.func_name = "test.quick_job"  # 1 minute timeout
        stuck_job.started_at = add_to_date(now_datetime(), minutes=-4)  # Running for 4 minutes
        stuck_job.get_status.return_value = "started"
        
        mock_get_jobs.return_value = [stuck_job]
        
        result = self.monitor.run_monitoring_cycle()
        
        stuck_job_info = result['stuck_jobs'][0]
        self.assertEqual(stuck_job_info['alert_level'], 'emergency')  # 4m > 3x 1m timeout
        self.assertEqual(stuck_job_info['recommended_action'], 'terminate_immediately')
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    def test_job_timeout_resolution(self, mock_config):
        """Test job timeout resolution with various patterns"""
        mock_config.return_value = self.test_config
        
        # Test exact match
        timeout = self.monitor._get_job_timeout_minutes("test.long_running_job", self.test_config)
        self.assertEqual(timeout, 10.0)  # 600 seconds / 60
        
        # Test partial match
        timeout = self.monitor._get_job_timeout_minutes("frappe.twofactor.delete_all_barcodes_for_users", self.test_config)
        self.assertEqual(timeout, 30.0)  # 1800 seconds / 60
        
        # Test default fallback
        timeout = self.monitor._get_job_timeout_minutes("unknown.job", self.test_config)
        self.assertEqual(timeout, 5.0)  # 300 seconds / 60 (default)
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    @patch('frappe.utils.scheduler_monitor.stop_job')
    def test_job_termination_active_mode(self, mock_stop_job, mock_config):
        """Test job termination in full protection mode"""
        config = self.test_config.copy()
        config['protection_level'] = 'full_protection'
        mock_config.return_value = config
        
        # Create emergency-level stuck job alert
        alert = StuckJobAlert(
            job_id="emergency_job",
            job_name="test.stuck_job",
            queue="default",
            runtime_minutes=30.0,
            configured_timeout_minutes=5.0,
            alert_level="emergency",
            recommended_action="terminate_immediately"
        )
        
        actions = self.monitor._handle_stuck_jobs([alert])
        
        mock_stop_job.assert_called_once_with("emergency_job")
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]['action'], 'terminated')
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    @patch('frappe.utils.scheduler_monitor.stop_job')
    def test_job_termination_monitor_only_mode(self, mock_stop_job, mock_config):
        """Test that jobs are not terminated in monitor-only mode"""
        config = self.test_config.copy()
        config['protection_level'] = 'monitor_only'
        mock_config.return_value = config
        
        # Create emergency-level stuck job alert
        alert = StuckJobAlert(
            job_id="emergency_job",
            job_name="test.stuck_job",
            queue="default",
            runtime_minutes=30.0,
            configured_timeout_minutes=5.0,
            alert_level="emergency",
            recommended_action="terminate_immediately"
        )
        
        actions = self.monitor._handle_stuck_jobs([alert])
        
        mock_stop_job.assert_not_called()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]['action'], 'monitoring')
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    def test_metrics_recording(self, mock_config):
        """Test metrics recording and retrieval"""
        mock_config.return_value = self.test_config
        
        # Record test metrics
        test_metrics = {
            'timestamp': now_datetime(),
            'site': 'test_site',
            'total_running_jobs': 5,
            'stuck_jobs_detected': 2,
            'actions_taken': 1,
            'cycle_duration_seconds': 0.123
        }
        
        self.monitor._record_metrics(test_metrics)
        
        # Verify metrics were stored
        cache_key = f"scheduler_monitor_metrics:test_site"
        stored_metrics = frappe.cache().get(cache_key)
        
        self.assertIsNotNone(stored_metrics)
        self.assertEqual(len(stored_metrics), 1)
        self.assertEqual(stored_metrics[0]['stuck_jobs_detected'], 2)
    
    @patch('frappe.get_site_config')
    def test_configuration_loading(self, mock_site_config):
        """Test configuration loading with site-specific overrides"""
        mock_site_config.return_value = {
            'scheduler_monitor': {
                'enabled': True,
                'protection_level': 'full_protection',
                'job_timeout_patterns': {
                    'custom.job': 120  # 2 hours in minutes
                }
            }
        }
        
        config = self.monitor._get_monitor_config()
        
        self.assertTrue(config['enabled'])
        self.assertEqual(config['protection_level'], 'full_protection')
        self.assertEqual(config['job_timeout_patterns']['custom.job'], 120)
        # Should merge with defaults
        self.assertIn('frappe.twofactor.delete_all_barcodes_for_users', config['job_timeout_patterns'])
    
    @patch('frappe.get_site_config')
    def test_configuration_defaults(self, mock_site_config):
        """Test that sensible defaults are used when no config exists"""
        mock_site_config.return_value = {}
        
        config = self.monitor._get_monitor_config()
        
        self.assertFalse(config['enabled'])  # Disabled by default
        self.assertEqual(config['protection_level'], 'monitor_only')
        self.assertEqual(config['standard_job_timeout_minutes'], 30)
        self.assertIn('frappe.twofactor.delete_all_barcodes_for_users', config['job_timeout_patterns'])
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    @patch.object(SchedulerMonitor, '_get_running_jobs')
    def test_error_handling(self, mock_get_jobs, mock_config):
        """Test error handling in monitoring cycle"""
        mock_config.return_value = self.test_config
        mock_get_jobs.side_effect = Exception("Redis connection failed")
        
        result = self.monitor.run_monitoring_cycle()
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Redis connection failed', result['error'])
    
    def test_global_max_runtime_detection(self):
        """Test that jobs exceeding global maximum runtime are detected"""
        job = Mock()
        job.started_at = add_to_date(now_datetime(), hours=-2)  # Running for 2 hours
        job.func_name = "unknown.job"
        
        config = self.test_config.copy()
        config['global_max_runtime_minutes'] = 90  # 1.5 hours
        
        is_stuck = self.monitor._is_job_stuck(job, config)
        self.assertTrue(is_stuck)
    
    @patch.object(SchedulerMonitor, '_get_monitor_config') 
    @patch.object(SchedulerMonitor, '_get_running_jobs')
    @patch('frappe.log_error')
    def test_stuck_job_logging(self, mock_log_error, mock_get_jobs, mock_config):
        """Test that stuck jobs are properly logged"""
        mock_config.return_value = self.test_config
        
        stuck_job = Mock()
        stuck_job.id = "logging_test_job"
        stuck_job.func_name = "test.logging_job"
        stuck_job.started_at = add_to_date(now_datetime(), minutes=-10)
        stuck_job.get_status.return_value = "started"
        
        mock_get_jobs.return_value = [stuck_job]
        
        result = self.monitor.run_monitoring_cycle()
        
        # Verify Error Log was created
        mock_log_error.assert_called()
        error_call = mock_log_error.call_args
        
        self.assertIn("Stuck Job Alert", error_call[1]['title'])
        self.assertEqual(error_call[1]['reference_doctype'], "RQ Job")
        self.assertEqual(error_call[1]['reference_name'], "logging_test_job")


class TestSchedulerMonitorIntegration(IntegrationTestCase):
    """Integration tests that verify the scheduler monitor works with real Frappe infrastructure"""
    
    def test_real_configuration_loading(self):
        """Test loading configuration from actual site config"""
        # This test uses the real site configuration
        monitor = SchedulerMonitor()
        config = monitor._get_monitor_config()
        
        # Should have all required keys
        required_keys = ['enabled', 'monitor_mode', 'default_timeout_seconds', 
                        'maximum_job_runtime_hours', 'job_timeout_patterns']
        for key in required_keys:
            self.assertIn(key, config)
    
    @patch('frappe.utils.scheduler_monitor.get_all_queued_jobs')
    def test_real_job_fetching(self, mock_get_jobs):
        """Test that we can fetch and process real job objects"""
        # Mock a realistic job structure
        mock_job = Mock()
        mock_job.id = "real_job_test"
        mock_job.func_name = "frappe.email.queue.flush"
        mock_job.started_at = now_datetime()
        mock_job.get_status.return_value = "started"
        
        mock_get_jobs.return_value = [mock_job]
        
        monitor = SchedulerMonitor()
        running_jobs = monitor._get_running_jobs()
        
        self.assertEqual(len(running_jobs), 1)
        self.assertEqual(running_jobs[0].id, "real_job_test")
    
    def test_cache_operations(self):
        """Test that metrics caching works with real Frappe cache"""
        monitor = SchedulerMonitor()
        
        test_metrics = {
            'timestamp': now_datetime(),
            'site': frappe.local.site,
            'test_data': 'integration_test'
        }
        
        monitor._record_metrics(test_metrics)
        
        # Verify we can retrieve the metrics
        cache_key = f"scheduler_monitor_metrics:{frappe.local.site}"
        cached_metrics = frappe.cache().get(cache_key)
        
        self.assertIsNotNone(cached_metrics)
        self.assertEqual(cached_metrics[0]['test_data'], 'integration_test')
        
        # Clean up
        frappe.cache().delete(cache_key)