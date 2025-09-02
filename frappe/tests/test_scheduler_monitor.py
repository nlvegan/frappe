"""
Test suite for Frappe Scheduler Monitor

Tests the proactive monitoring and protection capabilities for stuck jobs
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
        
        # Mock site config for testing
        self.test_config = {
            'enabled': True,
            'monitor_mode': 'observe_only',
            'default_timeout_seconds': 300,  # 5 minutes for testing
            'global_max_runtime_minutes': 30,  # 30 minutes for testing
            'enable_graceful_termination': True,
            'job_timeouts': {
                'test.long_running_job': 600,  # 10 minutes
                'test.quick_job': 60,  # 1 minute
                'frappe.twofactor.delete_all_barcodes_for_users': 1800  # 30 minutes
            }
        }
        
    def tearDown(self):
        super().tearDown()
        # Clean up cache
        cache_key = f"scheduler_monitor_metrics:test_site"
        frappe.cache().delete(cache_key)
    
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
    
    def test_stuck_job_alert_creation(self):
        """Test creation of StuckJobAlert objects"""
        alert = StuckJobAlert(
            job_id="test_123",
            job_name="test.job",
            queue="default",
            runtime_minutes=15.5,
            configured_timeout_minutes=10.0,
            alert_level="warning",
            recommended_action="monitor_closely"
        )
        
        serialized = self.monitor._serialize_stuck_job(alert)
        
        self.assertEqual(serialized['job_id'], 'test_123')
        self.assertEqual(serialized['runtime_minutes'], 15.5)
        self.assertEqual(serialized['alert_level'], 'warning')
    
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
        """Test job termination in active mode"""
        config = self.test_config.copy()
        config['monitor_mode'] = 'active'
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
    def test_job_termination_observe_only_mode(self, mock_stop_job, mock_config):
        """Test that jobs are not terminated in observe-only mode"""
        config = self.test_config.copy()
        config['monitor_mode'] = 'observe_only'
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
                'monitor_mode': 'active',
                'job_timeouts': {
                    'custom.job': 7200  # 2 hours
                }
            }
        }
        
        config = self.monitor._get_monitor_config()
        
        self.assertTrue(config['enabled'])
        self.assertEqual(config['monitor_mode'], 'active')
        self.assertEqual(config['job_timeouts']['custom.job'], 7200)
        # Should merge with defaults
        self.assertIn('frappe.twofactor.delete_all_barcodes_for_users', config['job_timeouts'])
    
    @patch('frappe.get_site_config')
    def test_configuration_defaults(self, mock_site_config):
        """Test that sensible defaults are used when no config exists"""
        mock_site_config.return_value = {}
        
        config = self.monitor._get_monitor_config()
        
        self.assertFalse(config['enabled'])  # Disabled by default
        self.assertEqual(config['monitor_mode'], 'observe_only')
        self.assertEqual(config['default_timeout_seconds'], 1800)
        self.assertIn('frappe.twofactor.delete_all_barcodes_for_users', config['job_timeouts'])
    
    @patch.object(SchedulerMonitor, '_get_monitor_config')
    def test_utility_functions(self, mock_config):
        """Test utility functions for external integration"""
        mock_config.return_value = self.test_config
        
        # Test is_monitoring_enabled
        self.assertTrue(is_monitoring_enabled("test_site"))
        
        # Test get_monitoring_status
        status = get_monitoring_status("test_site")
        self.assertTrue(status['enabled'])
        self.assertEqual(status['site'], "test_site")
    
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
                        'global_max_runtime_minutes', 'job_timeouts']
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