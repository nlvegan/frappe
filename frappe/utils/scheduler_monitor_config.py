"""
Scheduler Monitor Configuration Management
=========================================

Configuration loading, validation, and default values for the
Scheduler Monitor system.
"""

import frappe
from typing import Dict, Any, Optional


class SchedulerMonitorConfig:
    """
    Configuration manager for Scheduler Monitor.
    
    Loads configuration from site_config.json and provides validation
    and default values for scheduler monitor settings.
    """
    
    DEFAULT_CONFIG = {
        'enabled': True,
        'protection_level': 'safe_protection',
        'standard_job_timeout_minutes': 30,
        'maximum_job_runtime_hours': 2.0,
        'job_timeout_patterns': {
            '*dues*': 60,
            '*sepa*': 30,
            'frappe.email.queue.flush': 5,
            'frappe.*': 15,
        },
        'max_jobs_per_cycle': 100,
        'enable_graceful_termination': True,
        'minimum_runtime_before_termination_minutes': 10,
        'termination_exempt_patterns': [],
        'monitoring_cycle_interval_seconds': 300  # 5 minutes
    }
    
    VALID_PROTECTION_LEVELS = ['monitor_only', 'safe_protection', 'full_protection']
    
    def __init__(self, site: Optional[str] = None):
        self.site = site or frappe.local.site
        self._config_cache = {}
        
    def get_config(self) -> Dict[str, Any]:
        """
        Get validated configuration with defaults applied.
        
        Returns:
            Dict containing validated configuration
        """
        try:
            # Check cache first
            cache_key = f"scheduler_monitor_config:{self.site}"
            if cache_key in self._config_cache:
                return self._config_cache[cache_key]
            
            # Load from site_config.json
            site_config = frappe.get_site_config()
            monitor_config = site_config.get('scheduler_monitor', {})
            
            # Merge with defaults
            config = self.DEFAULT_CONFIG.copy()
            config.update(monitor_config)
            
            # Validate configuration
            config = self._validate_config(config)
            
            # Cache for performance
            self._config_cache[cache_key] = config
            
            return config
            
        except Exception as e:
            frappe.log_error(f"Error loading scheduler monitor config: {str(e)}", "SchedulerMonitorConfig")
            return self.DEFAULT_CONFIG.copy()
    
    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and sanitize configuration values"""
        try:
            # Validate protection level
            if config.get('protection_level') not in self.VALID_PROTECTION_LEVELS:
                frappe.log_error(
                    f"Invalid protection_level: {config.get('protection_level')}, using default",
                    "SchedulerMonitorConfig"
                )
                config['protection_level'] = 'safe_protection'
            
            # Validate numeric values
            config['standard_job_timeout_minutes'] = max(1, int(config.get('standard_job_timeout_minutes', 30)))
            config['maximum_job_runtime_hours'] = max(0.5, float(config.get('maximum_job_runtime_hours', 2.0)))
            config['max_jobs_per_cycle'] = max(10, min(1000, int(config.get('max_jobs_per_cycle', 100))))
            config['minimum_runtime_before_termination_minutes'] = max(1, int(config.get('minimum_runtime_before_termination_minutes', 10)))
            config['monitoring_cycle_interval_seconds'] = max(60, int(config.get('monitoring_cycle_interval_seconds', 300)))
            
            # Validate boolean values
            config['enabled'] = bool(config.get('enabled', True))
            config['enable_graceful_termination'] = bool(config.get('enable_graceful_termination', True))
            
            # Validate patterns
            if not isinstance(config.get('job_timeout_patterns'), dict):
                config['job_timeout_patterns'] = self.DEFAULT_CONFIG['job_timeout_patterns'].copy()
            
            if not isinstance(config.get('termination_exempt_patterns'), list):
                config['termination_exempt_patterns'] = []
            
            return config
            
        except Exception as e:
            frappe.log_error(f"Error validating scheduler monitor config: {str(e)}", "SchedulerMonitorConfig")
            return self.DEFAULT_CONFIG.copy()
    
    def is_enabled(self) -> bool:
        """Check if scheduler monitoring is enabled"""
        try:
            return self.get_config().get('enabled', True)
        except Exception:
            return True  # Fail-safe default
    
    def clear_cache(self):
        """Clear configuration cache (useful for testing)"""
        self._config_cache.clear()
    
    @staticmethod
    def validate_job_timeout_patterns(patterns: Dict[str, Any]) -> Dict[str, float]:
        """
        Validate and convert job timeout patterns to proper format.
        
        Args:
            patterns: Raw patterns from configuration
            
        Returns:
            Dict with validated patterns and numeric timeout values
        """
        validated = {}
        
        if not isinstance(patterns, dict):
            return {}
        
        for pattern, timeout in patterns.items():
            try:
                if isinstance(pattern, str) and pattern.strip():
                    timeout_float = float(timeout)
                    if timeout_float > 0:
                        validated[pattern.strip()] = timeout_float
            except (ValueError, TypeError):
                continue
        
        return validated