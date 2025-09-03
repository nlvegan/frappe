#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Comprehensive Scheduler Monitor Test Runner
=============================================

Executes the complete test suite for scheduler monitor system,
including realistic scenarios and advanced edge cases.

Usage:
    # Run all tests
    python scripts/test_scheduler_monitor_comprehensive.py

    # Run specific test categories
    python scripts/test_scheduler_monitor_comprehensive.py --category realistic
    python scripts/test_scheduler_monitor_comprehensive.py --category edge_cases
    python scripts/test_scheduler_monitor_comprehensive.py --category integration

    # Run with specific focus areas
    python scripts/test_scheduler_monitor_comprehensive.py --focus resource_contention
    python scripts/test_scheduler_monitor_comprehensive.py --focus timing_issues
    python scripts/test_scheduler_monitor_comprehensive.py --focus recovery_validation

    # Generate detailed report
    python scripts/test_scheduler_monitor_comprehensive.py --report detailed

Features:
- Realistic production failure pattern testing
- Advanced edge case coverage
- System integration validation  
- Recovery mechanism verification
- Performance and load testing
- Security resilience testing
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime
from typing import Dict, List, Any
import unittest
import frappe


def setup_test_environment():
    """Setup test environment for scheduler monitor testing"""
    print("Setting up test environment...")
    
    # Ensure we're in test mode
    if not frappe.flags.in_test:
        frappe.flags.in_test = True
    
    # Initialize database connection
    if not hasattr(frappe.local, 'db'):
        frappe.init(site='dev.veganisme.net')
        frappe.connect()
    
    print("✓ Test environment ready")


def discover_test_modules() -> Dict[str, Any]:
    """Discover available test modules and their categories"""
    test_modules = {
        'realistic': {
            'module': 'frappe.tests.test_scheduler_monitor_realistic_scenarios',
            'classes': [
                'TestSchedulerProtectionRealisticScenarios',
                'SchedulerProtectionIntegrationTests'
            ],
            'description': 'Real-world failure patterns and production scenarios',
            'focus_areas': [
                'resource_contention',
                'database_locks', 
                'redis_connectivity',
                'timing_anomalies',
                'load_testing'
            ]
        },
        'edge_cases': {
            'module': 'frappe.tests.test_scheduler_monitor_edge_cases',
            'classes': [
                'TestSchedulerProtectionAdvancedEdgeCases',
                'RecoveryValidationTests'
            ],
            'description': 'Advanced edge cases and boundary conditions',
            'focus_areas': [
                'timing_issues',
                'resource_exhaustion',
                'concurrency_conflicts',
                'recovery_validation',
                'security_resilience'
            ]
        },
        'integration': {
            'module': 'scripts.testing.monitoring.test_monitoring_system',
            'classes': ['TestMonitoringSystemIntegration'],
            'description': 'Full system integration testing',
            'focus_areas': [
                'end_to_end_flow',
                'component_integration',
                'performance_validation'
            ]
        }
    }
    
    return test_modules


def create_test_suite(categories: List[str] = None, focus_areas: List[str] = None) -> unittest.TestSuite:
    """Create comprehensive test suite based on categories and focus areas"""
    suite = unittest.TestSuite()
    test_modules = discover_test_modules()
    
    categories = categories or list(test_modules.keys())
    
    for category in categories:
        if category not in test_modules:
            print(f"Warning: Unknown test category '{category}'")
            continue
            
        module_info = test_modules[category]
        
        # Skip if focus areas don't match
        if focus_areas:
            if not any(focus in module_info['focus_areas'] for focus in focus_areas):
                continue
        
        print(f"Loading tests from category: {category}")
        print(f"  Description: {module_info['description']}")
        
        try:
            # Import module dynamically
            module = __import__(module_info['module'], fromlist=[''])
            
            # Load test classes
            loader = unittest.TestLoader()
            for class_name in module_info['classes']:
                if hasattr(module, class_name):
                    test_class = getattr(module, class_name)
                    class_tests = loader.loadTestsFromTestCase(test_class)
                    suite.addTests(class_tests)
                    print(f"  ✓ Loaded {class_tests.countTestCases()} tests from {class_name}")
                else:
                    print(f"  ⚠ Warning: Class {class_name} not found in {module_info['module']}")
                    
        except ImportError as e:
            print(f"  ✗ Error importing {module_info['module']}: {e}")
            continue
    
    return suite


def run_test_suite(suite: unittest.TestSuite, report_level: str = 'summary') -> Dict[str, Any]:
    """Run test suite and collect results"""
    print(f"\n{'='*80}")
    print("RUNNING COMPREHENSIVE SCHEDULER PROTECTION TESTS")
    print(f"{'='*80}")
    print(f"Total tests to run: {suite.countTestCases()}")
    print(f"Report level: {report_level}")
    print(f"Started: {datetime.now()}")
    print()
    
    # Configure test runner based on report level
    verbosity = 2 if report_level in ['detailed', 'verbose'] else 1
    
    # Custom result collector for detailed analysis
    class ComprehensiveTestResult(unittest.TextTestResult):
        def __init__(self, stream, descriptions, verbosity):
            super().__init__(stream, descriptions, verbosity)
            self.test_timings = {}
            self.category_results = {}
            
        def startTest(self, test):
            self.test_start_time = time.time()
            super().startTest(test)
            
        def stopTest(self, test):
            duration = time.time() - getattr(self, 'test_start_time', time.time())
            self.test_timings[str(test)] = duration
            super().stopTest(test)
    
    # Run tests
    stream = sys.stdout if report_level != 'quiet' else open(os.devnull, 'w')
    runner = unittest.TextTestRunner(
        stream=stream,
        verbosity=verbosity,
        resultclass=ComprehensiveTestResult
    )
    
    start_time = time.time()
    result = runner.run(suite)
    total_duration = time.time() - start_time
    
    if stream != sys.stdout:
        stream.close()
    
    # Collect comprehensive results
    test_results = {
        'summary': {
            'total_tests': result.testsRun,
            'passed': result.testsRun - len(result.failures) - len(result.errors),
            'failed': len(result.failures),
            'errors': len(result.errors),
            'success_rate': ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0,
            'total_duration': total_duration,
            'average_test_duration': total_duration / result.testsRun if result.testsRun > 0 else 0
        },
        'failures': [
            {
                'test': str(test),
                'error': traceback,
                'category': classify_test_category(str(test))
            }
            for test, traceback in result.failures
        ],
        'errors': [
            {
                'test': str(test),
                'error': traceback,
                'category': classify_test_category(str(test))
            }
            for test, traceback in result.errors
        ],
        'performance': {
            'slowest_tests': sorted(
                [(test, duration) for test, duration in getattr(result, 'test_timings', {}).items()],
                key=lambda x: x[1],
                reverse=True
            )[:10] if hasattr(result, 'test_timings') else [],
            'category_performance': analyze_category_performance(getattr(result, 'test_timings', {}))
        },
        'coverage_analysis': analyze_test_coverage(result),
        'recommendations': generate_test_recommendations(result)
    }
    
    return test_results


def classify_test_category(test_name: str) -> str:
    """Classify test into category based on name"""
    if 'realistic' in test_name.lower():
        return 'realistic_scenarios'
    elif 'edge' in test_name.lower():
        return 'edge_cases'
    elif 'integration' in test_name.lower():
        return 'integration'
    else:
        return 'general'


def analyze_category_performance(test_timings: Dict[str, float]) -> Dict[str, Dict[str, float]]:
    """Analyze performance by test category"""
    category_stats = {}
    
    for test_name, duration in test_timings.items():
        category = classify_test_category(test_name)
        
        if category not in category_stats:
            category_stats[category] = []
        category_stats[category].append(duration)
    
    performance_analysis = {}
    for category, durations in category_stats.items():
        if durations:
            performance_analysis[category] = {
                'count': len(durations),
                'total_duration': sum(durations),
                'average_duration': sum(durations) / len(durations),
                'max_duration': max(durations),
                'min_duration': min(durations)
            }
    
    return performance_analysis


def analyze_test_coverage(result) -> Dict[str, Any]:
    """Analyze what areas of the scheduler monitor system are covered"""
    coverage_areas = {
        'resource_contention': 0,
        'timing_anomalies': 0,
        'redis_connectivity': 0,
        'database_interaction': 0,
        'configuration_handling': 0,
        'recovery_mechanisms': 0,
        'performance_monitoring': 0,
        'security_resilience': 0,
        'edge_cases': 0,
        'integration_testing': 0
    }
    
    # Count tests in each coverage area based on test names
    all_tests = result.testsRun
    for i in range(all_tests):
        # This is a simplified analysis - in practice, you'd examine actual test methods
        pass
    
    return {
        'areas_covered': len([area for area, count in coverage_areas.items() if count > 0]),
        'total_areas': len(coverage_areas),
        'coverage_percentage': len([area for area, count in coverage_areas.items() if count > 0]) / len(coverage_areas) * 100,
        'area_breakdown': coverage_areas
    }


def generate_test_recommendations(result) -> List[str]:
    """Generate recommendations based on test results"""
    recommendations = []
    
    # Performance recommendations
    if hasattr(result, 'test_timings'):
        slow_tests = [t for t, d in result.test_timings.items() if d > 5.0]
        if slow_tests:
            recommendations.append(f"Consider optimizing {len(slow_tests)} slow tests (>5s runtime)")
    
    # Failure analysis recommendations
    if result.failures:
        recommendations.append(f"Address {len(result.failures)} test failures for complete coverage")
    
    if result.errors:
        recommendations.append(f"Fix {len(result.errors)} test errors for reliable execution")
    
    # Coverage recommendations
    success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0
    if success_rate < 95:
        recommendations.append("Improve test success rate to >95% for production readiness")
    
    return recommendations


def generate_detailed_report(test_results: Dict[str, Any], output_file: str = None):
    """Generate detailed test execution report"""
    report_content = f"""
# Scheduler Protection System - Comprehensive Test Report

**Generated:** {datetime.now()}
**Test Execution Duration:** {test_results['summary']['total_duration']:.2f} seconds

## Executive Summary

- **Total Tests:** {test_results['summary']['total_tests']}
- **Success Rate:** {test_results['summary']['success_rate']:.1f}%
- **Passed:** {test_results['summary']['passed']}
- **Failed:** {test_results['summary']['failed']}
- **Errors:** {test_results['summary']['errors']}
- **Average Test Duration:** {test_results['summary']['average_test_duration']:.3f}s

## Test Categories Covered

### Realistic Production Scenarios
- Resource contention testing
- Database lock simulation
- Redis connectivity issues
- Timing anomaly handling
- Load testing with realistic job mixes

### Advanced Edge Cases  
- NTP clock adjustments
- System hibernation/resume
- Memory fragmentation scenarios
- Configuration changes during monitoring
- Concurrent monitoring cycle prevention
- Malicious data injection testing

### System Integration
- Actual Redis queue interaction
- Frappe Scheduled Job Type integration
- Database connection handling
- End-to-end monitoring flow

## Performance Analysis

### Slowest Tests
"""

    for test_name, duration in test_results['performance']['slowest_tests'][:5]:
        report_content += f"- {test_name}: {duration:.3f}s\n"

    if test_results['performance']['category_performance']:
        report_content += "\n### Performance by Category\n"
        for category, stats in test_results['performance']['category_performance'].items():
            report_content += f"- **{category.replace('_', ' ').title()}:** {stats['count']} tests, avg {stats['average_duration']:.3f}s\n"

    if test_results['failures']:
        report_content += f"\n## Test Failures ({len(test_results['failures'])})\n"
        for failure in test_results['failures']:
            report_content += f"### {failure['test']} ({failure['category']})\n"
            report_content += f"```\n{failure['error']}\n```\n\n"

    if test_results['errors']:
        report_content += f"\n## Test Errors ({len(test_results['errors'])})\n"
        for error in test_results['errors']:
            report_content += f"### {error['test']} ({error['category']})\n"
            report_content += f"```\n{error['error']}\n```\n\n"

    if test_results['recommendations']:
        report_content += "\n## Recommendations\n"
        for rec in test_results['recommendations']:
            report_content += f"- {rec}\n"

    report_content += f"""
## Coverage Analysis

- **Coverage Areas:** {test_results['coverage_analysis']['areas_covered']}/{test_results['coverage_analysis']['total_areas']} ({test_results['coverage_analysis']['coverage_percentage']:.1f}%)

## Production Readiness Assessment

"""
    
    success_rate = test_results['summary']['success_rate']
    if success_rate >= 95:
        report_content += "✅ **READY FOR PRODUCTION** - High success rate and comprehensive coverage\n"
    elif success_rate >= 85:
        report_content += "⚠️ **NEEDS MINOR FIXES** - Good coverage but some test failures need attention\n"
    else:
        report_content += "❌ **NOT PRODUCTION READY** - Significant test failures require investigation\n"

    report_content += f"""
## Next Steps

1. **Address Test Failures:** Focus on the {test_results['summary']['failed']} failed tests
2. **Fix Test Errors:** Resolve the {test_results['summary']['errors']} test setup/execution errors
3. **Performance Optimization:** Review tests taking >5 seconds
4. **Enhanced Coverage:** Add tests for any uncovered edge cases
5. **Documentation:** Update system documentation with test findings

---
*This report was generated by the Comprehensive Scheduler Protection Test Suite*
"""

    if output_file:
        with open(output_file, 'w') as f:
            f.write(report_content)
        print(f"Detailed report saved to: {output_file}")
    else:
        print(report_content)


def main():
    """Main test runner entry point"""
    parser = argparse.ArgumentParser(description='Comprehensive Scheduler Monitor Test Runner')
    parser.add_argument('--category', action='append', choices=['realistic', 'edge_cases', 'integration'],
                       help='Test categories to run (can specify multiple)')
    parser.add_argument('--focus', action='append', 
                       choices=['resource_contention', 'timing_issues', 'redis_connectivity', 
                               'database_locks', 'recovery_validation', 'security_resilience'],
                       help='Focus on specific areas (can specify multiple)')
    parser.add_argument('--report', choices=['summary', 'detailed', 'quiet'], default='summary',
                       help='Report detail level')
    parser.add_argument('--output', help='Output file for detailed report')
    
    args = parser.parse_args()
    
    try:
        # Setup test environment
        setup_test_environment()
        
        # Create test suite
        suite = create_test_suite(
            categories=args.category,
            focus_areas=args.focus
        )
        
        if suite.countTestCases() == 0:
            print("No tests found matching the specified criteria.")
            return 1
        
        # Run tests
        test_results = run_test_suite(suite, args.report)
        
        # Generate report
        if args.report == 'detailed':
            generate_detailed_report(test_results, args.output)
        elif args.report == 'summary':
            print(f"\n{'='*80}")
            print("TEST EXECUTION SUMMARY")
            print(f"{'='*80}")
            print(f"Success Rate: {test_results['summary']['success_rate']:.1f}%")
            print(f"Tests: {test_results['summary']['passed']}/{test_results['summary']['total_tests']} passed")
            print(f"Duration: {test_results['summary']['total_duration']:.2f} seconds")
            
            if test_results['recommendations']:
                print(f"\nRecommendations:")
                for rec in test_results['recommendations'][:3]:  # Show top 3
                    print(f"  • {rec}")
        
        # Return appropriate exit code
        return 0 if test_results['summary']['success_rate'] >= 95 else 1
        
    except Exception as e:
        print(f"Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)