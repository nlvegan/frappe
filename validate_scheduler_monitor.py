#!/usr/bin/env python3
"""
Validation script for Scheduler Monitor implementation

This script validates that the scheduler monitor implementation is syntactically
correct and follows expected patterns without requiring a full Frappe environment.
"""

import ast
import sys
from pathlib import Path


def validate_python_syntax(file_path):
    """Validate that a Python file has correct syntax"""
    try:
        with open(file_path, 'r') as f:
            source = f.read()
        
        # Parse the AST to check syntax
        ast.parse(source)
        print(f"✅ {file_path.name}: Syntax valid")
        return True
        
    except SyntaxError as e:
        print(f"❌ {file_path.name}: Syntax error at line {e.lineno}: {e.msg}")
        return False
    except Exception as e:
        print(f"❌ {file_path.name}: Error - {str(e)}")
        return False


def validate_imports(file_path):
    """Validate that imports follow expected patterns"""
    try:
        with open(file_path, 'r') as f:
            source = f.read()
            
        # Check for expected imports
        required_patterns = [
            'import frappe',
            'from frappe.utils',
            'from frappe.core.doctype.rq_job',
            'from datetime import',
            'from typing import'
        ]
        
        missing_patterns = []
        for pattern in required_patterns:
            if pattern not in source:
                missing_patterns.append(pattern)
        
        if missing_patterns:
            print(f"⚠️  {file_path.name}: Missing expected imports: {missing_patterns}")
        else:
            print(f"✅ {file_path.name}: Import patterns valid")
            
        return len(missing_patterns) == 0
        
    except Exception as e:
        print(f"❌ {file_path.name}: Import validation error - {str(e)}")
        return False


def validate_class_structure(file_path):
    """Validate that required classes and methods exist"""
    try:
        with open(file_path, 'r') as f:
            source = f.read()
            
        tree = ast.parse(source)
        
        # Check for SchedulerMonitor class
        scheduler_monitor_found = False
        required_methods = [
            'run_monitoring_cycle',
            '_get_running_jobs',
            '_identify_stuck_jobs',
            '_handle_stuck_jobs'
        ]
        
        found_methods = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == 'SchedulerMonitor':
                scheduler_monitor_found = True
                
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        found_methods.append(item.name)
        
        if not scheduler_monitor_found:
            print(f"❌ {file_path.name}: SchedulerMonitor class not found")
            return False
            
        missing_methods = set(required_methods) - set(found_methods)
        if missing_methods:
            print(f"❌ {file_path.name}: Missing required methods: {missing_methods}")
            return False
            
        print(f"✅ {file_path.name}: Class structure valid")
        return True
        
    except Exception as e:
        print(f"❌ {file_path.name}: Class structure validation error - {str(e)}")
        return False


def validate_utility_functions(file_path):
    """Validate that utility functions exist"""
    try:
        with open(file_path, 'r') as f:
            source = f.read()
            
        required_functions = [
            'run_scheduler_monitoring_cycle',
            'is_monitoring_enabled',
            'get_monitoring_status'
        ]
        
        tree = ast.parse(source)
        found_functions = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                found_functions.append(node.name)
                
        missing_functions = set(required_functions) - set(found_functions)
        if missing_functions:
            print(f"❌ {file_path.name}: Missing utility functions: {missing_functions}")
            return False
            
        print(f"✅ {file_path.name}: Utility functions valid")
        return True
        
    except Exception as e:
        print(f"❌ {file_path.name}: Utility function validation error - {str(e)}")
        return False


def validate_hooks_integration():
    """Validate that hooks file has been updated correctly"""
    hooks_file = Path("frappe/hooks.py")
    
    if not hooks_file.exists():
        print("❌ hooks.py: File not found")
        return False
        
    try:
        with open(hooks_file, 'r') as f:
            content = f.read()
            
        if 'frappe.utils.scheduler_monitor.run_scheduler_monitoring_cycle' in content:
            print("✅ hooks.py: Scheduler monitor integration found")
            return True
        else:
            print("❌ hooks.py: Scheduler monitor integration not found")
            return False
            
    except Exception as e:
        print(f"❌ hooks.py: Error reading file - {str(e)}")
        return False


def main():
    """Main validation function"""
    print("🔍 Validating Frappe Scheduler Monitor Implementation")
    print("=" * 60)
    
    # Files to validate
    scheduler_monitor_file = Path("frappe/utils/scheduler_monitor.py")
    test_file = Path("frappe/tests/test_scheduler_monitor.py")
    
    all_valid = True
    
    # Validate main implementation
    if scheduler_monitor_file.exists():
        print(f"\n📁 Validating {scheduler_monitor_file}")
        all_valid &= validate_python_syntax(scheduler_monitor_file)
        all_valid &= validate_imports(scheduler_monitor_file)
        all_valid &= validate_class_structure(scheduler_monitor_file)
        all_valid &= validate_utility_functions(scheduler_monitor_file)
    else:
        print(f"❌ {scheduler_monitor_file}: File not found")
        all_valid = False
    
    # Validate test file
    if test_file.exists():
        print(f"\n📁 Validating {test_file}")
        all_valid &= validate_python_syntax(test_file)
        all_valid &= validate_imports(test_file)
    else:
        print(f"❌ {test_file}: File not found")
        all_valid = False
    
    # Validate hooks integration
    print(f"\n📁 Validating hooks integration")
    all_valid &= validate_hooks_integration()
    
    # Summary
    print("\n" + "=" * 60)
    if all_valid:
        print("🎉 All validations passed! Implementation appears correct.")
        return 0
    else:
        print("❌ Some validations failed. Please check the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())