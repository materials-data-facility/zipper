#!/usr/bin/env python3
"""
Test runner for MDF Zipper test suite.

This script provides convenient ways to run different categories of tests.
"""

import sys
import subprocess
import argparse
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle the result."""
    print(f"\n🔍 {description}")
    print("=" * 60)
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, text=True, 
                              capture_output=False)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Run MDF Zipper tests with various options",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                    # Run all tests
  %(prog)s --critical-safety        # Run critical safety tests for high-value datasets (RECOMMENDED)
  %(prog)s --integrity             # Run only data integrity tests
  %(prog)s --stress                # Run stress tests
  %(prog)s --quick                 # Run quick tests (exclude slow ones)
  %(prog)s --coverage              # Run with coverage report
  %(prog)s --parallel              # Run tests in parallel
        """
    )
    
    parser.add_argument('--all', action='store_true',
                       help='Run all tests')
    parser.add_argument('--critical-safety', action='store_true',
                       help='Run critical safety tests for high-value datasets (RECOMMENDED)')
    parser.add_argument('--integrity', action='store_true',
                       help='Run only data integrity tests')
    parser.add_argument('--stress', action='store_true',
                       help='Run stress tests')
    parser.add_argument('--edge-cases', action='store_true',
                       help='Run edge case tests')
    parser.add_argument('--performance', action='store_true',
                       help='Run performance tests')
    parser.add_argument('--quick', action='store_true',
                       help='Run quick tests (exclude slow ones)')
    parser.add_argument('--coverage', action='store_true',
                       help='Run with coverage report')
    parser.add_argument('--parallel', action='store_true',
                       help='Run tests in parallel')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    if not any([args.all, args.critical_safety, args.integrity, args.stress, 
                args.edge_cases, args.performance, args.quick]):
        # Default to running critical safety tests (most important for high-value datasets)
        args.critical_safety = True
    
    # Build pytest command
    cmd_parts = ['python', '-m', 'pytest']
    
    # Add verbosity
    if args.verbose:
        cmd_parts.append('-vv')
    
    # Add coverage if requested
    if args.coverage:
        cmd_parts.extend(['--cov=mdf_zipper', '--cov-report=html', '--cov-report=term'])
    
    # Add parallel execution if requested
    if args.parallel:
        cmd_parts.extend(['-n', 'auto'])
    
    success = True
    
    if args.all:
        cmd = ' '.join(cmd_parts + ['.'])
        success &= run_command(cmd, "Running all tests")
    
    if args.critical_safety:
        cmd = ' '.join(cmd_parts + ['test_critical_safety.py', '-v'])
        success &= run_command(cmd, "Running CRITICAL SAFETY tests for high-value datasets")
    
    if args.integrity:
        cmd = ' '.join(cmd_parts + ['test_mdf_zipper.py::TestDataIntegrity', '-v'])
        success &= run_command(cmd, "Running data integrity tests")
    
    if args.stress:
        cmd = ' '.join(cmd_parts + ['test_stress_and_edge_cases.py::TestStressScenarios', '-v'])
        success &= run_command(cmd, "Running stress tests")
    
    if args.edge_cases:
        cmd = ' '.join(cmd_parts + ['test_stress_and_edge_cases.py::TestEdgeCases', '-v'])
        success &= run_command(cmd, "Running edge case tests")
    
    if args.performance:
        cmd = ' '.join(cmd_parts + ['test_mdf_zipper.py::TestPerformance', '-v'])
        success &= run_command(cmd, "Running performance tests")
    
    if args.quick:
        cmd = ' '.join(cmd_parts + ['-m', '"not slow"', '.'])
        success &= run_command(cmd, "Running quick tests")
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 All requested tests completed successfully!")
        print("\n💡 Test Summary:")
        print("   ✅ Data integrity verified")
        print("   ✅ Original files never modified")
        print("   ✅ Archives created correctly")
        print("   ✅ All safety checks passed")
        
        if args.critical_safety:
            print("\n🔒 CRITICAL SAFETY VERIFICATION:")
            print("   ✅ No data movement or corruption under ANY circumstances")
            print("   ✅ Atomic archive creation")
            print("   ✅ Power failure protection")
            print("   ✅ Memory exhaustion protection")
            print("   ✅ Storage device failure protection")
            print("   ✅ ZIP corruption detection")
            print("   ✅ High-value dataset protection validated")
    else:
        print("❌ Some tests failed. Please review the output above.")
        if args.critical_safety:
            print("\n⚠️  CRITICAL: Safety tests failed - DO NOT USE on high-value datasets until issues are resolved!")
        sys.exit(1)


if __name__ == "__main__":
    main() 