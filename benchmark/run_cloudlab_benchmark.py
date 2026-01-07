#!/usr/bin/env python3
"""
Script to run CloudLab benchmark and process logs
This script runs 'fab cloudlab_remote', downloads logs, and processes them
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

# Add benchmark directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmark.logs import LogParser, ParseError
from benchmark.utils import PathMaker, Print, BenchError

# Import functions from time_storage_from_logs
try:
    from time_storage_from_logs import (
        process_node_log,
        export_round_end_pivot_table
    )
    TIME_STORAGE_AVAILABLE = True
except ImportError:
    TIME_STORAGE_AVAILABLE = False
    # Note: Print may not be available yet, so we'll handle the warning later

def run_fab_command(task='cloudlab_remote', debug=False):
    """Run fab command"""
    fab_cmd = ['fab', task]
    if debug:
        fab_cmd.append('debug=True')
    
    Print.info(f'Running: {" ".join(fab_cmd)}')
    Print.info('=' * 60)
    
    try:
        result = subprocess.run(
            fab_cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            check=False  # Don't raise on error, we'll check return code
        )
        return result.returncode == 0
    except FileNotFoundError:
        Print.error('fab command not found. Please install fabric:')
        Print.error('  pip install fabric')
        return False
    except Exception as e:
        Print.error(f'Failed to run fab command: {e}')
        return False

def download_logs_if_needed(settings_file='cloudlab_settings.json', max_workers=1):
    """Download logs if they don't exist locally"""
    logs_dir = Path(PathMaker.logs_path())
    
    # Check if logs already exist
    primary_logs = list(logs_dir.glob('primary-*.log'))
    worker_logs = list(logs_dir.glob('worker-*.log'))
    client_logs = list(logs_dir.glob('client-*.log'))
    
    if primary_logs or worker_logs or client_logs:
        Print.info(f'Found existing logs: {len(primary_logs)} primary, {len(worker_logs)} worker, {len(client_logs)} client')
        return True
    
    # Try to download logs
    Print.info('No local logs found, attempting to download from remote nodes...')
    try:
        from download_logs import download_logs
        return download_logs(settings_file, max_workers)
    except ImportError:
        Print.warn('download_logs.py not found, skipping download')
        return False

def process_logs(faults=0, save_to_file=True):
    """Process and display log results"""
    logs_dir = PathMaker.logs_path()
    
    if not os.path.exists(logs_dir):
        Print.error(f'Logs directory not found: {logs_dir}')
        return False
    
    Print.info('=' * 60)
    Print.info('Processing logs...')
    Print.info('=' * 60)
    
    try:
        parser = LogParser.process(logs_dir, faults=faults)
        result = parser.result()
        
        # Print results
        print(result)
        
        # Save to file
        if save_to_file:
            results_dir = Path(PathMaker.results_path())
            results_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            result_file = results_dir / f'benchmark_result_{timestamp}.txt'
            
            with open(result_file, 'w') as f:
                f.write(f'Benchmark Results - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
                f.write('=' * 60 + '\n')
                f.write(result)
            
            Print.info(f'\nResults saved to: {result_file}')
        
        # Export latency CSV
        csv_file = parser.export_latency_csv()
        if csv_file:
            Print.info(f'Latency CSV exported to: {csv_file}')
        else:
            Print.warn('Failed to export latency CSV (no latency data available)')
        
        return True
        
    except ParseError as e:
        Print.warn(f'Failed to parse logs: {e}')
        Print.warn('This may be because some log files are empty or incomplete.')
        return False
    except Exception as e:
        Print.warn(f'Error processing logs: {e}')
        return False

def extract_rate_from_logs(logs_dir):
    """Extract transaction rate from client log files.
    
    Args:
        logs_dir: Directory containing log files
    
    Returns:
        int or None: Transaction rate in tx/s, or None if not found
    """
    import re
    import glob
    from os.path import join
    
    # Try to find rate from any client log file
    client_logs = sorted(glob.glob(join(logs_dir, 'client-*.log')))
    for log_file in client_logs:
        try:
            with open(log_file, 'r') as f:
                content = f.read()
                match = re.search(r'Transactions rate: (\d+)', content)
                if match:
                    return int(match.group(1))
        except Exception:
            continue
    return None

def extract_imbalanced_rate_from_logs(logs_dir):
    """Extract imbalanced rates from client log files.
    
    Args:
        logs_dir: Directory containing log files
    
    Returns:
        list or None: List of imbalanced rates, or None if not found
    """
    import re
    import glob
    from os.path import join
    
    # Try to find rates from all client log files
    client_logs = sorted(glob.glob(join(logs_dir, 'client-*.log')))
    imbalanced_rates = []
    
    for log_file in client_logs:
        try:
            with open(log_file, 'r') as f:
                content = f.read()
                match = re.search(r'Transactions rate: (\d+)', content)
                if match:
                    imbalanced_rates.append(int(match.group(1)))
        except Exception:
            continue
    
    return imbalanced_rates if imbalanced_rates else None

def extract_duration_from_logs(logs_dir):
    """Extract duration from log summary files.
    
    Args:
        logs_dir: Directory containing log files
    
    Returns:
        int or None: Duration in seconds, or None if not found
    """
    import re
    import glob
    from os.path import join
    
    # Try to find duration from result files or log summaries
    result_files = sorted(glob.glob(join(logs_dir, '../results/bench-*.txt')))
    for result_file in result_files:
        try:
            with open(result_file, 'r') as f:
                content = f.read()
                match = re.search(r'Execution time: (\d+)\s+s', content)
                if match:
                    return int(match.group(1))
        except Exception:
            continue
    
    # Also try to find in any log file that might contain duration info
    log_files = sorted(glob.glob(join(logs_dir, '*.log')))
    for log_file in log_files[:5]:  # Check first few log files
        try:
            with open(log_file, 'r') as f:
                content = f.read()
                # Look for duration patterns
                match = re.search(r'Running benchmark\s*\((\d+)\s*sec\)', content)
                if match:
                    return int(match.group(1))
        except Exception:
            continue
    
    return None

def generate_round_end_time_pivot(num_nodes=10, experiment_group=None, logs_dir=None, rate=None, imbalanced_rate=None, duration=None):
    """Generate round_end_time_pivot.csv from log files.
    
    Args:
        num_nodes: Number of nodes to process
        experiment_group: Optional experiment group identifier (for multi-experiment support)
        logs_dir: Optional custom logs directory (default: uses PathMaker.logs_path() or 'logs')
        rate: Optional transaction rate (if None, will try to extract from logs)
        imbalanced_rate: Optional imbalanced rate list (if None, will try to extract from logs)
        duration: Optional duration in seconds (if None, will try to extract from logs)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not TIME_STORAGE_AVAILABLE:
        Print.warn('time_storage_from_logs module not available, skipping pivot table generation')
        return False
    
    Print.info('=' * 60)
    Print.info('Generating round_end_time_pivot.csv...')
    Print.info('=' * 60)
    
    try:
        # Change to benchmark directory to ensure relative paths work
        benchmark_dir = os.path.dirname(os.path.abspath(__file__))
        original_cwd = os.getcwd()
        os.chdir(benchmark_dir)
        
        try:
            # Determine logs directory
            if logs_dir is None:
                # Try to use PathMaker if available, otherwise default to 'logs'
                try:
                    logs_dir = PathMaker.logs_path()
                except:
                    logs_dir = 'logs'
            
            # Extract rate from logs if not provided
            if rate is None:
                rate = extract_rate_from_logs(logs_dir)
            
            # Extract imbalanced_rate from logs if not provided
            if imbalanced_rate is None:
                imbalanced_rate = extract_imbalanced_rate_from_logs(logs_dir)
            
            # Extract duration from logs if not provided
            if duration is None:
                duration = extract_duration_from_logs(logs_dir)
            
            # Build filename suffix with rate, imbalanced_rate, and duration
            suffix_parts = []
            
            if imbalanced_rate is not None and isinstance(imbalanced_rate, list) and len(imbalanced_rate) > 0:
                # Format imbalanced_rate for filename: use sum or a compact representation
                if len(imbalanced_rate) <= 10:
                    # For short lists, include all values
                    imbalanced_str = '_'.join(str(r) for r in imbalanced_rate)
                    suffix_parts.append(f'imbalanced_{imbalanced_str}')
                else:
                    # For long lists, use sum
                    imbalanced_sum = sum(imbalanced_rate)
                    suffix_parts.append(f'imbalanced_sum{imbalanced_sum}')
            elif rate is not None:
                # Fall back to regular rate if imbalanced_rate not available
                suffix_parts.append(f'rate{rate}')
            
            if duration is not None:
                suffix_parts.append(f'duration{duration}')
            
            suffix = '_' + '_'.join(suffix_parts) if suffix_parts else ''
            
            # Determine output filenames based on experiment group and parameters
            if experiment_group is not None:
                csv_filename = f'round_certificate_analysis_exp{experiment_group}{suffix}.csv'
                pivot_filename = f'round_end_time_pivot_exp{experiment_group}{suffix}.csv'
            else:
                csv_filename = f'round_certificate_analysis{suffix}.csv'
                pivot_filename = f'round_end_time_pivot{suffix}.csv'
            
            # Remove existing CSV file if it exists (to start fresh)
            if os.path.exists(csv_filename):
                os.remove(csv_filename)
            
            Print.info(f'Processing {num_nodes} nodes from logs directory: {logs_dir}')
            Print.info('')
            
            # Process each node's log
            for node_id in range(num_nodes):
                process_node_log(node_id, csv_filename, num_nodes, logs_dir=logs_dir)
            
            Print.info('=' * 60)
            Print.info(f'Analysis complete! Results saved to: {csv_filename}')
            
            # Generate round end time pivot table
            Print.info('\nGenerating round end time pivot table...')
            export_round_end_pivot_table(csv_filename, pivot_filename)
            
            Print.info(f'Round end time pivot table saved to: {pivot_filename}')
            Print.info('=' * 60)
            
            return True
            
        finally:
            os.chdir(original_cwd)
            
    except Exception as e:
        Print.error(f'Error generating pivot table: {e}')
        import traceback
        Print.error(traceback.format_exc())
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Run CloudLab benchmark and process logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Run benchmark and process logs
  python3 run_cloudlab_benchmark.py

  # Only process existing logs (skip running benchmark) and generate pivot table
  python3 run_cloudlab_benchmark.py --no-run

  # Run benchmark with debug mode
  python3 run_cloudlab_benchmark.py --debug

  # Only download logs without running benchmark
  python3 run_cloudlab_benchmark.py --download-only

  # Process multiple experiment groups
  python3 run_cloudlab_benchmark.py --no-run --experiment-groups 1 2 3

  # Process single experiment with custom node count
  python3 run_cloudlab_benchmark.py --no-run --num-nodes 20
        '''
    )
    
    parser.add_argument('--no-run', action='store_true',
                       help='Skip running fab cloudlab_remote, only process existing logs')
    parser.add_argument('--download-only', action='store_true',
                       help='Only download logs from remote nodes, do not run benchmark or process')
    parser.add_argument('--debug', action='store_true',
                       help='Run benchmark in debug mode')
    parser.add_argument('--faults', type=int, default=0,
                       help='Number of faulty nodes (default: 0)')
    parser.add_argument('--no-save', action='store_true',
                       help='Do not save results to file')
    parser.add_argument('--max-workers', type=int, default=1,
                       help='Maximum number of workers per node for log download (default: 1)')
    parser.add_argument('--settings', default='cloudlab_settings.json',
                       help='Path to CloudLab settings file (default: cloudlab_settings.json)')
    parser.add_argument('--num-nodes', type=int, default=10,
                       help='Number of nodes to process for pivot table (default: 10)')
    parser.add_argument('--experiment-groups', type=int, nargs='+', default=None,
                       help='Experiment group numbers to process (e.g., --experiment-groups 1 2 3). '
                            'If not specified, processes a single experiment.')
    parser.add_argument('--no-pivot', action='store_true',
                       help='Skip generating round_end_time_pivot.csv')
    parser.add_argument('--logs-dir', default=None,
                       help='Custom logs directory path (default: uses PathMaker.logs_path() or "logs")')
    
    args = parser.parse_args()
    
    Print.heading('CloudLab Benchmark Runner')
    Print.info('=' * 60)
    
    success = True
    
    # Step 1: Run benchmark (unless skipped)
    if not args.no_run and not args.download_only:
        success = run_fab_command('cloudlab_remote', debug=args.debug)
        if not success:
            Print.warn('Benchmark run completed with errors, but continuing to process logs...')
    
    # Step 2: Download logs if needed (unless download-only)
    if not args.download_only:
        download_logs_if_needed(args.settings, args.max_workers)
    else:
        # Download-only mode
        Print.info('Download-only mode: downloading logs from remote nodes...')
        download_logs_if_needed(args.settings, args.max_workers)
        Print.info('Download complete. Exiting.')
        return 0
    
    # Step 3: Process logs
    if not args.no_save:
        success = process_logs(faults=args.faults, save_to_file=True) and success
    else:
        success = process_logs(faults=args.faults, save_to_file=False) and success
    
    # Step 4: Generate round_end_time_pivot.csv (if not disabled)
    if not args.no_pivot:
        if args.experiment_groups:
            # Process multiple experiment groups
            Print.info('=' * 60)
            Print.info(f'Processing {len(args.experiment_groups)} experiment group(s)...')
            Print.info('=' * 60)
            
            for exp_group in args.experiment_groups:
                Print.info(f'\nProcessing experiment group {exp_group}...')
                # For multi-experiment, try logs_exp{group} directory if custom dir not specified
                exp_logs_dir = args.logs_dir
                if exp_logs_dir is None:
                    # Try experiment-specific logs directory
                    exp_logs_dir = f'logs_exp{exp_group}'
                    if not os.path.exists(exp_logs_dir):
                        exp_logs_dir = None  # Fall back to default
                
                exp_success = generate_round_end_time_pivot(
                    num_nodes=args.num_nodes,
                    experiment_group=exp_group,
                    logs_dir=exp_logs_dir,
                    rate=None  # Will be extracted from logs
                )
                success = exp_success and success
        else:
            # Process single experiment
            pivot_success = generate_round_end_time_pivot(
                num_nodes=args.num_nodes,
                experiment_group=None,
                logs_dir=args.logs_dir,
                rate=None  # Will be extracted from logs
            )
            success = pivot_success and success
    
    Print.info('=' * 60)
    if success:
        Print.info('✓ All operations completed successfully')
        return 0
    else:
        Print.warn('⚠ Some operations completed with errors')
        return 1

if __name__ == '__main__':
    sys.exit(main())

