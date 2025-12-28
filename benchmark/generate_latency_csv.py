#!/usr/bin/env python3
"""
Generate latency CSV file from logs
"""

import sys
import os
import argparse
from pathlib import Path

# Add benchmark directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmark.logs import LogParser, ParseError
from benchmark.utils import PathMaker, Print

def generate_csv(logs_dir=None, faults=0, output_file=None):
    """Generate latency CSV from logs"""
    
    if logs_dir is None:
        logs_dir_path = Path(PathMaker.logs_path())
    else:
        logs_dir_path = Path(logs_dir)
    
    if not logs_dir_path.exists():
        Print.error(f'Logs directory not found: {logs_dir_path}')
        return False
    
    Print.info(f'Processing logs from: {logs_dir_path}')
    Print.info('=' * 60)
    
    try:
        parser = LogParser.process(str(logs_dir_path), faults=faults)
        
        # Call result() to ensure latency details are calculated
        result = parser.result()
        Print.info('Log parsing completed successfully')
        
        # Export latency CSV
        csv_file = parser.export_latency_csv(filename=output_file)
        
        if csv_file:
            Print.info(f'✓ Latency CSV exported to: {csv_file}')
            return True
        else:
            Print.warn('Failed to export latency CSV (no latency data available)')
            return False
            
    except ParseError as e:
        Print.warn(f'Failed to parse logs: {e}')
        Print.warn('This may be because some log files are empty or incomplete.')
        return False
    except Exception as e:
        Print.warn(f'Error processing logs: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Generate latency CSV file from benchmark logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Generate CSV from default logs directory
  python3 generate_latency_csv.py

  # Specify logs directory
  python3 generate_latency_csv.py --logs-dir ./logs

  # Specify output file
  python3 generate_latency_csv.py -o results/my_latency.csv

  # With faults
  python3 generate_latency_csv.py --faults 1
        '''
    )
    
    parser.add_argument('--logs-dir', dest='logs_dir',
                       help='Directory containing log files (default: logs/)')
    parser.add_argument('-o', '--output', dest='output_file',
                       help='Output CSV file path (default: results/e2e_latency_YYYYMMDD_HHMMSS.csv)')
    parser.add_argument('--faults', type=int, default=0,
                       help='Number of faulty nodes (default: 0)')
    
    args = parser.parse_args()
    
    success = generate_csv(
        logs_dir=args.logs_dir,
        faults=args.faults,
        output_file=args.output_file
    )
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()

