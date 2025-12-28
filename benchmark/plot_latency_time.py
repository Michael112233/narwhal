#!/usr/bin/env python3
"""
Plot latency over time from CSV file
横坐标：相对时间（相对于日志中第一条日志的时间）
纵坐标：延迟（latency_ms）
"""

import csv
import sys
import argparse
import re
from pathlib import Path
from datetime import datetime
from glob import glob
import matplotlib.pyplot as plt
import numpy as np

def find_first_log_timestamp(logs_dir):
    """
    Find the timestamp of the first log entry from log files
    
    Args:
        logs_dir: Directory containing log files
        
    Returns:
        POSIX timestamp of the first log entry, or None if not found
    """
    logs_dir = Path(logs_dir)
    first_timestamp = None
    
    # Search for log files
    log_files = []
    log_files.extend(logs_dir.glob('primary-*.log'))
    log_files.extend(logs_dir.glob('client-*.log'))
    log_files.extend(logs_dir.glob('worker-*.log'))
    
    if not log_files:
        return None
    
    # Pattern to match log timestamp: [2025-12-22T12:55:53.786Z ...
    timestamp_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)')
    
    for log_file in log_files:
        try:
            with open(log_file, 'r') as f:
                first_line = f.readline()
                if first_line:
                    match = timestamp_pattern.search(first_line)
                    if match:
                        timestamp_str = match.group(1)
                        # Convert to POSIX timestamp
                        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        posix_time = dt.timestamp()
                        
                        if first_timestamp is None or posix_time < first_timestamp:
                            first_timestamp = posix_time
        except Exception as e:
            print(f"Warning: Failed to read {log_file}: {e}")
            continue
    
    return first_timestamp

def plot_latency_time(csv_file, output_file=None, window_size=None, show_scatter=True, show_line=False, logs_dir=None):
    """
    Plot latency over time from CSV file
    
    Args:
        csv_file: Path to CSV file
        output_file: Output image file path (optional)
        window_size: Moving average window size (optional, for smoothing)
        show_scatter: Show scatter plot (default: True)
        show_line: Show line plot (default: False)
        logs_dir: Directory containing log files to find first log timestamp (optional)
    """
    # Read CSV file
    time_data = []
    latency_data = []
    
    try:
        # First pass: read all data
        all_rows = []
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append(row)
        
        if not all_rows:
            print("Error: No data found in CSV file")
            sys.exit(1)
        
        # Use relative time from CSV (preferred: start_time_relative_sec, fallback: end_time_relative_sec)
        time_column = None
        if 'start_time_relative_sec' in all_rows[0]:
            time_column = 'start_time_relative_sec'
            print("Using start_time_relative_sec from CSV file")
        elif 'end_time_relative_sec' in all_rows[0]:
            time_column = 'end_time_relative_sec'
            print("Using end_time_relative_sec from CSV file (start_time_relative_sec not found)")
        else:
            # Fallback: calculate relative time from start_time
            print("Warning: relative time columns not found in CSV, calculating from start_time")
            
            # Find reference time: first log entry timestamp or minimum start_time
            reference_time = None
            
            if logs_dir:
                # Try to find first log timestamp
                reference_time = find_first_log_timestamp(logs_dir)
                if reference_time:
                    print(f"Using first log entry timestamp as reference: {reference_time}")
                else:
                    print("Warning: Could not find first log timestamp, using minimum start_time instead")
            
            if reference_time is None:
                # Fallback: use minimum start_time
                start_times = [float(row['start_time']) for row in all_rows]
                reference_time = min(start_times)
                print(f"Using minimum start_time as reference: {reference_time}")
        
        # Read data
        for row in all_rows:
            if time_column:
                # Use relative time from CSV
                time_data.append(float(row[time_column]))
            else:
                # Calculate relative time
                start_time = float(row['start_time'])
                relative_start_time = start_time - reference_time
                time_data.append(relative_start_time)
            latency_data.append(float(row['latency_ms']))
                
    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_file}")
        sys.exit(1)
    except KeyError as e:
        print(f"Error: Missing column in CSV: {e}")
        print("Expected columns: start_time, latency_ms")
        print(f"Available columns: {list(all_rows[0].keys()) if all_rows else 'N/A'}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    if not time_data:
        print("Error: No data found in CSV file")
        sys.exit(1)
    
    # Convert to numpy arrays for easier manipulation
    time_array = np.array(time_data)
    latency_array = np.array(latency_data)
    
    # Create figure
    plt.figure(figsize=(12, 6))
    
    # Plot scatter or line
    if show_scatter and not show_line:
        plt.scatter(time_array, latency_array, alpha=0.3, s=1, label='Latency')
    elif show_line and not show_scatter:
        plt.plot(time_array, latency_array, alpha=0.5, linewidth=0.5, label='Latency')
    else:
        # Both scatter and line
        plt.scatter(time_array, latency_array, alpha=0.2, s=1, label='Latency (points)')
        plt.plot(time_array, latency_array, alpha=0.5, linewidth=0.5, label='Latency (line)')
    
    # Add moving average if window_size is specified
    if window_size and window_size > 1:
        # Sort by time first
        sorted_indices = np.argsort(time_array)
        sorted_time = time_array[sorted_indices]
        sorted_latency = latency_array[sorted_indices]
        
        # Calculate moving average
        if len(sorted_latency) >= window_size:
            moving_avg = np.convolve(sorted_latency, np.ones(window_size)/window_size, mode='valid')
            moving_avg_time = sorted_time[window_size-1:]
            plt.plot(moving_avg_time, moving_avg, 'r-', linewidth=2, 
                    label=f'Moving Average (window={window_size})')
    
    # Add statistics text
    mean_latency = np.mean(latency_array)
    median_latency = np.median(latency_array)
    p95_latency = np.percentile(latency_array, 95)
    p99_latency = np.percentile(latency_array, 99)
    max_latency = np.max(latency_array)
    min_latency = np.min(latency_array)
    
    stats_text = (
        f'Mean: {mean_latency:.2f} ms\n'
        f'Median: {median_latency:.2f} ms\n'
        f'P95: {p95_latency:.2f} ms\n'
        f'P99: {p99_latency:.2f} ms\n'
        f'Min: {min_latency:.2f} ms\n'
        f'Max: {max_latency:.2f} ms'
    )
    
    plt.text(0.02, 0.98, stats_text, transform=plt.gca().transAxes,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            fontsize=9, family='monospace')
    
    # Labels and title
    plt.xlabel('Relative Start Time (seconds)', fontsize=12, fontweight='bold')
    plt.ylabel('Latency (ms)', fontsize=12, fontweight='bold')
    plt.title('End-to-End Latency Over Time (by Start Time)', fontsize=14, fontweight='bold')
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend(loc='best')
    
    # Set reasonable axis limits based on actual data range
    # X-axis: use actual data range with small padding
    x_min = np.min(time_array)
    x_max = np.max(time_array)
    x_padding = (x_max - x_min) * 0.02  # 2% padding
    plt.xlim(left=max(0, x_min - x_padding), right=x_max + x_padding)
    
    # Y-axis: start from 0, but allow some padding at top
    y_max = np.max(latency_array)
    y_padding = y_max * 0.05  # 5% padding
    plt.ylim(bottom=0, top=y_max + y_padding)
    
    plt.tight_layout()
    
    # Save or show
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f'Figure saved to: {output_path}')
    else:
        # Generate default output filename
        csv_path = Path(csv_file)
        output_path = csv_path.parent / f'{csv_path.stem}_plot.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f'Figure saved to: {output_path}')
    
    plt.close()

def main():
    parser = argparse.ArgumentParser(
        description='Plot latency over time from CSV file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Basic plot
  python3 plot_latency_time.py results/e2e_latency_20251222_210024.csv

  # With moving average (window size 100)
  python3 plot_latency_time.py results/e2e_latency_20251222_210024.csv --window 100

  # Line plot only
  python3 plot_latency_time.py results/e2e_latency_20251222_210024.csv --line-only

  # Custom output file
  python3 plot_latency_time.py results/e2e_latency_20251222_210024.csv -o latency_plot.png
        '''
    )
    
    parser.add_argument('csv_file', help='Path to latency CSV file')
    parser.add_argument('-o', '--output', dest='output_file', 
                       help='Output image file path (default: <csv_filename>_plot.png)')
    parser.add_argument('-w', '--window', type=int, dest='window_size',
                       help='Moving average window size for smoothing')
    parser.add_argument('--line-only', action='store_true',
                       help='Show only line plot (no scatter)')
    parser.add_argument('--scatter-only', action='store_true',
                       help='Show only scatter plot (no line)')
    parser.add_argument('--logs-dir', dest='logs_dir',
                       help='Directory containing log files to find first log timestamp (default: logs/)')
    
    args = parser.parse_args()
    
    # Determine plot type
    show_scatter = not args.line_only
    show_line = args.line_only or (not args.scatter_only and not args.line_only)
    
    # Default logs directory
    if args.logs_dir is None:
        # Try to infer from CSV file location
        csv_path = Path(args.csv_file)
        # If CSV is in results/, logs should be in logs/
        if csv_path.parent.name == 'results':
            args.logs_dir = csv_path.parent.parent / 'logs'
        else:
            args.logs_dir = Path('logs')
    
    plot_latency_time(
        args.csv_file,
        output_file=args.output_file,
        window_size=args.window_size,
        show_scatter=show_scatter,
        show_line=show_line,
        logs_dir=args.logs_dir
    )

if __name__ == '__main__':
    main()

