#!/usr/bin/env python3
"""
Script to run CloudLab benchmark and post-process logs.

Generated artifacts are stored under:
result_decouple/<network_tag>/<workload_tag>/
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

# Add benchmark directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmark.logs import LogParser, ParseError
from benchmark.utils import PathMaker, Print

try:
    from time_storage_from_logs import process_node_log, export_round_end_pivot_table

    TIME_STORAGE_AVAILABLE = True
except ImportError:
    TIME_STORAGE_AVAILABLE = False


def _default_run_context():
    return {
        'network_tag': 'default_network',
        'workload_tag': 'default_workload',
        'nodes': 10,
    }


def load_run_context():
    context_path = Path(PathMaker.run_context_file())
    if context_path.exists():
        try:
            with context_path.open('r') as handle:
                return json.load(handle)
        except Exception as e:
            Print.warn(f'Failed to load run context from {context_path}: {e}')
    return _default_run_context()


def _results_dir(run_context):
    directory = Path(
        PathMaker.tagged_results_path(
            run_context['network_tag'],
            run_context['workload_tag'],
        )
    )
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _summary_path(run_context):
    return Path(
        PathMaker.summary_file(
            run_context['network_tag'],
            run_context['workload_tag'],
        )
    )


def _analysis_path(run_context, experiment_group=None):
    return Path(
        PathMaker.analysis_csv_file(
            run_context['network_tag'],
            run_context['workload_tag'],
            experiment_group=experiment_group,
        )
    )


def _pivot_path(run_context, experiment_group=None):
    return Path(
        PathMaker.pivot_csv_file(
            run_context['network_tag'],
            run_context['workload_tag'],
            experiment_group=experiment_group,
        )
    )


def _metadata_path(run_context):
    return Path(
        PathMaker.metadata_file(
            run_context['network_tag'],
            run_context['workload_tag'],
        )
    )


def _annotate_summary_with_run_context(summary_text, run_context):
    config_marker = ' + CONFIG:\n'
    if config_marker not in summary_text:
        return summary_text

    context_lines = []

    network_tag = run_context.get('network_tag')
    if network_tag:
        context_lines.append(f' Network tag: {network_tag}\n')

    workload_tag = run_context.get('workload_tag')
    if workload_tag:
        context_lines.append(f' Workload tag: {workload_tag}\n')

    rate_type = run_context.get('rate_type')
    if rate_type:
        context_lines.append(f' Rate type: {rate_type}\n')

    run_index = run_context.get('run_index')
    runs_total = run_context.get('runs_total')
    if run_index is not None and runs_total is not None:
        context_lines.append(f' Run index: {run_index}/{runs_total}\n')

    if not context_lines:
        return summary_text

    return summary_text.replace(
        config_marker,
        config_marker + ''.join(context_lines),
        1,
    )


def run_fab_command(task='cloudlab_remote', debug=False):
    fab_cmd = ['fab', task]
    if debug:
        fab_cmd.append('debug=True')

    Print.info(f'Running: {" ".join(fab_cmd)}')
    Print.info('=' * 60)

    try:
        result = subprocess.run(
            fab_cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            check=False,
        )
        return result.returncode == 0
    except FileNotFoundError:
        Print.warn('fab command not found. Please install fabric: pip install fabric')
        return False
    except Exception as e:
        Print.warn(f'Failed to run fab command: {e}')
        return False


def download_logs_if_needed(settings_file='cloudlab_settings.json', max_workers=1):
    logs_dir = Path(PathMaker.logs_path())

    primary_logs = list(logs_dir.glob('primary-*.log'))
    worker_logs = list(logs_dir.glob('worker-*.log'))
    client_logs = list(logs_dir.glob('client-*.log'))

    if primary_logs or worker_logs or client_logs:
        Print.info(
            f'Found existing logs: {len(primary_logs)} primary, '
            f'{len(worker_logs)} worker, {len(client_logs)} client'
        )
        return True

    Print.info('No local logs found, attempting to download from remote nodes...')
    try:
        from download_logs import download_logs

        return download_logs(settings_file, max_workers)
    except ImportError:
        Print.warn('download_logs.py not found, skipping download')
        return False


def process_logs(run_context, faults=0, save_to_file=True, logs_dir=None):
    logs_dir = logs_dir or PathMaker.logs_path()

    if not os.path.exists(logs_dir):
        Print.warn(f'Logs directory not found: {logs_dir}')
        return False

    Print.info('=' * 60)
    Print.info('Processing logs...')
    Print.info('=' * 60)

    try:
        parser = LogParser.process(logs_dir, faults=faults)
        result = _annotate_summary_with_run_context(parser.result(), run_context)
        print(result)

        if save_to_file:
            _results_dir(run_context)
            summary_file = _summary_path(run_context)
            summary_file.write_text(result)

            metadata_file = _metadata_path(run_context)
            metadata_file.write_text(json.dumps(run_context, indent=2) + '\n')

            Print.info(f'\nResults saved to: {summary_file}')

        return True
    except ParseError as e:
        Print.warn(f'Failed to parse logs: {e}')
        Print.warn('This may be because some log files are empty or incomplete.')
        return False
    except Exception as e:
        Print.warn(f'Error processing logs: {e}')
        return False


def generate_round_end_time_pivot(run_context, num_nodes=10, experiment_group=None, logs_dir=None):
    if not TIME_STORAGE_AVAILABLE:
        Print.warn('time_storage_from_logs module not available, skipping pivot table generation')
        return False

    logs_dir = logs_dir or PathMaker.logs_path()
    csv_filename = _analysis_path(run_context, experiment_group=experiment_group)
    pivot_filename = _pivot_path(run_context, experiment_group=experiment_group)

    Print.info('=' * 60)
    Print.info('Generating CSV artifacts...')
    Print.info('=' * 60)

    try:
        benchmark_dir = os.path.dirname(os.path.abspath(__file__))
        original_cwd = os.getcwd()
        os.chdir(benchmark_dir)

        try:
            _results_dir(run_context)

            if csv_filename.exists():
                csv_filename.unlink()

            Print.info(f'Processing {num_nodes} nodes from logs directory: {logs_dir}')
            Print.info('')

            for node_id in range(num_nodes):
                process_node_log(node_id, str(csv_filename), num_nodes, logs_dir=logs_dir)

            Print.info('=' * 60)
            Print.info(f'Analysis complete! Results saved to: {csv_filename}')

            Print.info('\nGenerating round end time pivot table...')
            export_round_end_pivot_table(str(csv_filename), str(pivot_filename))

            Print.info(f'Round end time pivot table saved to: {pivot_filename}')
            Print.info('=' * 60)
            return True
        finally:
            os.chdir(original_cwd)
    except Exception as e:
        Print.warn(f'Error generating CSV artifacts: {e}')
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Run CloudLab benchmark and process logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--no-run',
        action='store_true',
        help='Skip running fab cloudlab_remote, only process existing logs',
    )
    parser.add_argument(
        '--download-only',
        action='store_true',
        help='Only download logs from remote nodes, do not run benchmark or process',
    )
    parser.add_argument('--debug', action='store_true', help='Run benchmark in debug mode')
    parser.add_argument('--faults', type=int, default=0, help='Number of faulty nodes (default: 0)')
    parser.add_argument('--no-save', action='store_true', help='Do not save results to file')
    parser.add_argument(
        '--max-workers',
        type=int,
        default=1,
        help='Maximum number of workers per node for log download (default: 1)',
    )
    parser.add_argument(
        '--settings',
        default='cloudlab_settings.json',
        help='Path to CloudLab settings file (default: cloudlab_settings.json)',
    )
    parser.add_argument(
        '--num-nodes',
        type=int,
        default=10,
        help='Number of nodes to process for pivot table (default: 10)',
    )
    parser.add_argument(
        '--experiment-groups',
        type=int,
        nargs='+',
        default=None,
        help='Experiment group numbers to process (e.g., --experiment-groups 1 2 3).',
    )
    parser.add_argument('--no-pivot', action='store_true', help='Skip generating round_end_time_pivot.csv')
    parser.add_argument(
        '--logs-dir',
        default=None,
        help='Custom logs directory path (default: uses PathMaker.logs_path() or "logs")',
    )

    args = parser.parse_args()

    Print.heading('CloudLab Benchmark Runner')
    Print.info('=' * 60)

    run_context = load_run_context()
    success = True

    if not args.no_run and not args.download_only:
        success = run_fab_command('cloudlab_remote', debug=args.debug)
        if not success:
            Print.warn('Benchmark run completed with errors, but continuing to process logs...')
        run_context = load_run_context()

    if not args.download_only:
        download_logs_if_needed(args.settings, args.max_workers)
    else:
        Print.info('Download-only mode: downloading logs from remote nodes...')
        download_logs_if_needed(args.settings, args.max_workers)
        Print.info('Download complete. Exiting.')
        return 0

    if not args.no_save:
        success = process_logs(
            run_context,
            faults=args.faults,
            save_to_file=True,
            logs_dir=args.logs_dir,
        ) and success
    else:
        success = process_logs(
            run_context,
            faults=args.faults,
            save_to_file=False,
            logs_dir=args.logs_dir,
        ) and success

    if not args.no_pivot:
        if args.experiment_groups:
            Print.info('=' * 60)
            Print.info(f'Processing {len(args.experiment_groups)} experiment group(s)...')
            Print.info('=' * 60)

            for exp_group in args.experiment_groups:
                Print.info(f'\nProcessing experiment group {exp_group}...')
                exp_logs_dir = args.logs_dir
                if exp_logs_dir is None:
                    candidate = f'logs_exp{exp_group}'
                    exp_logs_dir = candidate if os.path.exists(candidate) else None

                exp_success = generate_round_end_time_pivot(
                    run_context,
                    num_nodes=args.num_nodes,
                    experiment_group=exp_group,
                    logs_dir=exp_logs_dir,
                )
                success = exp_success and success
        else:
            pivot_success = generate_round_end_time_pivot(
                run_context,
                num_nodes=args.num_nodes,
                logs_dir=args.logs_dir,
            )
            success = pivot_success and success

    Print.info('=' * 60)
    if success:
        Print.info('✓ All operations completed successfully')
        return 0

    Print.warn('⚠ Some operations completed with errors')
    return 1


if __name__ == '__main__':
    sys.exit(main())

