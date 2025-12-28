# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
import re
from statistics import mean
import csv

from benchmark.utils import Print, PathMaker


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )
        
        # Store detailed latency data for CSV export
        self.e2e_latency_details = None
        self.system_start_time = None

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if not log or not log.strip():
            raise ParseError('Client log is empty - process may not have started correctly')
        
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size_match = search(r'Transactions size: (\d+)', log)
        if size_match is None:
            raise ParseError(f'Could not find transaction size in client log. Log content: {log[:200]}')
        size = int(size_match.group(1))
        
        rate_match = search(r'Transactions rate: (\d+)', log)
        if rate_match is None:
            raise ParseError(f'Could not find transaction rate in client log. Log content: {log[:200]}')
        rate = int(rate_match.group(1))

        start_match = search(r'\[(.*Z) .* Start ', log)
        if start_match is None:
            raise ParseError(f'Could not find start time in client log. Log content: {log[:200]}')
        tmp = start_match.group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if not log or not log.strip():
            raise ParseError('Primary log is empty - process may not have started correctly')
        
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        # Try to extract configs, but handle missing values gracefully
        configs = {}
        header_size_match = search(r'Header size .* (\d+)', log)
        if header_size_match:
            configs['header_size'] = int(header_size_match.group(1))
        else:
            raise ParseError(f'Could not find header size in primary log. Log content: {log[:500]}')
        
        max_header_delay_match = search(r'Max header delay .* (\d+)', log)
        if max_header_delay_match:
            configs['max_header_delay'] = int(max_header_delay_match.group(1))
        else:
            raise ParseError(f'Could not find max header delay in primary log. Log content: {log[:500]}')
        
        gc_depth_match = search(r'Garbage collection depth .* (\d+)', log)
        if gc_depth_match:
            configs['gc_depth'] = int(gc_depth_match.group(1))
        else:
            raise ParseError(f'Could not find GC depth in primary log. Log content: {log[:500]}')
        
        sync_retry_delay_match = search(r'Sync retry delay .* (\d+)', log)
        if sync_retry_delay_match:
            configs['sync_retry_delay'] = int(sync_retry_delay_match.group(1))
        else:
            raise ParseError(f'Could not find sync retry delay in primary log. Log content: {log[:500]}')
        
        sync_retry_nodes_match = search(r'Sync retry nodes .* (\d+)', log)
        if sync_retry_nodes_match:
            configs['sync_retry_nodes'] = int(sync_retry_nodes_match.group(1))
        else:
            raise ParseError(f'Could not find sync retry nodes in primary log. Log content: {log[:500]}')
        
        batch_size_match = search(r'Batch size .* (\d+)', log)
        if batch_size_match:
            configs['batch_size'] = int(batch_size_match.group(1))
        else:
            raise ParseError(f'Could not find batch size in primary log. Log content: {log[:500]}')
        
        max_batch_delay_match = search(r'Max batch delay .* (\d+)', log)
        if max_batch_delay_match:
            configs['max_batch_delay'] = int(max_batch_delay_match.group(1))
        else:
            raise ParseError(f'Could not find max batch delay in primary log. Log content: {log[:500]}')

        ip_match = search(r'booted on (\d+.\d+.\d+.\d+)', log)
        if ip_match is None:
            raise ParseError(f'Could not find IP address in primary log. Log content: {log[:500]}')
        ip = ip_match.group(1)
        
        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if not log or not log.strip():
            raise ParseError('Worker log is empty - process may not have started correctly')
        
        # Only check for actual panics, not warnings or connection errors
        # Look for "panicked at" (Rust panic) or "thread.*panicked" patterns
        if search(r'thread.*panicked|panicked at', log, re.IGNORECASE) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip_match = search(r'booted on (\d+.\d+.\d+.\d+)', log)
        if ip_match is None:
            raise ParseError(f'Could not find IP address in worker log. Log content: {log[:500]}')
        ip = ip_match.group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        # Handle cases where log lines might be merged or contain extra content
        # Extract just the timestamp part (format: YYYY-MM-DDTHH:MM:SS.mmmZ or YYYY-MM-DDTHH:MM:SS.mmm+00:00)
        import re
        # Match ISO format timestamp: YYYY-MM-DDTHH:MM:SS.mmmZ or YYYY-MM-DDTHH:MM:SS.mmm+00:00
        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[Z\+])', string)
        if timestamp_match:
            string = timestamp_match.group(1)
        # If still contains extra content, try to extract just the timestamp part
        if ' ' in string or ']' in string or '[' in string:
            # Extract only the timestamp part before any space or bracket
            parts = re.split(r'[\[\]\s]', string)
            for part in parts:
                if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+', part):
                    string = part
                    break
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        latency_details = []  # Store detailed data: (tx_id, start_time, end_time, latency, end_time_relative)
        
        # Calculate system start time (earliest start time from all clients)
        if not self.start:
            return 0, []
        system_start_time = min(self.start)
        self.system_start_time = system_start_time
        
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    lat = end - start
                    latency += [lat]
                    # Store relative times (relative to system start)
                    start_time_relative = start - system_start_time
                    end_time_relative = end - system_start_time
                    latency_details.append({
                        'tx_id': tx_id,
                        'start_time': start,
                        'end_time': end,
                        'latency': lat,
                        'start_time_relative': start_time_relative,
                        'end_time_relative': end_time_relative
                    })
        
        # Sort by end_time_relative for chronological order
        latency_details.sort(key=lambda x: x['end_time_relative'])
        self.e2e_latency_details = latency_details
        
        return mean(latency) if latency else 0, latency_details

    def export_latency_csv(self, filename=None):
        """Export end-to-end latency details to CSV file"""
        if not self.e2e_latency_details:
            Print.warn('No latency details available to export')
            return None
        
        if filename is None:
            # Generate default filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            results_dir = PathMaker.results_path()
            filename = join(results_dir, f'e2e_latency_{timestamp}.csv')
        
        # Ensure directory exists
        import os
        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
        
        try:
            with open(filename, 'w', newline='') as csvfile:
                fieldnames = ['tx_id', 'start_time', 'end_time', 'latency_ms', 'start_time_relative_sec', 'end_time_relative_sec']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for detail in self.e2e_latency_details:
                    writer.writerow({
                        'tx_id': detail['tx_id'],
                        'start_time': detail['start_time'],
                        'end_time': detail['end_time'],
                        'latency_ms': detail['latency'] * 1_000,  # Convert to milliseconds
                        'start_time_relative_sec': detail['start_time_relative'],
                        'end_time_relative_sec': detail['end_time_relative']
                    })
            
            Print.info(f'Latency details exported to: {filename}')
            return filename
        except Exception as e:
            Print.warn(f'Failed to export latency CSV: {e}')
            return None

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency, _ = self._end_to_end_latency()
        end_to_end_latency = end_to_end_latency * 1_000

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)
