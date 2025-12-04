# Copyright(C) Facebook, Inc. and its affiliates.
"""
CloudLab Remote Benchmark

This module provides functionality to run benchmarks on CloudLab nodes.
"""

from collections import OrderedDict
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from time import sleep
from math import ceil
from copy import deepcopy
import subprocess
import re

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.cloudlab_instance import CloudLabInstanceManager


class FabricError(Exception):
    """Wrapper for Fabric exception with a meaningful error message."""
    
    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class CloudLabBench:
    """Benchmark runner for CloudLab nodes"""
    
    def __init__(self, ctx):
        self.manager = CloudLabInstanceManager.make()
        self.settings = self.manager.settings
        
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)
    
    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)
    
    def _get_connection_kwargs(self, host_info):
        """Get connection kwargs for a specific host (without port/timeout, passed separately)"""
        # Create a new dict from connect_kwargs (which is a Config object)
        # Don't include port or timeout here - they will be passed as separate parameters
        kwargs = dict(self.connect)
        # Remove port and timeout if they exist to avoid ambiguity
        kwargs.pop('port', None)
        kwargs.pop('timeout', None)
        kwargs.pop('connect_timeout', None)
        return kwargs
    
    def install(self):
        """Install Rust and clone the repo on all CloudLab nodes"""
        Print.info('Installing rust and cloning the repo...')
        
        host_info = self.manager.get_host_info()
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',
            # Add cargo to PATH permanently
            'echo "export PATH=\\$HOME/.cargo/bin:\\$PATH" >> $HOME/.bashrc',
            'echo "export PATH=\\$HOME/.cargo/bin:\\$PATH" >> $HOME/.profile',
            'sudo apt-get install -y clang',
            f'(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))'
        ]
        
        try:
            # Since each host may have different ports, we need to handle them individually
            # Group all hosts with same connection settings first
            hosts_by_config = {}
            for host in host_info:
                username = host.get('username', 'root')
                hostname = host['hostname']
                port = host.get('port', 22)
                key = (username, port)
                if key not in hosts_by_config:
                    hosts_by_config[key] = []
                hosts_by_config[key].append(hostname)
            
            # Run commands on each group
            for (username, port), hostnames in hosts_by_config.items():
                conn_kwargs = self._get_connection_kwargs({})
                
                # Pass port and timeout as separate parameters, not in connect_kwargs
                # Try connecting to each host individually first to catch connection issues
                valid_hostnames = []
                for hostname in hostnames:
                    try:
                        Print.info(f'Connecting to {username}@{hostname}:{port}...')
                        test_conn = Connection(hostname, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
                        test_conn.open()
                        test_conn.close()
                        valid_hostnames.append(hostname)
                        Print.info(f'  ✓ Successfully connected to {hostname}:{port}')
                    except Exception as e:
                        Print.warn(f'  ✗ Failed to connect to {hostname}:{port} - {e}')
                        Print.warn(f'    Skipping this host. Please check SSH connection manually.')
                
                # Now run on the valid hosts in the group
                if valid_hostnames:
                    Print.info(f'Running commands on {len(valid_hostnames)} host(s)...')
                    g = Group(*valid_hostnames, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
                    g.run(' && '.join(cmd), hide=True)
                else:
                    Print.warn(f'No valid hosts for {username}@{port}, skipping...')
            
            Print.heading(f'Initialized testbed of {len(host_info)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)
    
    def status(self, hosts=[]):
        """Check if benchmark processes are running on specified hosts"""
        assert isinstance(hosts, list)
        
        host_info = self.manager.get_host_info()
        host_dict = {h['hostname']: h for h in host_info}
        
        Print.heading('Checking benchmark status on CloudLab nodes...')
        
        # Commands to check running processes
        check_cmd = '''
            echo "=== Node Status ===" && 
            echo "Hostname: $(hostname)" &&
            echo "---" &&
            echo "Tmux sessions:" &&
            (tmux ls 2>/dev/null || echo "  No tmux sessions") &&
            echo "---" &&
            echo "Running processes:" &&
            (pgrep -f "node.*primary" > /dev/null && echo "  ✓ Primary: running" || echo "  ✗ Primary: not running") &&
            (pgrep -f "node.*worker" > /dev/null && echo "  ✓ Worker: running" || echo "  ✗ Worker: not running") &&
            (pgrep -f "benchmark_client" > /dev/null && echo "  ✓ Client: running" || echo "  ✗ Client: not running") &&
            echo "---" &&
            echo "Process count:" &&
            echo "  Primary: $(pgrep -f 'node.*primary' | wc -l)" &&
            echo "  Worker: $(pgrep -f 'node.*worker' | wc -l)" &&
            echo "  Client: $(pgrep -f 'benchmark_client' | wc -l)"
        '''
        
        try:
            if not hosts:
                # Check all hosts
                hosts_by_config = {}
                for host in host_info:
                    username = host.get('username', 'root')
                    hostname = host['hostname']
                    port = host.get('port', 22)
                    key = (username, port)
                    if key not in hosts_by_config:
                        hosts_by_config[key] = []
                    hosts_by_config[key].append(hostname)
                
                # Check each group
                for (username, port), hostnames in hosts_by_config.items():
                    conn_kwargs = self._get_connection_kwargs({})
                    g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
                    Print.info(f'Checking {len(hostnames)} host(s) at {username}@{port}...')
                    try:
                        results = g.run(check_cmd, hide=False)
                        # Print results for each host
                        if isinstance(results, dict):
                            for hostname, result in results.items():
                                Print.info(f'\n--- {hostname} ---')
                                if result.stdout:
                                    print(result.stdout)
                        else:
                            if results.stdout:
                                print(results.stdout)
                    except Exception as e:
                        Print.warn(f'Failed to check status: {e}')
            else:
                # Check specific hosts
                hosts_by_config = {}
                for h in hosts:
                    if '@' in h:
                        username, hostname = h.split('@')
                    else:
                        hostname = h
                        username = 'root'
                    
                    host = host_dict.get(hostname, {})
                    username = host.get('username', username)
                    port = host.get('port', 22)
                    key = (username, port)
                    if key not in hosts_by_config:
                        hosts_by_config[key] = []
                    hosts_by_config[key].append(hostname)
                
                for (username, port), hostnames in hosts_by_config.items():
                    conn_kwargs = self._get_connection_kwargs({})
                    g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
                    Print.info(f'Checking {len(hostnames)} host(s) at {username}@{port}...')
                    try:
                        results = g.run(check_cmd, hide=False)
                        if isinstance(results, dict):
                            for hostname, result in results.items():
                                Print.info(f'\n--- {hostname} ---')
                                if result.stdout:
                                    print(result.stdout)
                        else:
                            if results.stdout:
                                print(results.stdout)
                    except Exception as e:
                        Print.warn(f'Failed to check status: {e}')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            Print.warn(f'Some hosts failed to respond: {e}')
    
    def kill(self, hosts=[], delete_logs=False):
        """Stop execution on specified hosts"""
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        
        host_info = self.manager.get_host_info()
        host_dict = {h['hostname']: h for h in host_info}
        delete_logs_cmd = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs_cmd, f'({CommandMaker.kill()} || true)']
        
        try:
            if not hosts:
                # Group hosts by (username, port) to use Group efficiently
                hosts_by_config = {}
                for host in host_info:
                    username = host.get('username', 'root')
                    hostname = host['hostname']
                    port = host.get('port', 22)
                    key = (username, port)
                    if key not in hosts_by_config:
                        hosts_by_config[key] = []
                    hosts_by_config[key].append(hostname)
                
                # Run on each group
                for (username, port), hostnames in hosts_by_config.items():
                    conn_kwargs = self._get_connection_kwargs({})
                    g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs)
                    g.run(' && '.join(cmd), hide=True)
            else:
                # Handle specific hosts
                hosts_by_config = {}
                for h in hosts:
                    # Extract hostname from host string
                    if '@' in h:
                        username, hostname = h.split('@')
                    else:
                        hostname = h
                        username = 'root'
                    
                    # Find host info
                    host = host_dict.get(hostname, {})
                    username = host.get('username', username)
                    port = host.get('port', 22)
                    key = (username, port)
                    if key not in hosts_by_config:
                        hosts_by_config[key] = []
                    hosts_by_config[key].append(hostname)
                
                # Run on each group
                for (username, port), hostnames in hosts_by_config.items():
                    conn_kwargs = self._get_connection_kwargs({})
                    g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs)
                    g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))
    
    def _select_hosts(self, bench_parameters):
        """Select hosts based on benchmark parameters"""
        host_info = self.manager.get_host_info()
        
        # Collocate primary and workers on the same machine
        if bench_parameters.collocate:
            nodes = max(bench_parameters.nodes)
            if len(host_info) < nodes:
                raise BenchError(f'Not enough hosts: need {nodes}, have {len(host_info)}')
            return host_info[:nodes]
        else:
            # One node per machine (primary + workers on separate machines)
            nodes = max(bench_parameters.nodes)
            workers = bench_parameters.workers
            total_machines = nodes * (1 + workers)  # primary + workers
            if len(host_info) < total_machines:
                raise BenchError(f'Not enough hosts: need {total_machines}, have {len(host_info)}')
            return host_info[:total_machines]
    
    def _modify_attack_rs(self, hosts, trigger_attack):
        """Modify TRIGGER_NETWORK_INTERRUPT in adversary/src/attack.rs on remote hosts"""
        if trigger_attack is None:
            return  # No modification needed
        
        Print.info(f'Modifying TRIGGER_NETWORK_INTERRUPT to {trigger_attack} on all nodes...')
        repo_name = self.settings.repo_name
        attack_rs_path = f'{repo_name}/adversary/src/attack.rs'
        
        # Use sed command instead of Python script to avoid escaping issues
        trigger_value_str = 'true' if trigger_attack else 'false'
        # sed command to replace the value (match the specific line pattern)
        # Match: pub const TRIGGER_NETWORK_INTERRUPT: bool = false;
        # or:    pub const TRIGGER_NETWORK_INTERRUPT: bool = true;
        # Replace with the desired value
        sed_cmd = f"sed -i 's/TRIGGER_NETWORK_INTERRUPT: bool = true/TRIGGER_NETWORK_INTERRUPT: bool = {trigger_value_str}/; s/TRIGGER_NETWORK_INTERRUPT: bool = false/TRIGGER_NETWORK_INTERRUPT: bool = {trigger_value_str}/' {attack_rs_path}"
        
        try:
            # Group hosts by (username, port) to use Group efficiently
            hosts_by_config = {}
            for host in hosts:
                username = host.get('username', 'root')
                hostname = host['hostname']
                port = host.get('port', 22)
                key = (username, port)
                if key not in hosts_by_config:
                    hosts_by_config[key] = []
                hosts_by_config[key].append(hostname)
            
            # Run modification command on each group
            for (username, port), hostnames in hosts_by_config.items():
                conn_kwargs = self._get_connection_kwargs({})
                g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
                # Verify file exists and modify it
                cmd = f'test -f {attack_rs_path} && {sed_cmd} && echo "Successfully modified {attack_rs_path}" || (echo "Error: {attack_rs_path} not found" && exit 1)'
                g.run(cmd, hide=True)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to modify attack.rs', e)
    
    def _update(self, hosts, collocate, trigger_attack=None):
        """Update code on all hosts"""
        Print.info('Updating code on all nodes...')
        repo_name = self.settings.repo_name
        branch = self.settings.branch
        
        # Modify attack.rs if trigger_attack is specified
        if trigger_attack is not None:
            self._modify_attack_rs(hosts, trigger_attack)
        
        cmd = [
            f'cd {repo_name}',
            'git fetch',
            f'git checkout {branch}',
            'git pull',
            # Source cargo environment before building
            'source $HOME/.cargo/env || export PATH=$HOME/.cargo/bin:$PATH',
            'cargo build --release --features benchmark'
        ]
        
        try:
            # Group hosts by (username, port) to use Group efficiently
            hosts_by_config = {}
            for host in hosts:
                username = host.get('username', 'root')
                hostname = host['hostname']
                port = host.get('port', 22)
                key = (username, port)
                if key not in hosts_by_config:
                    hosts_by_config[key] = []
                hosts_by_config[key].append(hostname)
            
            # Run commands on each group
            for (username, port), hostnames in hosts_by_config.items():
                conn_kwargs = self._get_connection_kwargs({})
                g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
                g.run(' && '.join(cmd), hide=True)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)
    
    def _config(self, hosts, node_parameters, bench_parameters):
        """Generate and upload configuration files"""
        Print.info('Generating configuration files...')
        
        # Generate keys and committee
        keys = []
        for i in range(len(hosts)):
            key_file = PathMaker.key_file(i)
            key = Key.from_file(key_file) if key_file.exists() else Key()
            key.to_file(key_file)
            keys += [key]
        
        # Create addresses dict for Committee
        # Format: {name: [primary_host, worker1_host, worker2_host, ...]}
        addresses = OrderedDict()
        for i, key in enumerate(keys):
            host = hosts[i]
            hostname = host['hostname']
            # Remove port if present
            if ':' in hostname:
                hostname = hostname.split(':')[0]
            
            # For collocated setup, primary and workers are on the same host
            if bench_parameters.collocate:
                worker_hosts = [hostname] * bench_parameters.workers
            else:
                # For non-collocated, each worker is on a different host
                # This is simplified - you may need to adjust based on your setup
                worker_hosts = [hostname] * bench_parameters.workers
            
            addresses[key.name] = [hostname] + worker_hosts
        
        committee = Committee(addresses, self.settings.base_port)
        committee.save(PathMaker.committee_file())
        
        node_parameters.save(PathMaker.parameters_file())
        
        # Upload files to all hosts
        repo_name = self.settings.repo_name
        files_to_upload = [
            (PathMaker.committee_file(), f'{repo_name}/.committee.json'),
            (PathMaker.parameters_file(), f'{repo_name}/.parameters.json'),
        ]
        
        # Upload keys
        for i, key in enumerate(keys):
            files_to_upload.append(
                (PathMaker.key_file(i), f'{repo_name}/{PathMaker.key_file(i)}')
            )
        
        Print.info('Uploading configuration files...')
        try:
            for host in hosts:
                username = host.get('username', 'root')
                hostname = host['hostname']
                port = host.get('port', 22)
                conn_kwargs = self._get_connection_kwargs({})
                conn = Connection(hostname, user=username, port=port, connect_kwargs=conn_kwargs)
                for local, remote in files_to_upload:
                    conn.put(local, remote)
        except Exception as e:
            raise BenchError('Failed to upload configuration files', e)
        
        return committee
    
    def _logs(self, committee, faults):
        """Download logs from all hosts"""
        Print.info('Downloading logs...')
        
        # Create local logs directory
        PathMaker.logs_path().mkdir(parents=True, exist_ok=True)
        
        # Download logs from each host
        repo_name = self.settings.repo_name
        host_info = self.manager.get_host_info()
        
        for i, host in enumerate(host_info):
            hostname = host['hostname']
            username = host.get('username', 'root')
            port = host.get('port', 22)
            conn_kwargs = self._get_connection_kwargs({})
            
            try:
                conn = Connection(hostname, user=username, port=port, connect_kwargs=conn_kwargs)
                
                # Download primary logs
                remote_log = f'{repo_name}/{PathMaker.primary_log_file(i)}'
                local_log = PathMaker.primary_log_file(i)
                try:
                    conn.get(remote_log, str(local_log))
                except:
                    pass  # Log file might not exist
                
                # Download worker logs
                for j in range(committee.workers()):
                    remote_log = f'{repo_name}/{PathMaker.worker_log_file(i, j)}'
                    local_log = PathMaker.worker_log_file(i, j)
                    try:
                        conn.get(remote_log, str(local_log))
                    except:
                        pass
                
                # Download client logs
                remote_log = f'{repo_name}/{PathMaker.client_log_file(i, 0)}'
                local_log = PathMaker.client_log_file(i, 0)
                try:
                    conn.get(remote_log, str(local_log))
                except:
                    pass
            except Exception as e:
                Print.warn(f'Failed to download logs from {hostname}: {e}')
        
        return LogParser.process(PathMaker.logs_path(), faults=faults)
    
    def _background_run(self, host_info, command, log_file):
        """Run a command in the background using tmux on a remote host"""
        from os.path import basename, splitext
        name = splitext(basename(log_file))[0]
        repo_name = self.settings.repo_name
        remote_log = f'{repo_name}/{log_file}'
        
        # Ensure log directory exists and change to repo directory
        # The command should be executed in the repo directory where binaries are located
        log_dir = f'{repo_name}/$(dirname {log_file})'
        full_cmd = f'cd {repo_name} && mkdir -p {log_dir} && {command} 2>&1 | tee {remote_log}'
        cmd = f'tmux new -d -s "{name}" "bash -c \\"{full_cmd}\\""'
        
        username = host_info.get('username', 'root')
        hostname = host_info['hostname']
        port = host_info.get('port', 22)
        conn_kwargs = self._get_connection_kwargs({})
        
        Print.info(f'Starting {name} on {hostname}...')
        c = Connection(hostname, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
        try:
            # First verify the repo directory and binaries exist
            test_cmd = f'cd {repo_name} && test -f node && test -f benchmark_client && echo "Binaries found" || echo "Binaries missing"'
            test_result = c.run(test_cmd, hide=True)
            if 'Binaries missing' in test_result.stdout:
                Print.warn(f'  ⚠ Binaries not found in {repo_name} on {hostname}')
            
            # Now start the process
            output = c.run(cmd, hide=True)
            self._check_stderr(output)
            
            # Verify tmux session was created
            verify_cmd = f'tmux has-session -t "{name}" 2>/dev/null && echo "Session exists" || echo "Session missing"'
            verify_result = c.run(verify_cmd, hide=True)
            if 'Session exists' in verify_result.stdout:
                Print.info(f'  ✓ {name} started on {hostname} (tmux session: {name})')
            else:
                Print.warn(f'  ⚠ {name} command executed but tmux session not found on {hostname}')
        except Exception as e:
            Print.error(f'  ✗ Failed to start {name} on {hostname}: {e}')
            raise
    
    def _get_host_by_address(self, address, selected_hosts):
        """Get host info by extracting IP from address"""
        from benchmark.config import Committee
        
        # Address format: "ip:port" or "hostname:port"
        ip = Committee.ip(address)
        
        # Try to match by IP
        for host in selected_hosts:
            hostname = host['hostname']
            # Remove port if present in hostname
            if ':' in hostname:
                host_ip = hostname.split(':')[0]
            else:
                host_ip = hostname
            
            # Try direct IP match
            if host_ip == ip:
                return host
            
            # Try DNS resolution (if hostname is not an IP)
            try:
                import socket
                resolved_ip = socket.gethostbyname(host_ip)
                if resolved_ip == ip:
                    return host
            except:
                pass
        
        # Fallback: use round-robin based on address index
        # This is a simple approach - assumes addresses are in order
        if selected_hosts:
            # Extract index from address by checking committee structure
            # For now, use first available host (this should be improved)
            return selected_hosts[0]
        
        return None
    
    def _run_single(self, rate, committee, bench_parameters, selected_hosts, debug=False):
        """Run a single benchmark iteration"""
        from math import ceil
        from time import sleep
        
        faults = bench_parameters.faults
        
        Print.info('Killing any existing processes...')
        # Kill any potentially unfinished run and delete logs
        self.kill(hosts=selected_hosts, delete_logs=True)
        
        # Small delay to ensure processes are killed
        sleep(2)
        
        # Run the clients (they will wait for the nodes to be ready)
        Print.info('Booting clients...')
        workers_addresses = committee.workers_addresses(faults)
        rate_share = ceil(rate / committee.workers())
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host_info = self._get_host_by_address(address, selected_hosts)
                if not host_info:
                    Print.warn(f'Could not find host for address {address}')
                    continue
                
                cmd = CommandMaker.run_client(
                    address,
                    bench_parameters.tx_size,
                    rate_share,
                    [x for y in workers_addresses for _, x in y]
                )
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(host_info, cmd, log_file)
        
        # Run the primaries (except the faulty ones)
        Print.info('Booting primaries...')
        for i, address in enumerate(committee.primary_addresses(faults)):
            host_info = self._get_host_by_address(address, selected_hosts)
            if not host_info:
                Print.warn(f'Could not find host for address {address}')
                continue
            
            # Use relative paths since we'll cd into repo directory in _background_run
            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                '.committee.json',
                PathMaker.db_path(i),
                '.parameters.json',
                debug=debug
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(host_info, cmd, log_file)
        
        # Run the workers (except the faulty ones)
        Print.info('Booting workers...')
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host_info = self._get_host_by_address(address, selected_hosts)
                if not host_info:
                    Print.warn(f'Could not find host for address {address}')
                    continue
                
                repo_name = self.settings.repo_name
                # Use relative paths since we'll cd into repo directory
                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(i),
                    '.committee.json',
                    PathMaker.db_path(i, id),
                    '.parameters.json',
                    id,  # The worker's id
                    debug=debug
                )
                log_file = PathMaker.worker_log_file(i, id)
                self._background_run(host_info, cmd, log_file)
        
        # Wait for all transactions to be processed
        duration = bench_parameters.duration
        Print.info(f'Running benchmark ({duration} sec)...')
        for i in range(20):
            sleep(ceil(duration / 20))
            if (i + 1) % 5 == 0:
                Print.info(f'  Progress: {((i + 1) * 100) // 20}%')
        
        self.kill(hosts=selected_hosts, delete_logs=False)
    
    def _check_stderr(self, output):
        """Check for errors in command output"""
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)
    
    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        """Run benchmarks on CloudLab nodes
        
        Args:
            bench_parameters_dict: Benchmark parameters (may include 'trigger_attack' as bool or list)
            node_parameters_dict: Node parameters
            debug: Enable debug mode
        """
        assert isinstance(debug, bool)
        Print.heading('Starting CloudLab benchmark')
        
        # Extract trigger_attack from bench_parameters_dict (optional)
        # Support both single value and list (like rate and nodes)
        trigger_attack_raw = bench_parameters_dict.get('trigger_attack', None)
        if trigger_attack_raw is not None:
            # Convert to list if it's a single value
            if isinstance(trigger_attack_raw, list):
                trigger_attack_list = trigger_attack_raw
            else:
                trigger_attack_list = [trigger_attack_raw]
        else:
            trigger_attack_list = [None]  # Default: don't modify
        
        # Remove trigger_attack from dict before creating BenchParameters
        # (since it's not a standard parameter)
        bench_params_for_parsing = {k: v for k, v in bench_parameters_dict.items() if k != 'trigger_attack'}
        
        try:
            bench_parameters = BenchParameters(bench_params_for_parsing)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)
        
        # Select which hosts to use
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return
        
        # Run benchmarks for each combination of parameters
        for n in bench_parameters.nodes:
            for rate in bench_parameters.rate:
                for trigger_attack in trigger_attack_list:
                    # Update nodes (this will also modify attack.rs if trigger_attack is specified)
                    try:
                        if trigger_attack is not None:
                            Print.heading(f'\nConfiguring nodes: attack={"ENABLED" if trigger_attack else "DISABLED"}')
                        self._update(selected_hosts, bench_parameters.collocate, trigger_attack=trigger_attack)
                    except (GroupException, ExecutionError) as e:
                        e = FabricError(e) if isinstance(e, GroupException) else e
                        raise BenchError('Failed to update nodes', e)
                    
                    # Upload all configuration files
                    try:
                        committee = self._config(
                            selected_hosts, node_parameters, bench_parameters
                        )
                    except (subprocess.SubprocessError, GroupException) as e:
                        e = FabricError(e) if isinstance(e, GroupException) else e
                        raise BenchError('Failed to configure nodes', e)
                    
                    # Create a copy of committee with only n nodes
                    from copy import deepcopy
                    committee_copy = deepcopy(committee)
                    committee_copy.remove_nodes(committee.size() - n)
                    
                    # Run benchmarks for this configuration
                    for run in range(bench_parameters.runs):
                        attack_str = f", attack={'ON' if trigger_attack else 'OFF'}" if trigger_attack is not None else ""
                        Print.heading(f'\nRunning benchmark: nodes={n}, rate={rate}{attack_str}, run={run+1}/{bench_parameters.runs}')
                        
                        try:
                            # Run the actual benchmark
                            self._run_single(
                                rate, committee_copy, bench_parameters, selected_hosts, debug
                            )
                            
                            # Download and parse logs
                            result = self._logs(committee_copy, bench_parameters.faults)
                            result.print(PathMaker.result_file(
                                bench_parameters.faults,
                                n,
                                bench_parameters.workers,
                                bench_parameters.collocate,
                                rate,
                                bench_parameters.tx_size,
                            ))
                        except (subprocess.SubprocessError, GroupException, ParseError) as e:
                            self.kill(hosts=selected_hosts)
                            if isinstance(e, GroupException):
                                e = FabricError(e)
                            Print.error(BenchError('Benchmark failed', e))
                            continue
        
        Print.heading('All benchmarks completed')

