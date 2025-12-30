# Copyright(C) Facebook, Inc. and its affiliates.
"""
CloudLab Remote Benchmark

This module provides functionality to run benchmarks on CloudLab nodes.
"""

from collections import OrderedDict
from pathlib import Path
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from time import sleep
from math import ceil
from copy import deepcopy
import subprocess
import re
import shlex
import sys
import shutil

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
            # Try to load key without password first
            try:
                ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                    self.manager.settings.key_path
                )
            except PasswordRequiredException:
                # Key is password-protected, try to get password
                import os
                password = os.environ.get('SSH_KEY_PASSWORD')
                
                # Try to get password from cloudlab_settings.json if it exists
                if not password:
                    try:
                        import json
                        settings_file = Path(__file__).parent.parent / 'cloudlab_settings.json'
                        if settings_file.exists():
                            with open(settings_file, 'r') as f:
                                settings_data = json.load(f)
                                password = settings_data.get('ssh_key_password')
                    except Exception:
                        pass
                
                if password:
                    ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                        self.manager.settings.key_path,
                        password=password
                    )
                else:
                    raise BenchError(
                        'SSH key is password-protected. Please provide password via SSH_KEY_PASSWORD environment variable or ssh_key_password in cloudlab_settings.json',
                        PasswordRequiredException('private key file is encrypted')
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
    
    def test_connections(self):
        """Test SSH connections to all CloudLab nodes"""
        Print.heading('Testing connections to CloudLab nodes...')
        
        host_info = self.manager.get_host_info()
        conn_kwargs = self._get_connection_kwargs({})
        
        results = {
            'success': [],
            'failed': []
        }
        
        for host in host_info:
            username = host.get('username', 'root')
            hostname = host['hostname']
            port = host.get('port', 22)
            region = host.get('region', 'default')
            
            Print.info(f'Testing {username}@{hostname}:{port} ({region})...')
            
            try:
                # Test basic connection
                test_conn = Connection(
                    hostname, 
                    user=username, 
                    port=port, 
                    connect_kwargs=conn_kwargs, 
                    connect_timeout=10
                )
                test_conn.open()
                
                # Test basic command execution
                result = test_conn.run('echo "Connection test successful" && hostname && uname -a', hide=True)
                test_conn.close()
                
                # Parse output
                output_lines = result.stdout.strip().split('\n')
                hostname_line = output_lines[1] if len(output_lines) > 1 else "N/A"
                system_line = output_lines[-1] if len(output_lines) > 0 else result.stdout.strip()
                
                Print.info(f'  ✓ Connected successfully')
                Print.info(f'    Hostname: {hostname_line}')
                Print.info(f'    System: {system_line}')
                
                results['success'].append({
                    'hostname': hostname,
                    'username': username,
                    'port': port,
                    'region': region
                })
            except Exception as e:
                Print.warn(f'  ✗ Connection failed: {e}')
                results['failed'].append({
                    'hostname': hostname,
                    'username': username,
                    'port': port,
                    'region': region,
                    'error': str(e)
                })
        
        # Print summary
        Print.heading('\nConnection Test Summary')
        Print.info(f'  Successful: {len(results["success"])}/{len(host_info)}')
        Print.info(f'  Failed: {len(results["failed"])}/{len(host_info)}')
        
        if results['failed']:
            Print.warn('\nFailed connections:')
            for failed in results['failed']:
                Print.warn(f'  - {failed["username"]}@{failed["hostname"]}:{failed["port"]} ({failed["region"]})')
                Print.warn(f'    Error: {failed["error"]}')
        
        return results
    
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
                        Print.info(f'  [OK] Successfully connected to {hostname}:{port}')
                    except Exception as e:
                        Print.warn(f'  [FAIL] Failed to connect to {hostname}:{port} - {e}')
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
            echo "Running processes:" &&
            (pgrep -f "node.*primary" > /dev/null && echo "  [OK] Primary: running" || echo "  [FAIL] Primary: not running") &&
            (pgrep -f "node.*worker" > /dev/null && echo "  [OK] Worker: running" || echo "  [FAIL] Worker: not running") &&
            (pgrep -f "benchmark_client" > /dev/null && echo "  [OK] Client: running" || echo "  [FAIL] Client: not running") &&
            echo "---" &&
            echo "Process count:" &&
            echo "  Primary: $(pgrep -f 'node.*primary' | wc -l)" &&
            echo "  Worker: $(pgrep -f 'node.*worker' | wc -l)" &&
            echo "  Client: $(pgrep -f 'benchmark_client' | wc -l)" &&
            echo "---" &&
            echo "Process details:" &&
            (pgrep -f "node.*primary" | xargs ps -p 2>/dev/null | tail -n +2 || echo "  No primary processes") &&
            (pgrep -f "node.*worker" | xargs ps -p 2>/dev/null | tail -n +2 || echo "  No worker processes") &&
            (pgrep -f "benchmark_client" | xargs ps -p 2>/dev/null | tail -n +2 || echo "  No client processes")
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
    
    def debug_sessions(self, hosts=[]):
        """Debug: Check tmux sessions and capture error messages"""
        Print.heading('Debugging CloudLab nodes - checking tmux sessions...')
        
        host_info = self.manager.get_host_info()
        repo_name = self.settings.repo_name
        
        debug_cmd = f'''
            echo "=== Debugging $(hostname) ===" &&
            echo "--- Running Processes ---" &&
            echo "Primary processes:" &&
            (pgrep -f "node.*primary" | xargs ps -fp 2>/dev/null || echo "  No primary processes") &&
            echo "" &&
            echo "Worker processes:" &&
            (pgrep -f "node.*worker" | xargs ps -fp 2>/dev/null || echo "  No worker processes") &&
            echo "" &&
            echo "Client processes:" &&
            (pgrep -f "benchmark_client" | xargs ps -fp 2>/dev/null || echo "  No client processes") &&
            echo "" &&
            echo "--- Log files in {repo_name}/logs ---" &&
            (ls -lh {repo_name}/logs/*.log 2>/dev/null | head -10 || echo "No log files found") &&
            echo "" &&
            echo "--- Recent log content (last 30 lines of each) ---" &&
            for log in {repo_name}/logs/*.log; do
                if [ -f "$log" ]; then
                    echo "=== $(basename $log) ===" &&
                    tail -30 "$log" 2>/dev/null || echo "  Could not read log" &&
                    echo ""
                fi
            done
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
                
                for (username, port), hostnames in hosts_by_config.items():
                    conn_kwargs = self._get_connection_kwargs({})
                    g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
                    Print.info(f'Debugging {len(hostnames)} host(s) at {username}@{port}...')
                    try:
                        results = g.run(debug_cmd, hide=False)
                        if isinstance(results, dict):
                            for hostname, result in results.items():
                                Print.info(f'\n--- {hostname} ---')
                                if result.stdout:
                                    print(result.stdout)
                        else:
                            if results.stdout:
                                print(results.stdout)
                    except Exception as e:
                        Print.warn(f'Failed to debug: {e}')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            Print.warn(f'Some hosts failed to respond: {e}')
    
    def _kill_ports_on_host(self, conn, ports):
        """Kill processes using specific ports on a host"""
        if not ports:
            return
        
        # Create a command to kill processes using these ports
        # Use lsof or fuser to find processes, then kill them
        port_list = ' '.join(str(p) for p in sorted(ports))
        kill_ports_cmd = f'''
            # Kill processes using specified ports
            for port in {port_list}; do
                # Try using lsof first (more common)
                if command -v lsof >/dev/null 2>&1; then
                    lsof -ti:$port 2>/dev/null | xargs kill -9 2>/dev/null || true
                # Fallback to fuser
                elif command -v fuser >/dev/null 2>&1; then
                    fuser -k $port/tcp 2>/dev/null || true
                # Last resort: use ss/netstat to find PIDs
                elif command -v ss >/dev/null 2>&1; then
                    ss -tlnp 2>/dev/null | grep ":$port " | grep -oP 'pid=\\K[0-9]+' | xargs kill -9 2>/dev/null || true
                elif command -v netstat >/dev/null 2>&1; then
                    netstat -tlnp 2>/dev/null | grep ":$port " | awk '{{print $7}}' | cut -d'/' -f1 | xargs kill -9 2>/dev/null || true
                fi
            done
            true
        '''
        try:
            conn.run(kill_ports_cmd, hide=True, warn=True, shell='/bin/bash')
        except:
            pass  # Ignore errors in port killing
    
    def _get_ports_from_committee(self, committee, faults):
        """Extract all ports that will be used by the committee"""
        ports_by_host = {}  # {hostname: set of ports}
        host_info = self.manager.get_host_info()
        
        # Create a mapping from IP to hostname
        ip_to_hostname = {}
        for host in host_info:
            hostname = host['hostname']
            # Try to resolve hostname to IP
            try:
                import socket
                ip = socket.gethostbyname(hostname)
                ip_to_hostname[ip] = hostname
                # Also map hostname itself in case it's already an IP
                ip_to_hostname[hostname] = hostname
            except:
                # If resolution fails, use hostname as-is
                ip_to_hostname[hostname] = hostname
        
        # Extract all addresses from committee JSON structure
        # Skip faulty nodes
        authorities = list(committee.json['authorities'].items())
        if faults > 0:
            authorities = authorities[:-faults]
        
        for name, authority in authorities:
            # Primary addresses
            primary = authority['primary']
            for addr_type, address in primary.items():
                ip, port = address.rsplit(':', 1)
                port = int(port)
                hostname = ip_to_hostname.get(ip, ip)
                if hostname not in ports_by_host:
                    ports_by_host[hostname] = set()
                ports_by_host[hostname].add(port)
            
            # Worker addresses
            workers = authority['workers']
            for worker_id, worker in workers.items():
                for addr_type, address in worker.items():
                    ip, port = address.rsplit(':', 1)
                    port = int(port)
                    hostname = ip_to_hostname.get(ip, ip)
                    if hostname not in ports_by_host:
                        ports_by_host[hostname] = set()
                    ports_by_host[hostname].add(port)
        
        return ports_by_host
    
    def kill(self, hosts=[], delete_logs=False, committee=None, faults=0):
        """Stop execution on specified hosts"""
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        
        host_info = self.manager.get_host_info()
        host_dict = {h['hostname']: h for h in host_info}
        delete_logs_cmd = CommandMaker.clean_logs() if delete_logs else 'true'
        # Kill benchmark processes by pattern matching
        # This will kill all processes matching the benchmark patterns
        kill_cmd = '''
            # Kill any running benchmark processes
            pkill -9 -f "node.*primary" 2>/dev/null || true
            pkill -9 -f "node.*worker" 2>/dev/null || true
            pkill -9 -f "benchmark_client" 2>/dev/null || true
            # Also kill any wrapper scripts that might still be running
            pkill -9 -f "/tmp/run_(primary|worker|client)-" 2>/dev/null || true
            true
        '''
        # Cleanup database directories and lock files
        cleanup_db_cmd = 'rm -rf .db-* 2>/dev/null || true'
        cmd = [delete_logs_cmd, kill_cmd, cleanup_db_cmd]
        
        # If committee is provided, also kill processes using the ports
        ports_by_host = {}
        if committee is not None:
            ports_by_host = self._get_ports_from_committee(committee, faults)
        
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
                    # Use warn=True since pkill may not find processes
                    g.run(' && '.join(cmd), hide=True, warn=True)
                    
                    # Kill processes using committee ports on these hosts
                    if ports_by_host:
                        for hostname in hostnames:
                            if hostname in ports_by_host:
                                ports = ports_by_host[hostname]
                                if ports:
                                    conn = Connection(hostname, user=username, port=port, connect_kwargs=conn_kwargs)
                                    self._kill_ports_on_host(conn, ports)
            else:
                # Handle specific hosts (can be strings or dicts)
                hosts_by_config = {}
                for h in hosts:
                    # Handle both string and dict formats
                    if isinstance(h, dict):
                        hostname = h['hostname']
                        username = h.get('username', 'root')
                        port = h.get('port', 22)
                    else:
                        # Extract hostname from host string
                        hostname = h.split('@')[1]
                        username = h.split('@')[0]
                        port = 22
                    key = (username, port)
                    if key not in hosts_by_config:
                        hosts_by_config[key] = []
                    hosts_by_config[key].append(hostname)
                
                # Run on each group
                for (username, port), hostnames in hosts_by_config.items():
                    conn_kwargs = self._get_connection_kwargs({})
                    g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs)
                    # Use warn=True since pkill may not find processes
                    g.run(' && '.join(cmd), hide=True, warn=True)
                    
                    # Kill processes using committee ports on these hosts
                    if ports_by_host:
                        for hostname in hostnames:
                            if hostname in ports_by_host:
                                ports = ports_by_host[hostname]
                                if ports:
                                    conn = Connection(hostname, user=username, port=port, connect_kwargs=conn_kwargs)
                                    self._kill_ports_on_host(conn, ports)
        except GroupException as e:
            # Don't fail if kill commands have errors - processes might not exist
            Print.warn(f'Some kill commands failed (this is OK if processes don\'t exist): {e}')
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
                # Check if repo directory exists first, then verify file exists and modify it
                # If file doesn't exist, warn but don't fail (repo might not be cloned yet)
                cmd = f'test -d {repo_name} && test -f {attack_rs_path} && ({sed_cmd} && echo "Successfully modified {attack_rs_path}") || (echo "Warning: {attack_rs_path} not found - repository may need to be installed first" && exit 0)'
                result = g.run(cmd, hide=True, warn=True)
                # Check if any host failed (but allow warnings)
                if isinstance(result, dict):
                    for hostname, res in result.items():
                        if not res.ok and 'Warning' not in res.stdout:
                            Print.warn(f'Failed to modify attack.rs on {hostname}: {res.stderr}')
                elif not result.ok and 'Warning' not in result.stdout:
                    Print.warn(f'Failed to modify attack.rs: {result.stderr}')
        except (GroupException, ExecutionError) as e:
            # Don't fail the entire benchmark if attack.rs modification fails
            Print.warn(f'Could not modify attack.rs (this is OK if repository is not installed yet): {e}')
    
    def _update(self, hosts, collocate, trigger_attack=None):
        """Update code on all hosts"""
        Print.info('Updating code on all nodes...')
        repo_name = self.settings.repo_name
        branch = self.settings.branch
        
        cmd = [
            # First ensure we're in home directory, then cd to repo
            f'cd $HOME/{repo_name} || (echo "Repository $HOME/{repo_name} not found. Please run: fab cloudlab-install" && exit 1)',
            'git fetch',
            f'git checkout {branch}',
            'git pull',
            # Verify essential files exist before compiling
            'test -f node/Cargo.toml || (echo "ERROR: node/Cargo.toml not found - branch may be missing files" && exit 1)',
            'test -f node/src/main.rs || (echo "ERROR: node/src/main.rs not found - branch may be missing files" && exit 1)',
            'test -f Cargo.toml || (echo "ERROR: Workspace Cargo.toml not found" && exit 1)',
            # Source cargo environment before building (use $HOME which will be the actual user's home directory)
            'source $HOME/.cargo/env 2>/dev/null || export PATH=$HOME/.cargo/bin:$PATH',
            # Compile from the workspace root (we're already in narwhal directory)
            # Clean and rebuild to ensure binaries are generated
            'echo "Cleaning previous build..."',
            'cargo clean --release 2>/dev/null || true',
            'echo "Starting compilation..."',
            'cargo build --release --features benchmark --bin node --bin benchmark_client || (echo "ERROR: Compilation failed with exit code $?" && exit 1)',
            'echo "Compilation completed, checking for binaries..."',
            # List all files in target/release to debug (ensure output is visible)
            'echo "Files in target/release/:"',
            'ls -la target/release/ 2>&1 | head -30',
            'echo "Looking for executables:"',
            'find target/release -maxdepth 1 -type f -executable 2>&1 | head -20',
            # Verify binaries were built (with better error messages)
            'test -f target/release/node || (echo "ERROR: node binary not found after compilation" && echo "Current directory: $(pwd)" && echo "Looking for node binary:" && find target/release -name "node" -type f 2>/dev/null || echo "node binary not found anywhere" && exit 1)',
            'test -f target/release/benchmark_client || (echo "ERROR: benchmark_client binary not found after compilation" && echo "Current directory: $(pwd)" && echo "Looking for benchmark_client binary:" && find target/release -name "benchmark_client" -type f 2>/dev/null || echo "benchmark_client binary not found anywhere" && exit 1)',
            # Create symlinks:
            # - benchmark_client in narwhal/ root
            # - node in narwhal/node/ directory
            # IMPORTANT: Do NOT delete the node/ directory (it contains source code)
            # Only remove the node/node symlink/file if it exists
            'echo "Creating symlinks..."',
            # Remove old symlinks/files if they exist (but preserve node/ directory)
            'rm -f benchmark_client 2>/dev/null || true',
            # Only remove node/node if it's a file or symlink (not a directory)
            # This preserves the node/ directory which contains source code
            '[ ! -d node/node ] && rm -f node/node 2>/dev/null || true',
            # Create benchmark_client symlink in repo root
            'ln -sf ./target/release/benchmark_client ./benchmark_client',
            # Create node symlink in node/ directory (node/ directory must exist and not be deleted)
            'mkdir -p node 2>/dev/null || true',
            'ln -sf ../target/release/node ./node/node',
            # Verify symlinks were created (with better error messages)
            'test -f benchmark_client || (echo "ERROR: benchmark_client symlink not created in repo root" && echo "Current directory: $(pwd)" && echo "Contents of current directory:" && ls -la | grep -E "benchmark" && exit 1)',
            'test -f node/node || (echo "ERROR: node symlink not created in node/ directory" && echo "Current directory: $(pwd)" && echo "Contents of node directory:" && ls -la node/ | head -10 && exit 1)',
            'echo "Symlinks created successfully"'
        ]
        
        # Modify attack.rs AFTER updating the code (so the file exists)
        # This ensures the repository is cloned and up-to-date first
        if trigger_attack is not None:
            # We'll modify attack.rs after git operations complete
            pass
        
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
                g = Group(*hostnames, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=60)
                # Don't hide output so we can see compilation and debugging messages
                result = g.run(' && '.join(cmd), hide=False)
                # Check for errors in the result
                if isinstance(result, dict):
                    for hostname, host_result in result.items():
                        if not host_result.ok:
                            Print.error(f'Failed on {hostname}: {host_result.stderr}')
                elif not result.ok:
                    Print.error(f'Command failed: {result.stderr}')
                
                # Modify attack.rs AFTER git operations (so the file exists)
                if trigger_attack is not None:
                    # Create a subset of hosts for modification
                    modify_hosts = [{'hostname': h, 'username': username, 'port': port} for h in hostnames]
                    self._modify_attack_rs(modify_hosts, trigger_attack)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)
    
    def _config(self, hosts, node_parameters, bench_parameters):
        """Generate and upload configuration files"""
        Print.info('Generating configuration files...')
        
        # Cleanup all local configuration files (same as remote.py)
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        
        # Try to compile locally, but fall back to remote key generation if cargo is not available
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        local_compilation_success = False
        
        try:
            # Recompile the latest code (same as remote.py)
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())
            local_compilation_success = True
            
            # Create alias for the client and nodes binary (same as remote.py)
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)
            
            # Generate keys locally
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]
                
        except (FileNotFoundError, subprocess.CalledProcessError) as e:
            # If cargo is not available locally (e.g., on Windows), generate keys on remote nodes
            Print.warn(f'Local compilation failed (this is OK if cargo is not available): {e}')
            Print.info('Generating keys on remote nodes instead...')
            
            # Generate keys on the first remote host (they're the same for all)
            repo_name = self.settings.repo_name
            first_host = hosts[0]
            username = first_host.get('username', 'root')
            hostname = first_host['hostname']
            port = first_host.get('port', 22)
            conn_kwargs = self._get_connection_kwargs({})
            
            try:
                conn = Connection(hostname, user=username, port=port, 
                                 connect_kwargs=conn_kwargs, connect_timeout=30)
                
                # Generate keys on remote node (code should already be compiled from _update)
                # First, get the actual home directory from remote
                home_result = conn.run('echo $HOME', hide=True, warn=True)
                remote_home = home_result.stdout.strip() if home_result.ok else '~'
                
                for i, key_file in enumerate(key_files):
                    # Use absolute path for key file on remote (expand $HOME)
                    remote_key_path = f'{remote_home}/{repo_name}/{key_file}'
                    # Generate key using the compiled node binary
                    # node binary is in node/ directory, benchmark_client is in repo root
                    # Use absolute path and ensure we're in the correct directory
                    # Try node/node first, then fallback to target/release/node
                    # Remove any stale .cargo-lock files (may require sudo if owned by root)
                    # The .cargo-lock error is usually harmless (just a warning from Cargo)
                    # Try to remove with sudo first, then without sudo
                    cmd = f'cd {remote_home}/{repo_name} && (sudo rm -f target/release/.cargo-lock 2>/dev/null || rm -f target/release/.cargo-lock 2>/dev/null || true) && CARGO_HOME={remote_home}/.cargo ./node/node generate_keys --filename {remote_key_path} 2>&1 | grep -v "failed to open.*cargo-lock" || true'
                    result = conn.run(cmd, hide=True, warn=True)
                    # Check if key file was actually created (more reliable than exit code)
                    key_check = conn.run(f'test -f {remote_key_path} && echo "OK" || echo "FAIL"', hide=True, warn=True)
                    if 'FAIL' in key_check.stdout or not result.ok:
                        # Try with target/release/node if ./node/node doesn't exist
                        cmd = f'cd {remote_home}/{repo_name} && (sudo rm -f target/release/.cargo-lock 2>/dev/null || rm -f target/release/.cargo-lock 2>/dev/null || true) && CARGO_HOME={remote_home}/.cargo ./target/release/node generate_keys --filename {remote_key_path} 2>&1 | grep -v "failed to open.*cargo-lock" || true'
                        result = conn.run(cmd, hide=True, warn=True)
                        # Verify key file was created
                        key_check = conn.run(f'test -f {remote_key_path} && echo "OK" || echo "FAIL"', hide=True, warn=True)
                        if 'FAIL' in key_check.stdout:
                            raise BenchError(f'Failed to generate key file {remote_key_path} on {hostname}')
                    
                    # Download the key file - ensure local directory exists
                    import os
                    local_key_dir = os.path.dirname(key_file) if os.path.dirname(key_file) else '.'
                    if local_key_dir and not os.path.exists(local_key_dir):
                        os.makedirs(local_key_dir, exist_ok=True)
                    conn.get(remote_key_path, key_file)
                
                # Load all keys
                for key_file in key_files:
                    keys += [Key.from_file(key_file)]
                    
            except Exception as e:
                raise BenchError('Failed to generate keys on remote nodes. Please ensure the code is compiled on remote nodes.', e)
        
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
        committee.print(PathMaker.committee_file())  # 改为 print() 而不是 save()
        
        node_parameters.print(PathMaker.parameters_file())  # 改为 print() 而不是 save()
        
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
    
    def _logs(self, committee, faults, max_workers=1):
        """Download logs from all hosts using download_logs.py"""
        Print.info('Downloading logs...')
        
        # Get benchmark directory (parent of benchmark/benchmark/)
        benchmark_dir = Path(__file__).parent.parent
        download_logs_script = benchmark_dir / 'download_logs.py'
        
        if not download_logs_script.exists():
            Print.error(f'download_logs.py not found at {download_logs_script}')
            Print.error('Falling back to basic log download...')
            # Fallback: create logs directory and return parser
            Path(PathMaker.logs_path()).mkdir(parents=True, exist_ok=True)
            return LogParser.process(PathMaker.logs_path(), faults=faults)
        
        # Run download_logs.py to download all logs
        try:
            import sys
            Print.info(f'Running download_logs.py with max_workers={max_workers}...')
            result = subprocess.run(
                [sys.executable, str(download_logs_script), '--max-workers', str(max_workers)],
                cwd=str(benchmark_dir),
                capture_output=False,  # Show output in real-time
                text=True
            )
            
            if result.returncode == 0:
                Print.info('✓ download_logs.py completed successfully')
            else:
                Print.warn(f'⚠ download_logs.py exited with code {result.returncode}')
        except Exception as e:
            Print.warn(f'⚠ Failed to run download_logs.py: {e}')
            Print.warn('Logs may be incomplete')
        
        # After downloading logs, run processing script
        Print.info('=' * 60)
        Print.info('Processing logs...')
        Print.info('=' * 60)
        
        try:
            run_benchmark_script = benchmark_dir / 'run_cloudlab_benchmark.py'
            # Use system Python instead of hardcoded path
            import shutil
            python_cmd = shutil.which('python3') or sys.executable
            if not python_cmd:
                Print.warn('⚠ Could not find python3, skipping run_cloudlab_benchmark.py')
                return
            
            if run_benchmark_script.exists():
                Print.info('Running run_cloudlab_benchmark.py --no-run to process logs...')
                result = subprocess.run(
                    [python_cmd, str(run_benchmark_script), '--no-run'],
                    cwd=str(benchmark_dir),
                    capture_output=False,  # Show output in real-time
                    text=True
                )
                if result.returncode == 0:
                    Print.info('✓ run_cloudlab_benchmark.py --no-run completed successfully')
                else:
                    Print.warn(f'⚠ run_cloudlab_benchmark.py --no-run exited with code {result.returncode}')
            else:
                Print.warn(f'⚠ run_cloudlab_benchmark.py not found at {run_benchmark_script}')
        except Exception as e:
            Print.warn(f'⚠ Failed to run run_cloudlab_benchmark.py --no-run: {e}')
        
        Print.info('=' * 60)
        
        # Parse and return logs
        return LogParser.process(PathMaker.logs_path(), faults=faults)
    
    def _background_run(self, host_info, command, log_file):
        """Run a command in the background using nohup on a remote host"""
        from os.path import basename, splitext, dirname
        name = splitext(basename(log_file))[0]
        repo_name = self.settings.repo_name
        # remote_log is the full path from repo root
        remote_log = f'{repo_name}/{log_file}'
        # log_file_relative is the path relative to repo directory (after cd)
        log_file_relative = log_file
        
        username = host_info.get('username', 'root')
        hostname = host_info['hostname']
        port = host_info.get('port', 22)
        conn_kwargs = self._get_connection_kwargs({})
        
        Print.info(f'Starting {name} on {hostname}...')
        c = Connection(hostname, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
        try:
            # First verify the repo directory and binaries exist
            # benchmark_client should be in repo root, node should be in node/ directory
            test_cmd = f'cd {repo_name} && test -f benchmark_client && test -f node/node && echo "Binaries found" || echo "Binaries missing"'
            test_result = c.run(test_cmd, hide=True)
            if 'Binaries missing' in test_result.stdout:
                Print.warn(f'  ⚠ Binaries not found in {repo_name} on {hostname}')
            
            # Ensure log directory exists
            from os.path import dirname
            log_dir = f'{repo_name}/{dirname(log_file)}' if dirname(log_file) else repo_name
            c.run(f'mkdir -p {log_dir}', hide=True)
            
            # Create a wrapper script that will run the command
            # The script will handle logging and ensure the process stays running
            script_path = f'/tmp/run_{name}.sh'
            
            # Extract store path from command for cleanup
            store_match = re.search(r'--store\s+(\S+)', command)
            store_path = store_match.group(1) if store_match else None
            
            # Modify command for worker/primary: replace ./node with ./node/node
            # For client, command stays as-is (uses ./benchmark_client in repo root)
            if name.startswith('worker-') or name.startswith('primary-'):
                command_modified = command.replace('./node ', './node/node ')
            else:
                command_modified = command
            
            # Write script with better error handling
            # Use relative path after cd to repo directory
            # log_file is already relative (e.g., "logs/client-0-0.log")
            # store_path is relative to repo root (we're running in repo root now)
            if store_path:
                cleanup_store = f'rm -rf {store_path} 2>/dev/null || true'
            else:
                cleanup_store = ''
            script_cmd = f'''cat > {script_path} << 'SCRIPTEOF'
#!/bin/bash
# Change to repo directory first
cd {repo_name} || {{
    echo "ERROR: Failed to cd to {repo_name}"
    echo "Current directory: $(pwd)"
    exit 1
}}

# Ensure log directory exists (relative to repo root)
mkdir -p $(dirname {log_file}) 2>/dev/null || true

# Cleanup database directory and lock files before starting
{cleanup_store}

# Check if binary exists (for client/worker/primary)
# benchmark_client is in repo root, node is in node/ directory
if [[ "{name}" == client-* ]]; then
    if [ ! -f "./benchmark_client" ] && [ ! -f "./target/release/benchmark_client" ]; then
        echo "ERROR: benchmark_client not found in repo root" | tee {log_file}
        echo "Looking in: $(pwd)" | tee -a {log_file}
        echo "Files in current dir: $(ls -la | grep benchmark | head -10)" | tee -a {log_file}
        exit 1
    fi
elif [[ "{name}" == worker-* ]] || [[ "{name}" == primary-* ]]; then
    if [ ! -f "./node/node" ] && [ ! -f "./target/release/node" ]; then
        echo "ERROR: node binary not found in node/ directory" | tee {log_file}
        echo "Looking in: $(pwd)" | tee -a {log_file}
        echo "Contents of node directory: $(ls -la node/ 2>/dev/null | head -10)" | tee -a {log_file}
        exit 1
    fi
fi

# Open log file and redirect stdout/stderr to it BEFORE exec
# This ensures the file is created and opened before the process starts
exec > {log_file} 2>&1

# Execute the command
# Replace ./node with ./node/node for worker/primary commands
# benchmark_client is already in repo root, so no change needed
if [[ "{name}" == worker-* ]] || [[ "{name}" == primary-* ]]; then
    # Use modified command with ./node/node
    exec {command_modified}
else
    # For client, use command as-is (benchmark_client is in repo root)
    exec {command}
fi
SCRIPTEOF'''
            script_write_result = c.run(script_cmd, hide=True, warn=True)
            if not script_write_result.ok:
                Print.error(f'  ✗ Failed to create script: {script_write_result.stderr}')
                raise BenchError(f'Failed to create script for {name} on {hostname}')
            
            chmod_result = c.run(f'chmod +x {script_path}', hide=True, warn=True)
            if not chmod_result.ok:
                Print.error(f'  ✗ Failed to make script executable: {chmod_result.stderr}')
                raise BenchError(f'Failed to make script executable for {name} on {hostname}')
            
            # Verify script was created correctly
            verify_script = c.run(f'test -f {script_path} && echo "OK" || echo "FAIL"', hide=True, warn=True)
            if 'FAIL' in verify_script.stdout:
                Print.error(f'  ✗ Script file {script_path} was not created')
                raise BenchError(f'Script file not created for {name} on {hostname}')
            
            # Use nohup to run the script in background
            # Use setsid to create a new session and detach from terminal
            # Redirect all output to /dev/null since script handles its own logging
            nohup_cmd = f'setsid nohup bash {script_path} </dev/null >/dev/null 2>&1 & echo $!'
            nohup_result = c.run(nohup_cmd, hide=True, warn=True)
            
            if not nohup_result.ok:
                Print.error(f'  ✗ Failed to start {name}: {nohup_result.stderr}')
                raise BenchError(f'Failed to start {name} on {hostname}')
            
            pid = nohup_result.stdout.strip()
            if not pid or not pid.isdigit():
                Print.error(f'  ✗ Failed to get PID for {name}')
                raise BenchError(f'Failed to start {name} on {hostname}')
            
            Print.info(f'  ✓ {name} started on {hostname} (PID: {pid})')
            
            # Wait a bit and verify the process is actually running
            sleep(1.0)
            check_cmd = f'ps -p {pid} >/dev/null 2>&1 && echo "Running" || echo "Not running"'
            check_result = c.run(check_cmd, hide=True, warn=True)
            
            if 'Not running' in check_result.stdout:
                # Process exited, check the log for errors
                Print.warn(f'  ⚠ Process {pid} exited immediately, checking logs...')
                
                # Check if script exists and can be read
                script_check = c.run(f'test -f {script_path} && cat {script_path} || echo "Script not found"', hide=True, warn=True)
                if 'Script not found' not in script_check.stdout:
                    Print.warn(f'  Script content:\n{script_check.stdout}')
                
                # Check script execution directly to see what happens
                test_exec = c.run(f'bash -x {script_path} 2>&1 | head -20 || true', hide=True, warn=True)
                if test_exec.stdout:
                    Print.warn(f'  Script execution test:\n{test_exec.stdout}')
                
                # Check log file
                log_check = c.run(f'test -f {remote_log} && tail -30 {remote_log} || echo "Log file not found"', hide=True, warn=True)
                if 'Log file not found' not in log_check.stdout:
                    Print.warn(f'  Last log lines:\n{log_check.stdout}')
                else:
                    Print.warn(f'  Log file {remote_log} not created')
                    # Try to check if directory exists
                    from os.path import dirname as os_dirname
                    log_dir_check = f'{repo_name}/{os_dirname(log_file)}' if os_dirname(log_file) else repo_name
                    dir_check = c.run(f'test -d {log_dir_check} && echo "Dir exists" || echo "Dir missing"', hide=True, warn=True)
                    Print.warn(f'  Log directory check: {dir_check.stdout}')
                
                # Check if command/binary exists
                binary_check = c.run(f'cd {repo_name} && which benchmark_client 2>/dev/null || which ./benchmark_client 2>/dev/null || echo "Binary not found"', hide=True, warn=True)
                if 'Binary not found' not in binary_check.stdout:
                    Print.warn(f'  Binary location: {binary_check.stdout}')
                else:
                    Print.warn(f'  ⚠ benchmark_client binary not found in PATH or current directory')
                
                # Don't raise error - let it continue, but provide detailed diagnostics
            else:
                Print.info(f'  ✓ Process {pid} is running')
                
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
        """Run a single benchmark iteration (CloudLab), mirroring logic from Bench._run_single"""
        from math import ceil
        from time import sleep

        faults = bench_parameters.faults

        # 1. Kill any potentially unfinished run and delete logs (same intent as Bench._run_single)
        Print.info('Killing any existing processes and ports...')
        self.kill(hosts=selected_hosts, delete_logs=True, committee=committee, faults=faults)

        # Small delay to ensure processes are killed and database cleanup completes
        sleep(3)

        # Pre-compute workers' addresses (filtered for faults) – same as Bench._run_single
        workers_addresses = committee.workers_addresses(faults)

        # 2. Run the clients first (they will wait for the nodes to be ready)
        #    This mirrors benchmark/benchmark/remote.py::_run_single
        Print.info('Booting clients...')
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

        # 3. Run the primaries (except the faulty ones) – same order as Bench._run_single
        Print.info('Booting primaries...')
        for i, address in enumerate(committee.primary_addresses(faults)):
            host_info = self._get_host_by_address(address, selected_hosts)
            if not host_info:
                Print.warn(f'Could not find host for address {address}')
                continue

            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                PathMaker.db_path(i),
                PathMaker.parameters_file(),
                debug=debug
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(host_info, cmd, log_file)

        # 4. Run the workers (except the faulty ones) – same as Bench._run_single
        Print.info('Booting workers...')
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host_info = self._get_host_by_address(address, selected_hosts)
                if not host_info:
                    Print.warn(f'Could not find host for address {address}')
                    continue

                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, id),
                    PathMaker.parameters_file(),
                    id,  # The worker's id.
                    debug=debug
                )
                log_file = PathMaker.worker_log_file(i, id)
                self._background_run(host_info, cmd, log_file)

        # 5. Wait for all transactions to be processed (progress output with log monitoring)
        duration = bench_parameters.duration
        Print.info(f'Running benchmark ({duration} sec)...')
        
        # Monitor logs for new round information (similar to local execution)
        repo_name = self.settings.repo_name
        last_line_counts = {}  # Track last line count per node to show only new lines
        
        for i in range(20):
            sleep(ceil(duration / 20))
            
            # Check for new log lines every iteration (to show round information like local)
            for j, address in enumerate(committee.primary_addresses(faults)):
                host_info = self._get_host_by_address(address, selected_hosts)
                if not host_info:
                    continue
                
                try:
                    log_file = PathMaker.primary_log_file(j)
                    remote_log = f'{repo_name}/{log_file}'
                    username = host_info.get('username', 'root')
                    hostname = host_info['hostname']
                    port = host_info.get('port', 22)
                    conn_kwargs = self._get_connection_kwargs({})
                    
                    c = Connection(hostname, user=username, port=port, 
                                 connect_kwargs=conn_kwargs, connect_timeout=10)
                    
                    # Get current line count
                    line_count_cmd = f'test -f {remote_log} && wc -l < {remote_log} || echo 0'
                    line_count_result = c.run(line_count_cmd, hide=True, warn=True, timeout=5)
                    
                    if line_count_result.ok:
                        current_count = int(line_count_result.stdout.strip() or 0)
                        last_count = last_line_counts.get(j, 0)
                        
                        # Show new lines (especially those containing round information)
                        if current_count > last_count:
                            # Get new lines
                            new_lines_cmd = f'test -f {remote_log} && tail -n +{last_count + 1} {remote_log} | tail -20 || echo ""'
                            new_lines_result = c.run(new_lines_cmd, hide=True, warn=True, timeout=5)
                            
                            if new_lines_result.ok and new_lines_result.stdout.strip():
                                new_lines = new_lines_result.stdout.strip().split('\n')
                                for line in new_lines:
                                    if line.strip():
                                        # Show lines containing round, committed, or created (similar to local output)
                                        if any(keyword in line.lower() for keyword in ['round', 'committed', 'created', 'dag']):
                                            Print.info(f'  Node {j}: {line[:120]}')  # Show first 120 chars
                            
                            last_line_counts[j] = current_count
                    
                    c.close()
                except Exception:
                    pass  # Ignore errors in log monitoring
            
            # Show progress every 5 iterations
            if (i + 1) % 5 == 0:
                Print.info(f'  Progress: {((i + 1) * 100) // 20}%')

        # 6. Kill processes but keep logs (same intent as Bench._run_single)
        self.kill(hosts=selected_hosts, delete_logs=False)
    
    def _run_single_imbalanced(self, imbalanced_rate_list, committee, bench_parameters, selected_hosts, debug=False):
        """Run a single benchmark with imbalanced rates (different rate per node)"""
        from math import ceil
        from time import sleep

        faults = bench_parameters.faults

        # 1. Kill any potentially unfinished run and delete logs
        Print.info('Killing any existing processes and ports...')
        self.kill(hosts=selected_hosts, delete_logs=True, committee=committee, faults=faults)

        # Small delay to ensure processes are killed and database cleanup completes
        sleep(3)

        # Pre-compute workers' addresses (filtered for faults)
        workers_addresses = committee.workers_addresses(faults)

        # 2. Run the clients first with imbalanced rates
        Print.info('Booting clients...')
        client_rates = imbalanced_rate_list
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host_info = self._get_host_by_address(address, selected_hosts)
                if not host_info:
                    Print.warn(f'Could not find host for address {address}')
                    continue

                cmd = CommandMaker.run_client(
                    address,
                    bench_parameters.tx_size,
                    client_rates[i],
                    [x for y in workers_addresses for _, x in y]
                )
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(host_info, cmd, log_file)

        # 3. Run the primaries (except the faulty ones)
        Print.info('Booting primaries...')
        for i, address in enumerate(committee.primary_addresses(faults)):
            host_info = self._get_host_by_address(address, selected_hosts)
            if not host_info:
                Print.warn(f'Could not find host for address {address}')
                continue

            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                PathMaker.db_path(i),
                PathMaker.parameters_file(),
                debug=debug
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(host_info, cmd, log_file)

        # 4. Run the workers (except the faulty ones)
        Print.info('Booting workers...')
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host_info = self._get_host_by_address(address, selected_hosts)
                if not host_info:
                    Print.warn(f'Could not find host for address {address}')
                    continue

                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, id),
                    PathMaker.parameters_file(),
                    id,  # The worker's id.
                    debug=debug
                )
                log_file = PathMaker.worker_log_file(i, id)
                self._background_run(host_info, cmd, log_file)

        # 5. Wait for all transactions to be processed (progress output with log monitoring)
        duration = bench_parameters.duration
        Print.info(f'Running benchmark ({duration} sec)...')
        
        # Monitor logs for new round information (similar to local execution)
        repo_name = self.settings.repo_name
        last_line_counts = {}  # Track last line count per node to show only new lines
        
        for i in range(20):
            sleep(ceil(duration / 20))
            
            # Check for new log lines every iteration (to show round information like local)
            for j, address in enumerate(committee.primary_addresses(faults)):
                host_info = self._get_host_by_address(address, selected_hosts)
                if not host_info:
                    continue
                
                try:
                    log_file = PathMaker.primary_log_file(j)
                    remote_log = f'{repo_name}/{log_file}'
                    username = host_info.get('username', 'root')
                    hostname = host_info['hostname']
                    port = host_info.get('port', 22)
                    conn_kwargs = self._get_connection_kwargs({})
                    
                    c = Connection(hostname, user=username, port=port, 
                                 connect_kwargs=conn_kwargs, connect_timeout=10)
                    
                    # Get current line count
                    line_count_cmd = f'test -f {remote_log} && wc -l < {remote_log} || echo 0'
                    line_count_result = c.run(line_count_cmd, hide=True, warn=True, timeout=5)
                    
                    if line_count_result.ok:
                        current_count = int(line_count_result.stdout.strip() or 0)
                        last_count = last_line_counts.get(j, 0)
                        
                        # Show new lines (especially those containing round information)
                        if current_count > last_count:
                            # Get new lines
                            new_lines_cmd = f'test -f {remote_log} && tail -n +{last_count + 1} {remote_log} | tail -20 || echo ""'
                            new_lines_result = c.run(new_lines_cmd, hide=True, warn=True, timeout=5)
                            
                            if new_lines_result.ok and new_lines_result.stdout.strip():
                                new_lines = new_lines_result.stdout.strip().split('\n')
                                for line in new_lines:
                                    if line.strip():
                                        # Show lines containing round, committed, or created (similar to local output)
                                        if any(keyword in line.lower() for keyword in ['round', 'committed', 'created', 'dag']):
                                            Print.info(f'  Node {j}: {line[:120]}')  # Show first 120 chars
                            
                            last_line_counts[j] = current_count
                    
                    c.close()
                except Exception:
                    pass  # Ignore errors in log monitoring
            
            # Show progress every 5 iterations
            if (i + 1) % 5 == 0:
                Print.info(f'  Progress: {((i + 1) * 100) // 20}%')

        # 6. Kill processes but keep logs
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
            if bench_parameters.rate_type == 'balanced':
                rate_list = bench_parameters.rate
            else:  # imbalanced
                # For imbalanced, we run once with the imbalanced_rate list
                rate_list = [bench_parameters.imbalanced_rate]
            
            for rate in rate_list:
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
                        if bench_parameters.rate_type == 'balanced':
                            rate_str = f'rate={rate}'
                            rate_for_file = rate
                        else:  # imbalanced
                            rate_str = f'imbalanced_rates={rate}'
                            rate_for_file = sum(rate) if isinstance(rate, list) else rate
                        Print.heading(f'\nRunning benchmark: nodes={n}, {rate_str}{attack_str}, run={run+1}/{bench_parameters.runs}')
                        
                        try:
                            # Run the actual benchmark
                            if bench_parameters.rate_type == 'balanced':
                                self._run_single(
                                    rate, committee_copy, bench_parameters, selected_hosts, debug
                                )
                            else:  # imbalanced
                                self._run_single_imbalanced(
                                    rate, committee_copy, bench_parameters, selected_hosts, debug
                                )
                            
                            # Download and parse logs
                            result = self._logs(committee_copy, bench_parameters.faults, max_workers=bench_parameters.workers)
                            result.print(PathMaker.result_file(
                                bench_parameters.faults,
                                n,
                                bench_parameters.workers,
                                bench_parameters.collocate,
                                rate_for_file,
                                bench_parameters.tx_size,
                            ))
                        except (subprocess.SubprocessError, GroupException, ParseError) as e:
                            self.kill(hosts=selected_hosts)
                            if isinstance(e, GroupException):
                                e = FabricError(e)
                            Print.error(BenchError('Benchmark failed', e))
                            continue
        
        Print.heading('All benchmarks completed')

