#!/usr/bin/env python3
"""
Experiment script to run cloudlab_remote on specific nodes (10.10.1.1-4)

This script filters the CloudLab settings to only use nodes 10.10.1.1-4
and runs the benchmark on those nodes.
"""

import sys
import os
from pathlib import Path

# Add benchmark directory to path
benchmark_dir = Path(__file__).parent / 'benchmark'
sys.path.insert(0, str(benchmark_dir))

from benchmark.cloudlab_remote import CloudLabBench
from benchmark.cloudlab_settings import CloudLabSettings
from benchmark.cloudlab_instance import CloudLabInstanceManager
from benchmark.utils import Print, BenchError


class ExperimentCloudLabBench(CloudLabBench):
    """Custom CloudLabBench that only uses specific nodes"""
    
    def _get_connection_kwargs(self, host_info):
        """Get connection kwargs for a specific host (without port/timeout, passed separately)
        
        Override parent method to only extract connection-related parameters,
        not all Config object keys.
        """
        # Only extract connection-related parameters from self.connect
        # The parent class's dict(self.connect) includes all config keys which causes issues
        kwargs = {}
        
        # Extract pkey if it exists
        if hasattr(self.connect, 'pkey') and self.connect.pkey is not None:
            kwargs['pkey'] = self.connect.pkey
        
        # Extract other connection kwargs if they exist in connect_kwargs
        if hasattr(self.connect, 'connect_kwargs'):
            connect_kwargs = self.connect.connect_kwargs
            if isinstance(connect_kwargs, dict):
                kwargs.update(connect_kwargs)
            elif hasattr(connect_kwargs, '__dict__'):
                # If it's a DataProxy or similar, try to get its dict
                try:
                    kwargs.update(dict(connect_kwargs))
                except:
                    pass
        
        # Remove port and timeout if they exist to avoid ambiguity
        kwargs.pop('port', None)
        kwargs.pop('timeout', None)
        kwargs.pop('connect_timeout', None)
        
        return kwargs
    
    def __init__(self, ctx, target_hosts):
        """
        Initialize with filtered hosts
        
        Args:
            ctx: Fabric context
            target_hosts: List of hostnames to use (e.g., ['10.10.1.1', '10.10.1.2', '10.10.1.3', '10.10.1.4'])
        """
        # Load original settings
        # Try to find cloudlab_settings.json in multiple locations
        settings_file = None
        possible_paths = [
            Path(__file__).parent / 'cloudlab_settings.json',
        ]
        for path in possible_paths:
            if path.exists():
                settings_file = str(path)
                break
        
        if settings_file:
            original_manager = CloudLabInstanceManager.make(settings_file)
        else:
            # Use default location
            original_manager = CloudLabInstanceManager.make()
        original_settings = original_manager.settings
        
        # Filter hosts to only include target_hosts
        filtered_hosts = [
            host for host in original_settings.hosts 
            if host['hostname'] in target_hosts
        ]
        
        if len(filtered_hosts) != len(target_hosts):
            missing = set(target_hosts) - {h['hostname'] for h in filtered_hosts}
            if missing:
                raise BenchError(f'Some target hosts not found in settings: {missing}')
        
        # Create new settings with filtered hosts
        filtered_settings = CloudLabSettings(
            original_settings.key_path,
            original_settings.base_port,
            original_settings.repo_name,
            original_settings.repo_url,
            original_settings.branch,
            filtered_hosts
        )
        
        # Update manager with filtered settings
        self.manager = CloudLabInstanceManager(filtered_settings)
        self.settings = filtered_settings
        
        # Initialize SSH connection kwargs (same as parent class)
        # The parent class expects ctx.connect_kwargs to be a Config object with pkey attribute
        from fabric import Config
        try:
            from paramiko import RSAKey
            from paramiko.ssh_exception import PasswordRequiredException, SSHException
            import os
            
            # Initialize ctx.connect_kwargs as a Config object (same as parent class)
            ctx.connect_kwargs = Config({'connect_kwargs': {}})
            
            # Try to load key without password first
            try:
                ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                    str(self.settings.key_path)
                )
            except PasswordRequiredException:
                # Key is password-protected, try to get password
                password = os.environ.get('SSH_KEY_PASSWORD')
                
                # Try to get password from cloudlab_settings.json if it exists
                if not password:
                    try:
                        import json
                        # Try both locations for cloudlab_settings.json
                        settings_file1 = Path(__file__).parent / 'cloudlab_settings.json'
                        settings_file2 = Path(__file__).parent / 'benchmark' / 'cloudlab_settings.json'
                        settings_file = settings_file1 if settings_file1.exists() else settings_file2
                        if settings_file.exists():
                            with open(settings_file, 'r') as f:
                                data = json.load(f)
                                password = data.get('ssh_key_password')
                    except:
                        pass
                
                if password:
                    ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                        str(self.settings.key_path),
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


def main():
    """Main function to run experiment on nodes 10.10.1.1-4"""
    
    # Target nodes
    target_nodes = ['10.10.1.1', '10.10.1.2', '10.10.1.3', '10.10.1.4']
    
    Print.heading('Experiment: Running cloudlab_remote on nodes 10.10.1.1-4')
    Print.info(f'Target nodes: {", ".join(target_nodes)}')
    Print.info('=' * 60)
    
    # Create a Fabric context (CloudLabBench expects ctx.connect_kwargs)
    from fabric import Config
    class DummyContext:
        def __init__(self):
            self.connect_kwargs = Config({'connect_kwargs': {}})
    
    ctx = DummyContext()
    
    try:
        # Create custom CloudLabBench with filtered hosts
        Print.info('Initializing CloudLabBench with filtered hosts...')
        bench = ExperimentCloudLabBench(ctx, target_nodes)
        
        # Test connections first
        Print.info('Testing SSH connections to target nodes...')
        try:
            bench.test_connections()
        except Exception as e:
            Print.warn(f'Connection test failed: {e}')
            Print.warn('Continuing anyway...')
        
        # Benchmark parameters (same as fabfile.py cloudlab_remote)
        bench_params = {
            'faults': 0,
            'nodes': [4],  # Use all 4 nodes
            'workers': 1,
            'collocate': True,
            'rate': [220000],
            'tx_size': 512,
            'duration': 90,
            'runs': 1,
            # 'trigger_attack': [True],  # Uncomment to enable attack
        }
        
        # Node parameters
        node_params = {
            'header_size': 1_000,  # bytes
            'max_header_delay': 200,  # ms
            'gc_depth': 50,  # rounds
            'sync_retry_delay': 10_000,  # ms
            'sync_retry_nodes': 3,  # number of nodes
            'batch_size': 500_000,  # bytes
            'max_batch_delay': 200  # ms
        }
        
        # Run the benchmark
        Print.info('Starting benchmark...')
        bench.run(bench_params, node_params, debug=False)
        
        Print.heading('Experiment completed successfully!')
        
    except BenchError as e:
        Print.error(f'Benchmark error: {e}')
        # Print more details if available
        if hasattr(e, 'args') and len(e.args) > 1:
            Print.error(f'Error details: {e.args[1]}')
        sys.exit(1)
    except AssertionError as e:
        Print.error(f'Assertion error: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        Print.error(f'Unexpected error: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

