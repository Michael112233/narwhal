#!/usr/bin/env python3
"""
Script to download logs from CloudLab remote nodes
"""

import sys
import os
from pathlib import Path
from fabric import Connection
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
import signal
from contextlib import contextmanager

# Add benchmark directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmark.cloudlab_settings import CloudLabSettings
from benchmark.cloudlab_instance import CloudLabInstanceManager
from benchmark.utils import PathMaker, Print

def get_connection_kwargs(key_path):
    """Get SSH connection kwargs"""
    try:
        key = RSAKey.from_private_key_file(key_path)
        return {'pkey': key}
    except (FileNotFoundError, PasswordRequiredException, SSHException) as e:
        Print.error(f'Failed to load SSH key: {e}')
        return {}

@contextmanager
def timeout_context(seconds):
    """Context manager for timeout"""
    def timeout_handler(signum, frame):
        raise TimeoutError(f'Operation timed out after {seconds} seconds')
    
    # Set the signal handler
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        # Restore the old handler and cancel the alarm
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

def check_file_exists(conn, remote_path):
    """Check if a file exists on remote host"""
    try:
        result = conn.run(f'test -f {remote_path} && echo "exists" || echo "not_found"', hide=True, warn=True)
        return 'exists' in result.stdout
    except:
        return False

def safe_get_file(conn, remote_path, local_path, timeout=60):
    """Download file with timeout, existence check, and progress percentage"""
    # First check if file exists
    Print.info(f'    Checking if file exists...')
    sys.stdout.flush()
    if not check_file_exists(conn, remote_path):
        raise FileNotFoundError(f'File not found: {remote_path}')

    # Get file size for progress indication
    file_size = 0
    try:
        result = conn.run(f'stat -c%s {remote_path} 2>/dev/null || echo \"0\"', hide=True, warn=True)
        file_size = int(result.stdout.strip() or '0')
        if file_size > 0:
            size_mb = file_size / (1024 * 1024)
            Print.info(f'    File size: {size_mb:.2f} MB')
        else:
            Print.info(f'    File size: unknown')
        sys.stdout.flush()
    except Exception:
        pass

    # Download with timeout and progress
    Print.info(f'    Downloading (timeout: {timeout}s)...')
    sys.stdout.flush()

    # Track last progress so we can decide what to do on timeout
    progress_info = {"percent": 0, "done": 0, "total": 0}

    def _report_progress(transferred_bytes, total_bytes):
        # Prefer known file_size as ground truth for total size if available
        total = file_size or total_bytes or 1
        done = min(transferred_bytes, total)
        percent = min(100, max(0, int(done * 100 / total)))
        # Save last progress
        progress_info["percent"] = percent
        progress_info["done"] = done
        progress_info["total"] = total
        sys.stdout.write(f'\r    Progress: {percent:3d}% ({done}/{total} bytes)')
        sys.stdout.flush()

    sftp = None
    try:
        with timeout_context(timeout):
            sftp = conn.sftp()
            sftp.get(remote_path, str(local_path), callback=_report_progress)
        # Download completed successfully
        sys.stdout.write('\n')
        sys.stdout.flush()
        if sftp:
            try:
                sftp.close()
            except Exception:
                pass
        return True
    except (TimeoutError, Exception) as e:
        # Close SFTP connection if it exists
        if sftp:
            try:
                sftp.close()
            except Exception:
                pass
        
        sys.stdout.write('\n')
        sys.stdout.flush()
        
        # Check if file was actually downloaded completely
        file_complete = False
        if Path(local_path).exists():
            local_size = Path(local_path).stat().st_size
            # If local file size matches remote file size, consider it complete
            if file_size > 0 and local_size == file_size:
                file_complete = True
            # Or if progress reached 100% and file exists
            elif progress_info.get("percent", 0) >= 100:
                file_complete = True
        
        if file_complete:
            Print.warn(f'    ⚠ Download timed out but file appears complete ({Path(local_path).stat().st_size} bytes), treating as success')
            sys.stdout.flush()
            return True
        
        # File is incomplete, report error
        if isinstance(e, TimeoutError):
            Print.warn(f'    ⚠ Download timed out after {timeout}s')
        else:
            msg = str(e)[:100]
            Print.warn(f'    ⚠ Download failed: {msg}')
        sys.stdout.flush()
        raise

def download_single_file(hostname, username, port, conn_kwargs, remote_path, local_path, timeout=120):
    """Download a single file using a fresh connection"""
    conn = None
    try:
        # Create new connection for each file
        conn = Connection(hostname, user=username, port=port, connect_kwargs=conn_kwargs, connect_timeout=30)
        conn.open()
        safe_get_file(conn, remote_path, local_path, timeout=timeout)
        return True
    except FileNotFoundError:
        raise
    except Exception as e:
        Print.warn(f'    ⚠ Download failed: {str(e)[:100]}')
        sys.stdout.flush()
        raise
    finally:
        # Always close connection after each file
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def download_logs(settings_file='cloudlab_settings.json', max_workers=1):
    """Download logs from all CloudLab hosts"""
    
    # Load settings
    try:
        settings = CloudLabSettings.load(settings_file)
    except Exception as e:
        Print.error(f'Failed to load settings: {e}')
        return False
    
    # Create instance manager
    manager = CloudLabInstanceManager(settings)
    host_info = manager.get_host_info()
    repo_name = settings.repo_name
    
    # Create local logs directory
    logs_dir = Path(PathMaker.logs_path())
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    Print.info(f'Downloading logs from {len(host_info)} hosts...')
    Print.info(f'Repository name: {repo_name}')
    Print.info('=' * 60)
    sys.stdout.flush()
    
    conn_kwargs = get_connection_kwargs(settings.key_path)
    if not conn_kwargs:
        Print.error('Failed to load SSH key. Cannot proceed.')
        return False
    
    success_count = 0
    fail_count = 0
    
    for i, host in enumerate(host_info):
        hostname = host['hostname']
        username = host.get('username', 'root')
        port = host.get('port', 22)
        
        Print.info(f'[{i+1}/{len(host_info)}] Processing {username}@{hostname}:{port}...')
        sys.stdout.flush()
        
        host_success = True
        
        try:
            # Download primary log (new connection for each file)
            remote_log = f'{repo_name}/{PathMaker.primary_log_file(i)}'
            local_log = PathMaker.primary_log_file(i)
            Path(local_log).parent.mkdir(parents=True, exist_ok=True)
            Print.info(f'  Downloading primary-{i}.log...')
            sys.stdout.flush()
            try:
                download_single_file(hostname, username, port, conn_kwargs, remote_log, local_log, timeout=120)
                Print.info(f'  ✓ Downloaded primary-{i}.log')
                sys.stdout.flush()
            except FileNotFoundError:
                Print.warn(f'  ⚠ primary-{i}.log not found on remote')
                sys.stdout.flush()
            except Exception as e:
                Print.warn(f'  ⚠ primary-{i}.log download failed: {str(e)[:100]}')
                sys.stdout.flush()
                host_success = False
            
            # Download worker logs (new connection for each file)
            for j in range(max_workers):
                remote_log = f'{repo_name}/{PathMaker.worker_log_file(i, j)}'
                local_log = PathMaker.worker_log_file(i, j)
                Path(local_log).parent.mkdir(parents=True, exist_ok=True)
                Print.info(f'  Downloading worker-{i}-{j}.log...')
                sys.stdout.flush()
                try:
                    download_single_file(hostname, username, port, conn_kwargs, remote_log, local_log, timeout=120)
                    Print.info(f'  ✓ Downloaded worker-{i}-{j}.log')
                    sys.stdout.flush()
                except FileNotFoundError:
                    # Don't warn for worker logs as they might not exist
                    pass
                except Exception as e:
                    # Don't warn for worker logs as they might not exist
                    pass
            
            # Download client logs (new connection for each file)
            for j in range(max_workers):
                remote_log = f'{repo_name}/{PathMaker.client_log_file(i, j)}'
                local_log = PathMaker.client_log_file(i, j)
                Path(local_log).parent.mkdir(parents=True, exist_ok=True)
                Print.info(f'  Downloading client-{i}-{j}.log...')
                sys.stdout.flush()
                try:
                    download_single_file(hostname, username, port, conn_kwargs, remote_log, local_log, timeout=120)
                    Print.info(f'  ✓ Downloaded client-{i}-{j}.log')
                    sys.stdout.flush()
                except FileNotFoundError:
                    # Don't warn for client logs as they might not exist
                    pass
                except Exception as e:
                    # Don't warn for client logs as they might not exist
                    pass
            
            if host_success:
                success_count += 1
            else:
                fail_count += 1
            Print.info(f'  ✓ Completed {hostname}')
            sys.stdout.flush()
            
        except KeyboardInterrupt:
            Print.warn('\n  ✗ Interrupted by user')
            break
        except Exception as e:
            fail_count += 1
            error_msg = str(e)
            if len(error_msg) > 200:
                error_msg = error_msg[:200] + '...'
            Print.warn(f'  ✗ Failed to process {hostname}: {error_msg}')
            sys.stdout.flush()
    
    Print.info('=' * 60)
    Print.info(f'Download complete: {success_count} succeeded, {fail_count} failed')
    Print.info(f'Logs saved to: {logs_dir.absolute()}')
    sys.stdout.flush()
    
    return fail_count == 0

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Download logs from CloudLab remote nodes')
    parser.add_argument('--settings', default='cloudlab_settings.json', 
                       help='Path to CloudLab settings file')
    parser.add_argument('--max-workers', type=int, default=1,
                       help='Maximum number of workers per node (default: 1)')
    
    args = parser.parse_args()
    
    success = download_logs(args.settings, args.max_workers)
    sys.exit(0 if success else 1)

