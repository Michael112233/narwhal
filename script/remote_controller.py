import paramiko
import os
import getpass
import subprocess

# ssh -p 28611 wucy@amd008.utah.cloudlab.us
host_ids = [135]
ports = [27610]
username = "wucy"
key_path = os.path.expanduser("~/.ssh/id_rsa")
passphrase = os.environ.get("SSH_KEY_PASSPHRASE")

REPO_URL = os.environ.get("REPO_URL", "https://github.com/Michael112233/narwhal.git")
BRANCH = os.environ.get("BRANCH", "GeneralizedTest")

for id in host_ids:
    for port in ports:
        host = f"amd{id}.utah.cloudlab.us"
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.load_system_host_keys()
        try:
            # First try ssh-agent/default keys to avoid bcrypt dependency on encrypted key parsing
            client.connect(
                hostname=host,
                port=port,
                username=username,
                timeout=10,
                look_for_keys=True,
                allow_agent=True,
            )
        except Exception:
            try:
                # Fallback to explicit key; use passphrase from env if provided
                if key_path.endswith('id_ed25519'):
                    pkey = paramiko.Ed25519Key.from_private_key_file(key_path, password=passphrase)
                else:
                    pkey = paramiko.RSAKey.from_private_key_file(key_path, password=passphrase)
                client.connect(
                    hostname=host,
                    port=port,
                    username=username,
                    pkey=pkey,
                    timeout=10,
                    look_for_keys=False,
                    allow_agent=False,
                )
            except Exception as e:
                print(f"connect {host}:{port} failed: {e}")
                try:
                    client.close()
                except Exception:
                    pass
                
                # Try using system SSH as fallback
                print(f"Trying system SSH as fallback...")
                try:
                    remote_cmd = (
                        f"if [ -d 'narwhal' ]; then "
                        f"cd narwhal && git fetch origin {BRANCH} && git checkout {BRANCH} && git pull origin {BRANCH}; "
                        f"else git clone -b {BRANCH} {REPO_URL}; fi"
                    )
                    result = subprocess.run([
                        'ssh', '-p', str(port), '-o', 'ConnectTimeout=10',
                        '-o', 'StrictHostKeyChecking=no',
                        f'{username}@{host}',
                        remote_cmd
                    ], capture_output=True, text=True, timeout=30)
                    
                    if result.returncode == 0:
                        print("System SSH connection successful!")
                        print("STDOUT:", result.stdout)
                        if result.stderr:
                            print("STDERR:", result.stderr)
                        continue
                    else:
                        print(f"System SSH failed with return code {result.returncode}")
                        print("STDERR:", result.stderr)
                except subprocess.TimeoutExpired:
                    print("System SSH connection timed out")
                except Exception as ssh_e:
                    print(f"System SSH error: {ssh_e}")
                
                continue
        try:
            remote_cmd = (
                f"if [ -d 'narwhal' ]; then "
                f"cd narwhal && git fetch origin {BRANCH} && git checkout {BRANCH} && git pull origin {BRANCH}; "
                f"else git clone -b {BRANCH} {REPO_URL}; fi"
            )
            stdin, stdout, stderr = client.exec_command(remote_cmd)
            print("STDOUT:", stdout.read().decode())
            print("STDERR:", stderr.read().decode())
        finally:
            try:
                client.close()
            except Exception:
                pass



