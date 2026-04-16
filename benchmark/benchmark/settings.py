# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError
from ipaddress import ip_address
import re


class SettingsError(Exception):
    pass


def _expand_ip_spec(spec):
    assert isinstance(spec, str)
    # Support "10.1.1.10-39" syntax.
    m = re.match(r'^(\d+\.\d+\.\d+\.)(\d+)-(\d+)$', spec.strip())
    if m:
        prefix, start, end = m.groups()
        start, end = int(start), int(end)
        if not (0 <= start <= 255 and 0 <= end <= 255 and start <= end):
            raise SettingsError(f'Invalid IP range: {spec}')
        ips = [f'{prefix}{i}' for i in range(start, end + 1)]
    else:
        ips = [spec.strip()]

    # Validate IPs.
    for ip in ips:
        try:
            ip_address(ip)
        except ValueError as e:
            raise SettingsError(f'Invalid IP address "{ip}" from "{spec}": {e}')
    return ips


def _normalize_hosts(hosts):
    if hosts is None:
        return []
    if not isinstance(hosts, list):
        raise SettingsError('"hosts" must be a list')

    normalized = []
    for host in hosts:
        if isinstance(host, str):
            for ip in _expand_ip_spec(host):
                normalized.append({'ip': ip, 'region': 'default'})
            continue

        if not isinstance(host, dict):
            raise SettingsError('Each host entry must be a string or an object')

        region = host.get('region', 'default')
        if not isinstance(region, str):
            raise SettingsError('Host "region" must be a string')

        ip_spec = host.get('ip', host.get('range'))
        if not isinstance(ip_spec, str):
            raise SettingsError(
                'Host object must contain "ip" (or "range") as a string'
            )

        for ip in _expand_ip_spec(ip_spec):
            normalized.append({'ip': ip, 'region': region})

    return normalized


class Settings:
    def __init__(self, key_name, key_path, base_port, repo_name, repo_url,
                 branch, instance_type, aws_regions, host_ip='public', hosts=None):
        inputs_str = [
            key_name, key_path, repo_name, repo_url, branch, instance_type
        ]
        if isinstance(aws_regions, list):
            regions = aws_regions
        else:
            regions = [aws_regions]
        inputs_str += regions
        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= isinstance(base_port, int)
        ok &= len(regions) > 0
        ok &= host_ip in ('public', 'private')
        if not ok:
            raise SettingsError('Invalid settings types')

        self.key_name = key_name
        self.key_path = key_path

        self.base_port = base_port

        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

        self.instance_type = instance_type
        self.aws_regions = regions
        self.host_ip = host_ip
        self.hosts = _normalize_hosts(hosts)

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            return cls(
                data['key']['name'],
                data['key']['path'],
                data['port'],
                data['repo']['name'],
                data['repo']['url'],
                data['repo']['branch'],
                data['instances']['type'],
                data['instances']['regions'],
                data.get('network', {}).get('host_ip', 'public'),
                data.get('hosts'),
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')
