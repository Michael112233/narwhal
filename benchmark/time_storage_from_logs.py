#!/usr/bin/env python3
"""
Parse Narwhal primary node logs and extract round progression and certificate information.
Exports data to CSV format for analysis.
"""

import re
import csv
from datetime import datetime
from collections import defaultdict


def parse_timestamp(timestamp_str):
    """Convert ISO format timestamp to POSIX timestamp.
    
    Args:
        timestamp_str: ISO format timestamp string (e.g., "2025-12-19T09:24:08.261Z")
    
    Returns:
        POSIX timestamp (float) or None if parsing fails
    """
    try:
        timestamp_str = timestamp_str.strip('[]')
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return datetime.timestamp(dt)
    except Exception as e:
        print(f"Error parsing timestamp {timestamp_str}: {e}")
        return None


def parse_log_file(log_file_path):
    """Parse log file and extract round advancement and certificate events.
    
    Args:
        log_file_path: Path to the log file
    
    Returns:
        tuple: (round_time_info, certificate_info) - lists of log lines
    """
    round_time_info = []
    certificate_info = []
    
    try:
        with open(log_file_path, 'r') as f:
            lines = f.readlines()
        
        for line in lines:
            if 'Received certificate' in line:
                certificate_info.append(line)
            if 'Dag starting at round' in line or 'Dag moved to round' in line:
                round_time_info.append(line)
        
        return round_time_info, certificate_info
    except FileNotFoundError:
        print(f"Warning: Log file not found: {log_file_path}")
        return [], []


def extract_round_info(round_time_info, node_id):
    """Extract round information from round advancement log lines.
    
    Args:
        round_time_info: List of round advancement log lines
        node_id: Node ID
    
    Returns:
        list: List of round_info dictionaries
    """
    round_info = []
    pattern = r'\[(.*?Z)\s+.*?\]\s+.*?(?:Dag starting at round|Dag moved to round)\s+(\d+)'
    
    for info in round_time_info:
        match = re.search(pattern, info)
        if match:
            timestamp_str = match.group(1)
            round_number = int(match.group(2))
            round_info.append({
                'node_id': node_id,
                'round_number': round_number,
                'time_stamp': timestamp_str,
                'certificates': []
            })
    
    return round_info


def create_round_dict(round_info):
    """Create a dictionary mapping round_number to round_info index.
    
    Args:
        round_info: List of round_info dictionaries
    
    Returns:
        dict: Mapping of round_number -> index in round_info list
    """
    round_info_dict = {}
    for idx, item in enumerate(round_info):
        try:
            round_num = int(item['round_number'])
            round_info_dict[round_num] = idx
        except (ValueError, KeyError):
            continue
    return round_info_dict


def calculate_time_delta(cert_timestamp_str, round_start_timestamp_str):
    """Calculate time difference between certificate and round start.
    
    Args:
        cert_timestamp_str: Certificate timestamp string
        round_start_timestamp_str: Round start timestamp string
    
    Returns:
        float: Time difference in milliseconds, or 0 if calculation fails
    """
    cert_posix = parse_timestamp(cert_timestamp_str)
    round_start_posix = parse_timestamp(round_start_timestamp_str)
    
    if cert_posix is not None and round_start_posix is not None:
        time_diff_seconds = cert_posix - round_start_posix
        return max(time_diff_seconds, 0) * 1000
    return 0


def process_certificates(certificate_info, round_info, round_info_dict):
    """Process certificate log lines and add them to round_info.
    
    Args:
        certificate_info: List of certificate log lines
        round_info: List of round_info dictionaries (modified in place)
        round_info_dict: Dictionary mapping round_number to round_info index
    
    Returns:
        int: Number of successfully matched certificates
    """
    pattern = r'\[(.*?Z)\s+.*?\]\s+.*?Received certificate from network: round (\d+), origin: ([^\s,]+), digest:'
    matched_count = 0
    
    for certificate_line in certificate_info:
        match = re.search(pattern, certificate_line)
        if match:
            matched_count += 1
            ts_str = match.group(1)
            round_str = match.group(2)
            origin = match.group(3)
            round_num = int(round_str)
            
            # Find the corresponding round_info entry
            if round_num in round_info_dict:
                idx = round_info_dict[round_num]
                cert_timestamp = calculate_time_delta(ts_str, round_info[idx]['time_stamp'])
                
                new_cert = {
                    'timestamp': cert_timestamp,
                    'origin': origin
                }
                
                # Check if certificate with same origin already exists, replace if found
                certs = round_info[idx]['certificates']
                found_duplicate = False
                for cert_idx, existing_cert in enumerate(certs):
                    if existing_cert.get('origin') == origin:
                        certs[cert_idx] = new_cert
                        found_duplicate = True
                        break
                
                # If no duplicate found, append the new certificate
                if not found_duplicate:
                    certs.append(new_cert)
        else:
            # Debug: print first few non-matching lines
            if matched_count < 3:
                print(f"Warning: Could not parse certificate line: {certificate_line[:100]}...")
    
    return matched_count


def find_max_certificates(round_info):
    """Find the maximum number of certificates in any round.
    
    Args:
        round_info: List of round_info dictionaries
    
    Returns:
        int: Maximum number of certificates
    """
    max_certs = 0
    for round_item in round_info:
        max_certs = max(max_certs, len(round_item.get('certificates', [])))
    return max_certs


def format_timestamp(timestamp):
    """Format timestamp to 3 decimal places.
    
    Args:
        timestamp: Timestamp value (int, float, or string)
    
    Returns:
        str: Formatted timestamp string
    """
    if timestamp != '' and isinstance(timestamp, (int, float)):
        return f"{timestamp:.3f}"
    return str(timestamp) if timestamp else ''


def export_to_csv(round_info, csv_filename, write_header=False):
    """Export round_info to CSV file.
    
    Args:
        round_info: List of round_info dictionaries
        csv_filename: Output CSV file path
        write_header: Whether to write CSV header
    """
    max_certs = find_max_certificates(round_info)
    
    with open(csv_filename, 'a' if not write_header else 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Write header if this is the first node
        if write_header:
            header = ['Node_ID', 'Round', 'Round_Start_Time', 'Certificate_Count']
            for j in range(1, max_certs + 1):
                header.append(f'Certificate_{j}_Time_Delta_ms')
                header.append(f'Certificate_{j}_Origin')
            writer.writerow(header)
        
        # Write data rows
        for round_item in sorted(round_info, key=lambda x: x['round_number']):
            node_id = round_item.get('node_id', '')
            round_num = round_item.get('round_number', '')
            round_start = round_item.get('time_stamp', '')
            cert_count = len(round_item.get('certificates', []))
            certs = round_item.get('certificates', [])
            
            # Build row
            row = [
                node_id,
                round_num,
                round_start if round_start else '',
                cert_count
            ]
            
            # Add certificate data
            for j in range(max_certs):
                if j < len(certs):
                    cert = certs[j]
                    row.append(format_timestamp(cert.get('timestamp', '')))
                    row.append(cert.get('origin', ''))
                else:
                    row.append('')
                    row.append('')
            
            writer.writerow(row)


def process_node_log(node_id, csv_filename, num_nodes):
    """Process a single node's log file.
    
    Args:
        node_id: Node ID
        csv_filename: Output CSV file path
        num_nodes: Total number of nodes (for header writing)
    """
    log_file_path = f'logs/primary-{node_id}.log'
    
    # Parse log file
    round_time_info, certificate_info = parse_log_file(log_file_path)
    if not round_time_info and not certificate_info:
        print(f"Node {node_id}: No data found, skipping...")
        return
    
    # Extract round information
    round_info = extract_round_info(round_time_info, node_id)
    
    # Create round dictionary for quick lookup
    round_info_dict = create_round_dict(round_info)
    
    # Process certificates
    print(f"Node {node_id}: Found {len(certificate_info)} certificate log lines")
    matched_count = process_certificates(certificate_info, round_info, round_info_dict)
    print(f"Node {node_id}: Successfully matched {matched_count} certificates")
    
    # Export to CSV
    write_header = (node_id == 0)
    export_to_csv(round_info, csv_filename, write_header)
    print(f"Node {node_id}: CSV exported to {csv_filename}\n")


def main():
    """Main function to process all node logs."""
    num_nodes = 10
    csv_filename = 'round_certificate_analysis.csv'
    
    print("=" * 80)
    print("Narwhal Log Analysis - Round and Certificate Extraction")
    print("=" * 80)
    print(f"Processing {num_nodes} nodes...\n")
    
    for node_id in range(num_nodes):
        process_node_log(node_id, csv_filename, num_nodes)
    
    print("=" * 80)
    print(f"Analysis complete! Results saved to: {csv_filename}")
    print("=" * 80)


if __name__ == '__main__':
    main()
