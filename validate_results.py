#!/usr/bin/env python3
"""
Validate Union-Find results by comparing standalone and burst outputs.

This script verifies that both implementations find the same connected components,
even if the parent array representations differ (since roots can be arbitrary).
"""

import argparse
import json
import sys
import os
import boto3
from collections import defaultdict


def load_standalone_results(filepath):
    """Load results from standalone Union-Find execution."""
    with open(filepath, 'r') as f:
        data = json.load(f)
    return data['parent'], data['num_components']


def load_burst_results_from_s3(bucket, key, endpoint, num_nodes):
    """Load results from S3 (burst execution output)."""
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'minioadmin'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'minioadmin'),
        region_name='us-east-1'
    )
    
    output_key = f"{key}/output/parent_final.json"
    try:
        response = s3.get_object(Bucket=bucket, Key=output_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return data['parent'], data['num_components']
    except Exception as e:
        print(f"Error loading burst results from S3: {e}")
        return None, None


def load_burst_results_from_file(filepath):
    """Load results from burst JSON output file."""
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    # Find the result with num_components (from root worker)
    for result in data:
        if 'num_components' in result:
            return result.get('parent'), result['num_components']
    
    return None, None


def get_component_mapping(parent):
    """
    Convert parent array to component sets.
    Returns a frozenset of frozensets, each inner set containing nodes in same component.
    """
    components = defaultdict(set)
    for node, root in enumerate(parent):
        components[root].add(node)
    
    return frozenset(frozenset(nodes) for nodes in components.values())


def validate_components(parent1, parent2, num_nodes):
    """
    Validate that two parent arrays represent the same connected components.
    Returns (is_valid, message).
    """
    if len(parent1) != len(parent2):
        return False, f"Different sizes: {len(parent1)} vs {len(parent2)}"
    
    if len(parent1) != num_nodes:
        return False, f"Size mismatch with num_nodes: {len(parent1)} vs {num_nodes}"
    
    components1 = get_component_mapping(parent1)
    components2 = get_component_mapping(parent2)
    
    if components1 == components2:
        return True, f"✓ Both implementations found {len(components1)} identical components"
    
    # Find differences
    only_in_1 = components1 - components2
    only_in_2 = components2 - components1
    
    msg = f"✗ Components differ!\n"
    msg += f"  Standalone: {len(components1)} components\n"
    msg += f"  Burst: {len(components2)} components\n"
    
    if only_in_1:
        msg += f"  Only in standalone: {len(only_in_1)} component(s)\n"
        for comp in list(only_in_1)[:3]:
            msg += f"    Sample: {list(comp)[:5]}...\n"
    
    if only_in_2:
        msg += f"  Only in burst: {len(only_in_2)} component(s)\n"
        for comp in list(only_in_2)[:3]:
            msg += f"    Sample: {list(comp)[:5]}...\n"
    
    return False, msg


def main():
    parser = argparse.ArgumentParser(description="Validate Union-Find results")
    parser.add_argument("--standalone", type=str, required=True,
                        help="Path to standalone results JSON file")
    parser.add_argument("--burst", type=str, default=None,
                        help="Path to burst results JSON file (alternative to S3)")
    parser.add_argument("--bucket", type=str, default=None,
                        help="S3 bucket for burst results")
    parser.add_argument("--key", type=str, default=None,
                        help="S3 key for burst results")
    parser.add_argument("--endpoint", type=str, default=None,
                        help="S3 endpoint URL")
    parser.add_argument("--num-nodes", type=int, required=True,
                        help="Number of nodes in the graph")
    args = parser.parse_args()

    # Load standalone results
    print(f"Loading standalone results from {args.standalone}...")
    standalone_parent, standalone_components = load_standalone_results(args.standalone)
    print(f"  Found {standalone_components} components")

    # Load burst results
    burst_parent = None
    burst_components = None
    
    if args.burst:
        print(f"Loading burst results from {args.burst}...")
        burst_parent, burst_components = load_burst_results_from_file(args.burst)
    elif args.bucket and args.key and args.endpoint:
        print(f"Loading burst results from S3: s3://{args.bucket}/{args.key}/output/...")
        burst_parent, burst_components = load_burst_results_from_s3(
            args.bucket, args.key, args.endpoint, args.num_nodes
        )
    else:
        print("ERROR: Must provide either --burst file or S3 parameters (--bucket, --key, --endpoint)")
        sys.exit(1)

    if burst_parent is None:
        print("ERROR: Could not load burst results")
        sys.exit(1)
    
    print(f"  Found {burst_components} components")

    # Validate
    print("\nValidating components...")
    is_valid, message = validate_components(standalone_parent, burst_parent, args.num_nodes)
    print(message)

    if is_valid:
        print("\n✓ VALIDATION PASSED: Both implementations produce identical results!")
        sys.exit(0)
    else:
        print("\n✗ VALIDATION FAILED: Results differ!")
        sys.exit(1)


if __name__ == "__main__":
    main()
