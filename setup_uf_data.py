#!/usr/bin/env python3
"""
Generate Union-Find graph data and upload to S3/MinIO.

Creates a random graph with configurable number of nodes, edges, and connected components.
The graph is partitioned and uploaded to S3 in TSV format (source\ttarget).
"""

import argparse
import random
import os
import tempfile
from collections import defaultdict

import boto3
from botocore.config import Config


def generate_graph(num_nodes: int, num_edges: int, num_components: int, seed: int = 42) -> list:
    """
    Generate a random graph with specified number of connected components.
    
    Args:
        num_nodes: Total number of nodes
        num_edges: Total number of edges
        num_components: Number of connected components to create
        seed: Random seed for reproducibility
    
    Returns:
        List of (source, target) tuples
    """
    random.seed(seed)
    edges = set()
    
    # Divide nodes into components
    nodes_per_component = num_nodes // num_components
    component_ranges = []
    for i in range(num_components):
        start = i * nodes_per_component
        end = (i + 1) * nodes_per_component if i < num_components - 1 else num_nodes
        component_ranges.append((start, end))
    
    # First, create a spanning tree for each component to ensure connectivity
    print(f"Creating {num_components} connected components...")
    for start, end in component_ranges:
        nodes_in_component = list(range(start, end))
        random.shuffle(nodes_in_component)
        # Create a chain to ensure connectivity
        for i in range(len(nodes_in_component) - 1):
            u, v = nodes_in_component[i], nodes_in_component[i + 1]
            edges.add((min(u, v), max(u, v)))
    
    # Add random edges within each component until we reach num_edges
    print(f"Adding random edges to reach {num_edges} total edges...")
    attempts = 0
    max_attempts = num_edges * 10
    while len(edges) < num_edges and attempts < max_attempts:
        # Pick a random component
        comp_idx = random.randint(0, num_components - 1)
        start, end = component_ranges[comp_idx]
        if end - start < 2:
            attempts += 1
            continue
        
        u = random.randint(start, end - 1)
        v = random.randint(start, end - 1)
        if u != v:
            edge = (min(u, v), max(u, v))
            edges.add(edge)
        attempts += 1
    
    print(f"Generated {len(edges)} edges")
    return list(edges)


def partition_edges(edges: list, num_partitions: int, num_nodes: int) -> dict:
    """
    Partition edges by source node.
    
    Each partition contains edges where source % num_partitions == partition_id.
    """
    partitions = defaultdict(list)
    for src, dst in edges:
        partition_id = src % num_partitions
        partitions[partition_id].append((src, dst))
        # Add reverse edge for undirected graph
        reverse_partition_id = dst % num_partitions
        partitions[reverse_partition_id].append((dst, src))
    
    return partitions


def upload_to_s3(partitions: dict, bucket: str, key: str, endpoint: str, 
                 access_key: str, secret_key: str):
    """Upload partitioned graph data to S3."""
    
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # Check if bucket exists, create if not
    try:
        s3.head_bucket(Bucket=bucket)
    except:
        print(f"Creating bucket: {bucket}")
        s3.create_bucket(Bucket=bucket)
    
    for partition_id, edges in partitions.items():
        part_key = f"{key}/part-{partition_id:05d}"
        content = "\n".join(f"{src}\t{dst}" for src, dst in edges)
        
        print(f"Uploading {part_key} ({len(edges)} edges, {len(content)} bytes)")
        s3.put_object(
            Bucket=bucket,
            Key=part_key,
            Body=content.encode('utf-8')
        )
    
    print(f"\nUploaded {len(partitions)} partitions to s3://{bucket}/{key}/")


def main():
    parser = argparse.ArgumentParser(description="Generate Union-Find graph data")
    parser.add_argument("--nodes", type=int, required=True, help="Number of nodes")
    parser.add_argument("--edges", type=int, default=None, 
                        help="Number of edges (default: 5 * nodes)")
    parser.add_argument("--components", type=int, default=10,
                        help="Number of connected components (default: 10)")
    parser.add_argument("--partitions", type=int, default=4,
                        help="Number of partitions (default: 4)")
    parser.add_argument("--bucket", type=str, default="test-bucket",
                        help="S3 bucket name")
    parser.add_argument("--key", type=str, default=None,
                        help="S3 key prefix (default: uf-graphs/uf-{nodes})")
    parser.add_argument("--endpoint", type=str, default="http://localhost:9000",
                        help="S3 endpoint URL")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    if args.edges is None:
        args.edges = args.nodes * 5  # Default: 5 edges per node
    
    if args.key is None:
        args.key = f"uf-graphs/uf-{args.nodes}"
    
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    
    print(f"=== Union-Find Graph Generator ===")
    print(f"Nodes: {args.nodes}")
    print(f"Edges: {args.edges}")
    print(f"Components: {args.components}")
    print(f"Partitions: {args.partitions}")
    print(f"Destination: s3://{args.bucket}/{args.key}/")
    print()
    
    # Generate graph
    edges = generate_graph(args.nodes, args.edges, args.components, args.seed)
    
    # Partition edges
    partitions = partition_edges(edges, args.partitions, args.nodes)
    
    # Print partition stats
    print("\nPartition statistics:")
    for pid in sorted(partitions.keys()):
        print(f"  Partition {pid}: {len(partitions[pid])} edges")
    
    # Upload to S3
    print()
    upload_to_s3(partitions, args.bucket, args.key, args.endpoint, access_key, secret_key)
    
    print(f"\n=== Done! ===")
    print(f"Run Union-Find with:")
    print(f"  cd unionfind && python3 unionfind.py \\")
    print(f"    --ow-host localhost --ow-port 31001 \\")
    print(f"    --uf-endpoint http://minio-service.default.svc.cluster.local:9000 \\")
    print(f"    --partitions {args.partitions} --num-nodes {args.nodes} \\")
    print(f"    --bucket {args.bucket} --key {args.key} \\")
    print(f"    --backend redis-list --granularity 1")


if __name__ == "__main__":
    main()
