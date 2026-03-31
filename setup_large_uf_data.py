#!/usr/bin/env python3
"""
Generate large WCC graph datasets for benchmarking.
Creates both local .tsv files for standalone and S3 partitions for burst.

Graph structure: N nodes divided into C connected components. Each component has
a spanning tree for guaranteed connectivity plus random intra-component edges
for density. Edges are undirected (stored as src→dst and dst→src in partitions).
"""
import argparse
import random
import os
import struct
from collections import defaultdict

import boto3
from botocore.client import Config


def generate_graph(num_nodes, edges_per_node=5, num_components=10, seed=42):
    """
    Generate a random graph with specified number of connected components.

    Returns list of (src, dst) tuples (undirected edges, canonical form min→max).
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

    # Create spanning tree for each component (guarantees connectivity)
    for start, end in component_ranges:
        nodes = list(range(start, end))
        random.shuffle(nodes)
        for i in range(len(nodes) - 1):
            u, v = nodes[i], nodes[i + 1]
            edges.add((min(u, v), max(u, v)))

    # Add random intra-component edges until target density
    target_edges = num_nodes * edges_per_node
    attempts = 0
    max_attempts = target_edges * 10
    while len(edges) < target_edges and attempts < max_attempts:
        comp_idx = random.randint(0, num_components - 1)
        start, end = component_ranges[comp_idx]
        if end - start < 2:
            attempts += 1
            continue
        u = random.randint(start, end - 1)
        v = random.randint(start, end - 1)
        if u != v:
            edges.add((min(u, v), max(u, v)))
        attempts += 1

    return list(edges)


def write_local_file(edges, output_file):
    """Write edges to local TSV file for standalone binary."""
    print(f"Writing local file: {output_file}")
    with open(output_file, 'w') as f:
        for src, dst in edges:
            f.write(f"{src}\t{dst}\n")
    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"  ✅ Written {len(edges)} edges ({size_mb:.1f} MB)")


def _vertex_partition(node: int, num_partitions: int, num_nodes: int) -> int:
    return min(num_partitions - 1, (node * num_partitions) // max(1, num_nodes))


def partition_and_upload_s3(edges, num_nodes, num_partitions, bucket, key, endpoint,
                            access_key="minioadmin", secret_key="minioadmin",
                            output_format="binary", partition_mode="vertex"):
    """Partition edges by source node and upload to S3."""
    print(f"Uploading to S3: {bucket}/{key}/ ({num_partitions} partitions)")

    s3 = boto3.client(
        's3',
        endpoint_url=endpoint if endpoint.startswith("http") else f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        print(f"Creating bucket: {bucket}")
        s3.create_bucket(Bucket=bucket)

    # Partition:
    # - vertex: assign edge to owner partition of src only (lower communication)
    # - edge:   legacy mode, duplicate as reverse edge into dst partition too
    partitions = defaultdict(list)
    for src, dst in edges:
        if partition_mode == "vertex":
            partitions[_vertex_partition(src, num_partitions, num_nodes)].append((src, dst))
        else:
            partitions[src % num_partitions].append((src, dst))
            partitions[dst % num_partitions].append((dst, src))

    for part_id in range(num_partitions):
        part_edges = partitions.get(part_id, [])
        part_key = f"{key}/part-{part_id:05d}"
        if output_format in ("tsv", "text"):
            body = "\n".join(f"{s}\t{d}" for s, d in part_edges).encode('utf-8')
            content_type = "text/plain"
        else:
            payload = bytearray()
            for s, d in part_edges:
                payload.extend(struct.pack('<I', s))
                payload.extend(struct.pack('<I', d))
            body = bytes(payload)
            content_type = "application/octet-stream"
        s3.put_object(
            Bucket=bucket,
            Key=part_key,
            Body=body,
            ContentType=content_type
        )
        print(f"  ✅ Partition {part_id}: {len(part_edges)} edges, {len(body)} bytes ({output_format})")

    print(f"✅ Uploaded {num_partitions} partitions to s3://{bucket}/{key}/")


def main():
    parser = argparse.ArgumentParser(
        description="Generate large WCC graph datasets (local + S3)")
    parser.add_argument("--nodes", type=int, required=True, help="Number of nodes")
    parser.add_argument("--edges-per-node", type=int, default=5,
                        help="Edges per node (density, default: 5)")
    parser.add_argument("--components", type=int, default=10,
                        help="Number of connected components (default: 10)")
    parser.add_argument("--partitions", type=int, default=4,
                        help="Number of S3 partitions (default: 4)")
    parser.add_argument("--output", type=str, default=None,
                        help="Local output file (default: wcc_graph_{nodes}.tsv)")
    parser.add_argument("--bucket", type=str, default="test-bucket",
                        help="S3 bucket name")
    parser.add_argument("--key", type=str, default=None,
                        help="S3 key prefix (default: wcc-graphs/wcc-{nodes})")
    parser.add_argument("--endpoint", type=str, default="http://localhost:9000",
                        help="S3 endpoint URL")
    parser.add_argument("--no-s3", action="store_true",
                        help="Skip S3 upload (local file only)")
    parser.add_argument("--no-local", action="store_true",
                        help="Skip local file (S3 only)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    parser.add_argument("--format", type=str, default="binary", choices=["binary", "tsv", "text"],
                        help="Partition storage format in S3 (default: binary)")
    parser.add_argument("--partition-mode", type=str, default="vertex", choices=["vertex", "edge"],
                        help="Partition strategy: vertex (faster merge) or edge (legacy)")

    args = parser.parse_args()

    if args.output is None:
        args.output = f"wcc_graph_{args.nodes}.tsv"
    if args.key is None:
        args.key = f"wcc-graphs/wcc-{args.nodes}"

    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    print(f"=== WCC Graph Generator ===")
    print(f"Nodes:      {args.nodes:,}")
    print(f"Density:    {args.edges_per_node} edges/node")
    print(f"Components: {args.components}")
    print(f"Partitions: {args.partitions}")
    print()

    # Generate graph
    edges = generate_graph(args.nodes, args.edges_per_node, args.components, args.seed)
    print(f"Generated {len(edges):,} edges")

    # Write local file
    if not args.no_local:
        write_local_file(edges, args.output)

    # Upload to S3
    if not args.no_s3:
        partition_and_upload_s3(
            edges, args.nodes, args.partitions, args.bucket, args.key, args.endpoint,
            access_key, secret_key, args.format, args.partition_mode
        )

    print(f"\n=== Done! ===")
    if not args.no_local:
        print(f"  Local file:  {args.output}")
    if not args.no_s3:
        print(f"  S3 location: s3://{args.bucket}/{args.key}/")


if __name__ == "__main__":
    main()
