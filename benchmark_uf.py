#!/usr/bin/env python3
"""
Benchmark Union-Find: Burst vs Standalone comparison.

This script runs both burst (distributed) and standalone (local) Union-Find
and compares the results for validation.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Tuple, Dict, List

import boto3
from botocore.config import Config

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ow_client.parser import add_openwhisk_to_parser, add_burst_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from unionfind_utils import generate_payload, add_unionfind_to_parser


def download_graph_from_s3(bucket: str, key: str, endpoint: str, 
                           output_file: str, num_partitions: int = 4) -> int:
    """Download partitioned graph from S3 and merge into single file."""
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    edges_seen = set()
    total_edges = 0
    
    with open(output_file, 'w') as f:
        for part_id in range(num_partitions):
            part_key = f"{key}/part-{part_id:05d}"
            try:
                print(f"  Streaming {part_key}...")
                response = s3.get_object(Bucket=bucket, Key=part_key)
                for line in response['Body'].iter_lines():
                    if line:
                        decoded_line = line.decode('utf-8')
                        parts = decoded_line.split('\t')
                        if len(parts) >= 2:
                            u, v = parts[0], parts[1]
                            f.write(f"{u}\t{v}\n")
                            total_edges += 1
            except Exception as e:
                print(f"Warning: Could not download {part_key}: {e}")
    
    return total_edges


def run_standalone(graph_file: str, num_nodes: int, ufst_binary: str) -> Dict:
    """Run standalone Union-Find and return results."""
    start = time.time()
    result = subprocess.run(
        [ufst_binary, graph_file, str(num_nodes)],
        capture_output=True,
        text=True
    )
    duration = time.time() - start
    
    if result.returncode != 0:
        print(f"Standalone error: {result.stderr}")
        return None
    
    try:
        data = json.loads(result.stdout)
        data['total_time_s'] = duration
        return data
    except json.JSONDecodeError:
        print(f"Could not parse standalone output: {result.stdout}")
        return None


def run_burst(args, num_nodes: int, bucket: str, key: str) -> Tuple[int, float]:
    """Run burst Union-Find and return (num_components, time_s).
    
    Time is measured from the earliest worker_start to the latest worker_end
    across all workers (pure computation time, excluding invocation overhead).
    """
    granularity = args.granularity if args.granularity else 1
    params = generate_payload(
        endpoint=args.uf_endpoint,
        partitions=args.partitions,
        num_nodes=num_nodes,
        bucket=bucket,
        key=key,
        max_iterations=args.max_iterations if hasattr(args, 'max_iterations') else None,
        granularity=granularity
    )
    
    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)
    
    host_submit = get_millis()
    dt = executor.burst("unionfind",
                        params,
                        file="./unionfind.zip",
                        memory=args.runtime_memory if args.runtime_memory else 2048,
                        custom_image=args.custom_image,
                        debug_mode=args.debug,
                        burst_size=1,
                        join=args.join,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        is_zip=True,
                        timeout=900000,
                        completion_timeout=120)
    finished = get_millis()
    
    dt_results = dt.get_results()
    
    num_components = None
    min_start = float('inf')
    max_end = 0
    
    for sublist in dt_results:
        if isinstance(sublist, list):
            for item in sublist:
                if isinstance(item, dict):
                    if item.get('num_components') is not None:
                        num_components = item['num_components']
                    # Extract worker_start and worker_end from timestamps
                    for ts in item.get('timestamps', []):
                        if ts['key'] == 'worker_start':
                            val = int(ts['value'])
                            if val < min_start:
                                min_start = val
                        elif ts['key'] == 'worker_end':
                            val = int(ts['value'])
                            if val > max_end:
                                max_end = val
    
    # Use worker timestamps if available, otherwise fall back to wall-clock
    if min_start < float('inf') and max_end > 0:
        burst_time_s = (max_end - min_start) / 1000.0
    else:
        burst_time_s = (finished - host_submit) / 1000.0
    
    return num_components, burst_time_s


def main():
    parser = argparse.ArgumentParser(description="Benchmark Union-Find: Burst vs Standalone")
    add_openwhisk_to_parser(parser)
    add_burst_to_parser(parser)
    
    # UF-specific arguments
    parser.add_argument("--uf-endpoint", type=str, required=True,
                        help="Endpoint of the S3 service (for workers)")
    parser.add_argument("--local-endpoint", type=str, default="http://localhost:9000",
                        help="Local S3 endpoint (for downloading)")
    parser.add_argument("--partitions", type=int, default=4,
                        help="Number of partitions")
    parser.add_argument("--bucket", type=str, default="test-bucket",
                        help="S3 bucket name")
    parser.add_argument("--max-iterations", type=int, default=None,
                        help="Maximum iterations")
    
    # Benchmark specific
    parser.add_argument("--sizes", type=str, default="5000,10000,50000,100000",
                        help="Comma-separated list of node counts to benchmark")
    parser.add_argument("--ufst-binary", type=str, default="./ufst/target/release/union-find",
                        help="Path to standalone Union-Find binary")
    parser.add_argument("--graph-file", type=str, default=None,
                        help="Local graph file for standalone (skip S3 download). "
                             "If not set, downloads from S3.")
    parser.add_argument("--skip-standalone", action="store_true",
                        help="Skip standalone execution")
    parser.add_argument("--skip-burst", action="store_true",
                        help="Skip burst execution")
    parser.add_argument("--output", type=str, default="uf_benchmark_results.json",
                        help="Output file for results")
    
    args = try_or_except(parser)
    
    sizes = [int(s.strip()) for s in args.sizes.split(',')]
    results = []
    
    print("=" * 60)
    print("Union-Find Benchmark: Burst vs Standalone")
    print("=" * 60)
    
    for num_nodes in sizes:
        key = f"uf-graphs/uf-{num_nodes}"
        print(f"\n{'='*60}")
        print(f"Testing with {num_nodes:,} nodes")
        print(f"{'='*60}")
        
        result = {
            "nodes": num_nodes,
            "partitions": args.partitions,
        }
        
        # Use local file or download from S3 for standalone
        if not args.skip_standalone:
            if args.graph_file and os.path.exists(args.graph_file):
                temp_graph = args.graph_file
                delete_after = False
                print(f"\nUsing local graph file: {temp_graph}")
            else:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
                    temp_graph = f.name
                delete_after = True
                
                print(f"\nDownloading graph from S3...")
                num_edges = download_graph_from_s3(
                    args.bucket, key, args.local_endpoint, 
                    temp_graph, args.partitions
                )
                print(f"Downloaded {num_edges:,} unique edges")
                result["edges"] = num_edges
            
            # Run standalone
            print(f"\nRunning standalone Union-Find...")
            standalone_result = run_standalone(temp_graph, num_nodes, args.ufst_binary)
            
            if standalone_result:
                result["standalone"] = {
                    "num_components": standalone_result["num_components"],
                    "time_s": standalone_result["total_time_s"],
                    "execution_time_ms": standalone_result["execution_time_ms"],
                }
                print(f"  Components: {standalone_result['num_components']}")
                print(f"  Time: {standalone_result['total_time_s']:.3f}s")
                print(f"Standalone Processing Time (Execution): {standalone_result['execution_time_ms']} ms")
            else:
                print(f"Standalone Processing Time (Execution): FAILED")
            
            if delete_after:
                os.unlink(temp_graph)
        
        # Run burst
        if not args.skip_burst:
            print(f"\nRunning burst Union-Find ({args.partitions} workers)...")
            try:
                burst_components, burst_time = run_burst(args, num_nodes, args.bucket, key)
                burst_time_ms = burst_time * 1000
                result["burst"] = {
                    "num_components": burst_components,
                    "time_s": burst_time,
                    "time_ms": burst_time_ms,
                }
                print(f"  Components: {burst_components}")
                print(f"  Time: {burst_time:.3f}s")
                print(f"Burst Processing Time (Distributed Span): {burst_time_ms:.0f} ms")
            except Exception as e:
                print(f"  Error: {e}")
                result["burst"] = {"error": str(e)}
                print(f"Burst Processing Time (Distributed Span): FAILED")
        
        # Compute speedups
        st_exec_ms = result.get("standalone", {}).get("execution_time_ms")
        bt_ms = result.get("burst", {}).get("time_ms")
        if st_exec_ms and bt_ms and bt_ms > 0:
            algo_speedup = st_exec_ms / bt_ms
            print(f"\nAlgorithmic Speedup: {algo_speedup:.2f}x")
            if algo_speedup > 1.0:
                print("✓ Burst is faster!")
            else:
                print("✗ Standalone is faster")
        
        # Validate
        if "standalone" in result and "burst" in result:
            standalone_comp = result["standalone"]["num_components"]
            burst_comp = result["burst"].get("num_components")
            
            if standalone_comp == burst_comp:
                print(f"\n✅ VALIDATION PASSED: Both found {standalone_comp} components")
                result["validation"] = "PASSED"
            else:
                print(f"\n❌ VALIDATION FAILED: Standalone={standalone_comp}, Burst={burst_comp}")
                result["validation"] = "FAILED"
        
        results.append(result)
    
    # Summary
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)
    print(f"\n{'Nodes':>12} | {'Components':>10} | {'Standalone':>12} | {'Burst':>12} | {'Speedup':>8} | {'Valid'}")
    print("-" * 75)
    
    for r in results:
        nodes = r["nodes"]
        comp = r.get("standalone", {}).get("num_components") or r.get("burst", {}).get("num_components") or "?"
        st_time = r.get("standalone", {}).get("time_s", float('nan'))
        bt_time = r.get("burst", {}).get("time_s", float('nan'))
        
        if st_time and bt_time and st_time > 0:
            speedup = st_time / bt_time
            speedup_str = f"{speedup:.2f}x"
        else:
            speedup_str = "N/A"
        
        valid = r.get("validation", "N/A")
        
        print(f"{nodes:>12,} | {comp:>10} | {st_time:>10.3f}s | {bt_time:>10.3f}s | {speedup_str:>8} | {valid}")
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
