#!/usr/bin/env python3
"""
Benchmark WCC: Burst vs Standalone comparison.

This script runs both burst (distributed) and standalone (local) WCC
and compares the results for validation.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import struct
from typing import Tuple, Dict, List

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ow_client.parser import add_openwhisk_to_parser, add_burst_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from unionfind_utils import generate_payload

BENCHMARK_JSON_PREFIX = "BENCHMARK_RESULT_JSON:"
HERE = os.path.dirname(os.path.abspath(__file__))
BENCHMARK_NAME = os.environ.get("WCC_BENCHMARK_TITLE", "WCC")
BENCHMARK_SLUG = os.environ.get("WCC_ALGORITHM_SLUG", "wcc")
ACTION_NAME = os.environ.get("WCC_ACTION_NAME", "wcc")
GRAPH_FILE_PREFIX = os.environ.get("WCC_GRAPH_FILE_PREFIX", "wcc_graph")
DATASET_PREFIX = os.environ.get("WCC_DATASET_PREFIX", "wcc-graphs")
DATASET_BASENAME = os.environ.get("WCC_DATASET_BASENAME", "wcc")
ZIP_FILE = os.environ.get("WCC_ZIP_FILE", os.path.join(HERE, "unionfind.zip"))
CLEAN_BURST_CLUSTER_SCRIPT = os.path.join(os.path.dirname(HERE), "clean_burst_cluster.sh")


def clean_burst_cluster() -> None:
    result = subprocess.run(
        ["bash", CLEAN_BURST_CLUSTER_SCRIPT],
        cwd=os.path.dirname(HERE),
        text=True,
        capture_output=True,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        raise RuntimeError("failed to clean Burst cluster before running wcc")


def canonical_component_hash(parent: List[int]) -> str:
    root_to_component: dict[int, int] = {}
    next_component = 0
    hash_value = 0xCBF29CE484222325
    for root in parent:
        component = root_to_component.setdefault(root, next_component)
        if component == next_component:
            next_component += 1
        hash_value ^= component
        hash_value = (hash_value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"{hash_value:016x}"


def download_graph_from_s3(bucket: str, key: str, endpoint: str, 
                           output_file: str, num_partitions: int = 4,
                           input_format: str = "binary") -> int:
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
    
    total_edges = 0
    
    with open(output_file, 'w') as f:
        for part_id in range(num_partitions):
            part_key = f"{key}/part-{part_id:05d}"
            try:
                print(f"  Streaming {part_key}...")
                response = s3.get_object(Bucket=bucket, Key=part_key)
                payload = response['Body'].read()
                if input_format == "binary":
                    for off in range(0, len(payload) - (len(payload) % 8), 8):
                        u, v = struct.unpack_from("<II", payload, off)
                        f.write(f"{u}\t{v}\n")
                        total_edges += 1
                else:
                    for line in payload.decode('utf-8').splitlines():
                        if not line:
                            continue
                        parts = line.split('\t')
                        if len(parts) >= 2:
                            u, v = parts[0], parts[1]
                            f.write(f"{u}\t{v}\n")
                            total_edges += 1
            except Exception as e:
                print(f"Warning: Could not download {part_key}: {e}")
    
    return total_edges


def s3_partitions_available(bucket: str, key: str, endpoint: str, num_partitions: int) -> bool:
    """Return True when every expected UF partition exists and is non-empty."""
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint if endpoint.startswith("http") else f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    try:
        present = 0
        for part_id in range(num_partitions):
            head = s3.head_object(Bucket=bucket, Key=f"{key}/part-{part_id:05d}")
            if int(head.get("ContentLength", 0)) > 0:
                present += 1
        return present >= num_partitions
    except (BotoCoreError, ClientError):
        return False


def ensure_input_data(args, num_nodes: int, key: str) -> str | None:
    """Ensure the local graph and S3 partitions required by the benchmark exist."""
    local_graph = args.graph_file or f"{GRAPH_FILE_PREFIX}_{num_nodes}.tsv"
    need_local = not args.skip_standalone
    need_s3 = not args.skip_burst
    local_ready = os.path.exists(local_graph) and os.path.getsize(local_graph) > 0
    s3_ready = s3_partitions_available(args.bucket, key, args.local_endpoint, args.partitions) if need_s3 else True

    if (not need_local or local_ready) and s3_ready:
        return local_graph if need_local else None

    command = [
        sys.executable,
        os.path.join(HERE, "setup_large_uf_data.py"),
        "--nodes", str(num_nodes),
        "--partitions", str(args.partitions),
        "--bucket", args.bucket,
        "--endpoint", args.local_endpoint,
        "--output", local_graph,
        "--format", args.input_format,
    ]
    if not need_local:
        command.append("--no-local")
    if not need_s3:
        command.append("--no-s3")

    print(f"\nPreparing {BENCHMARK_NAME} input data...")
    completed = subprocess.run(command, capture_output=True, text=True, timeout=900)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Failed to generate {BENCHMARK_NAME} input data.\n"
            f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )

    local_ready = os.path.exists(local_graph) and os.path.getsize(local_graph) > 0
    s3_ready = s3_partitions_available(args.bucket, key, args.local_endpoint, args.partitions) if need_s3 else True
    if need_local and not local_ready:
        raise RuntimeError(f"{BENCHMARK_NAME} local graph was not generated correctly: {local_graph}")
    if need_s3 and not s3_ready:
        raise RuntimeError(f"{BENCHMARK_NAME} S3 partitions are still missing under s3://{args.bucket}/{key}/")
    return local_graph if need_local else None


def run_standalone(graph_file: str, num_nodes: int, standalone_binary: str) -> Dict:
    """Run standalone WCC and return results."""
    result = subprocess.run(
        [standalone_binary, graph_file, str(num_nodes)],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Standalone error: {result.stderr}")
        return None
    
    try:
        data = json.loads(result.stdout)
        data['total_time_s'] = data.get('total_time_ms', 0) / 1000.0
        data['component_hash'] = canonical_component_hash(data.get('parent', []))
        return data
    except json.JSONDecodeError:
        print(f"Could not parse standalone output: {result.stdout}")
        return None


def run_burst(args, num_nodes: int, bucket: str, key: str) -> Tuple[int, float, float, Dict, str | None]:
    """Run burst WCC and return (num_components, algo_time_s, total_time_s, timing_details, component_hash)."""
    if args.backend == "s3":
        raise ValueError(
            "The deployed WCC action does not support the S3 middleware backend. "
            "Use --backend redis-list for the comparable graph campaign."
        )
    granularity = args.granularity if args.granularity else 1
    params = generate_payload(
        endpoint=args.wcc_endpoint,
        partitions=args.partitions,
        num_nodes=num_nodes,
        bucket=bucket,
        key=key,
        max_iterations=args.max_iterations if hasattr(args, 'max_iterations') else None,
        granularity=granularity,
        input_format=args.input_format,
    )
    
    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)
    
    host_submit = get_millis()
    dt = executor.burst(ACTION_NAME,
                        params,
                        file=ZIP_FILE,
                        memory=args.runtime_memory if args.runtime_memory else 2048,
                        custom_image=args.custom_image,
                        debug_mode=args.debug,
                        burst_size=granularity,
                        join=args.join,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        is_zip=True,
                        timeout=900000,
                        completion_timeout=120)
    finished = get_millis()
    
    dt_results = dt.get_results()
    
    num_components = None
    component_hash = None
    worker_starts = []
    worker_ends = []
    algo_starts = []
    algo_ends = []
    
    for sublist in dt_results:
        if isinstance(sublist, list):
            for item in sublist:
                if isinstance(item, dict):
                    if item.get('num_components') is not None:
                        num_components = item['num_components']
                    if item.get('component_hash') is not None:
                        component_hash = item['component_hash']
                    # Extract per-worker timestamps
                    for ts in item.get('timestamps', []):
                        if ts['key'] == 'worker_start':
                            worker_starts.append(int(ts['value']))
                        elif ts['key'] == 'worker_end':
                            worker_ends.append(int(ts['value']))
                        elif ts['key'] == 'local_uf_start':
                            algo_starts.append(int(ts['value']))
                        elif ts['key'] == 'global_merge_end':
                            algo_ends.append(int(ts['value']))

    total_time_s = (finished - host_submit) / 1000.0
    if worker_starts and worker_ends:
        warm_total_s = (max(worker_ends) - min(worker_starts)) / 1000.0
    else:
        warm_total_s = total_time_s
    if algo_starts and algo_ends:
        burst_time_s = (max(algo_ends) - min(algo_starts)) / 1000.0
    else:
        burst_time_s  = warm_total_s

    if worker_starts:
        cold_start_s = max(0.0, (min(worker_starts) - host_submit) / 1000.0)
        stagger_s = max(0.0, (max(worker_starts) - min(worker_starts)) / 1000.0)
    else:
        cold_start_s = 0.0
        stagger_s = 0.0
    
    timing_details = {
        "cold_start_ms":  round(cold_start_s  * 1000),
        "stagger_ms":     round(stagger_s     * 1000),
        "computation_ms": round(burst_time_s  * 1000),
        "warm_total_ms":  round(warm_total_s  * 1000),
        "total_ms": round(total_time_s * 1000),
    }
    return num_components, burst_time_s, total_time_s, timing_details, component_hash


def build_benchmark_summary(result: Dict) -> Dict:
    standalone = result.get("standalone", {})
    burst = result.get("burst", {})
    standalone_exec_ms = standalone.get("execution_time_ms")
    standalone_total_ms = standalone.get("total_time_ms", standalone_exec_ms)
    burst_algo_ms = burst.get("processing_time_ms", burst.get("time_ms"))
    burst_warm_total_ms = burst.get("total_time_ms", burst.get("time_ms"))
    burst_host_total_ms = burst.get("host_total_time_ms")
    if burst_host_total_ms is None:
        burst_host_total_ms = burst.get("timing_details", {}).get("total_ms")

    algo_speedup = None
    warm_speedup = None
    cold_speedup = None
    if standalone_exec_ms not in (None, 0) and burst_algo_ms not in (None, 0):
        algo_speedup = standalone_exec_ms / burst_algo_ms
    if standalone_total_ms not in (None, 0) and burst_warm_total_ms not in (None, 0):
        warm_speedup = standalone_total_ms / burst_warm_total_ms
    if standalone_total_ms not in (None, 0) and burst_host_total_ms not in (None, 0):
        cold_speedup = standalone_total_ms / burst_host_total_ms

    winner_span = "Burst" if algo_speedup is not None and algo_speedup > 1.0 else "Standalone" if algo_speedup is not None else None
    winner_warm = "Burst" if warm_speedup is not None and warm_speedup > 1.0 else "Standalone" if warm_speedup is not None else None
    winner_total = "Burst" if cold_speedup is not None and cold_speedup > 1.0 else "Standalone" if cold_speedup is not None else None
    primary_metric = "total" if cold_speedup is not None else ("warm" if warm_speedup is not None else "span")

    return {
        "algorithm": BENCHMARK_SLUG,
        "dataset": {
            "nodes": result.get("nodes"),
            "s3_prefix": result.get("s3_prefix"),
        },
        "configuration": {
            "partitions": result.get("partitions"),
        },
        "standalone": {
            "execution_time_ms": standalone_exec_ms,
            "total_time_ms": standalone_total_ms,
            "num_components": standalone.get("num_components"),
            "component_hash": standalone.get("component_hash"),
        },
        "burst": {
            "processing_time_ms": burst_algo_ms,
            "total_time_ms": burst_warm_total_ms,
            "host_total_time_ms": burst_host_total_ms,
            "num_components": burst.get("num_components"),
            "component_hash": burst.get("component_hash"),
            "timing_details": burst.get("timing_details"),
        },
        "speedup": {
            "algorithmic": algo_speedup,
            "warm_total": warm_speedup,
            "cold_total": cold_speedup,
            "overall": cold_speedup if cold_speedup is not None else warm_speedup,
        },
        "winner": {
            "span": winner_span,
            "warm": winner_warm,
            "total": winner_total,
            "primary_metric": primary_metric,
            "primary": winner_total if winner_total is not None else winner_warm if winner_warm is not None else winner_span,
        },
        "validation": {
            "requested": "standalone" in result and "burst" in result,
            "performed": "validation" in result,
            "passed": result.get("validation") == "PASSED",
            "skipped_reason": None if "validation" in result else "validation not available",
            "mode": "exact_hash" if "validation" in result else None,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Benchmark WCC: Burst vs Standalone")
    add_openwhisk_to_parser(parser)
    add_burst_to_parser(parser)
    
    # UF-specific arguments
    parser.add_argument("--wcc-endpoint", "--uf-endpoint", dest="wcc_endpoint", type=str, required=True,
                        help="Endpoint of the S3 service (for workers)")
    parser.add_argument("--local-endpoint", type=str, default="http://localhost:9000",
                        help="Local S3 endpoint (for downloading)")
    parser.add_argument("--partitions", type=int, default=4,
                        help="Number of partitions")
    parser.add_argument("--bucket", type=str, default="test-bucket",
                        help="S3 bucket name")
    parser.add_argument("--max-iterations", type=int, default=None,
                        help="Maximum iterations")
    parser.add_argument("--input-format", type=str, default="binary", choices=["binary", "tsv", "text"],
                        help="Input partition format in S3 (default: binary)")
    
    # Benchmark specific
    parser.add_argument("--sizes", type=str, default="5000,10000,50000,100000",
                        help="Comma-separated list of node counts to benchmark")
    parser.add_argument("--wcc-binary", "--ufst-binary", dest="wcc_binary", type=str,
                        default=os.path.join(HERE, "ufst/target/release/union-find"),
                        help="Path to standalone WCC binary")
    parser.add_argument("--graph-file", type=str, default=None,
                        help="Local graph file for standalone (skip S3 download). "
                             "If not set, downloads from S3.")
    parser.add_argument("--skip-standalone", action="store_true",
                        help="Skip standalone execution")
    parser.add_argument("--skip-burst", action="store_true",
                        help="Skip burst execution")
    parser.add_argument("--output", type=str, default="wcc_benchmark_results.json",
                        help="Output file for results")
    
    args = try_or_except(parser)
    
    sizes = [int(s.strip()) for s in args.sizes.split(',')]
    results = []
    
    print("=" * 60)
    print(f"{BENCHMARK_NAME} Benchmark: Burst vs Standalone")
    print("=" * 60)
    
    for num_nodes in sizes:
        key = f"{DATASET_PREFIX}/{DATASET_BASENAME}-{num_nodes}"
        print(f"\n{'='*60}")
        print(f"Testing with {num_nodes:,} nodes")
        print(f"{'='*60}")
        
        result = {
            "nodes": num_nodes,
            "partitions": args.partitions,
            "s3_prefix": key,
        }
        prepared_graph = ensure_input_data(args, num_nodes, key)
        
        # Use local file or download from S3 for standalone
        if not args.skip_standalone:
            if prepared_graph and os.path.exists(prepared_graph):
                temp_graph = prepared_graph
                delete_after = False
                print(f"\nUsing local graph file: {temp_graph}")
            else:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
                    temp_graph = f.name
                delete_after = True
                
                print(f"\nDownloading graph from S3...")
                num_edges = download_graph_from_s3(
                    args.bucket, key, args.local_endpoint, 
                    temp_graph, args.partitions, args.input_format
                )
                print(f"Downloaded {num_edges:,} unique edges")
                result["edges"] = num_edges
            
            # Run standalone
            print(f"\nRunning standalone {BENCHMARK_NAME}...")
            standalone_result = run_standalone(temp_graph, num_nodes, args.wcc_binary)
            
            if standalone_result:
                result["standalone"] = {
                    "num_components": standalone_result["num_components"],
                    "time_s": standalone_result["total_time_s"],
                    "execution_time_ms": standalone_result["execution_time_ms"],
                    "total_time_ms": standalone_result["total_time_ms"],
                    "component_hash": standalone_result.get("component_hash"),
                }
                print(f"  Components: {standalone_result['num_components']}")
                print(f"  Time: {standalone_result['total_time_s']:.3f}s")
                print(f"Standalone Processing Time (Execution): {standalone_result['execution_time_ms']} ms")
                print(f"Standalone Time (Total): {standalone_result['total_time_ms']} ms")
            else:
                print(f"Standalone Processing Time (Execution): FAILED")
            
            if delete_after:
                os.unlink(temp_graph)
        
        # Run burst
        if not args.skip_burst:
            clean_burst_cluster()
            print(f"\nRunning burst {BENCHMARK_NAME} ({args.partitions} workers)...")
            try:
                burst_components, burst_time, burst_total_time, timing, burst_hash = run_burst(args, num_nodes, args.bucket, key)
                burst_time_ms = burst_time * 1000
                burst_host_total_ms = burst_total_time * 1000
                burst_warm_total_ms = timing.get("warm_total_ms", burst_host_total_ms)
                result["burst"] = {
                    "num_components": burst_components,
                    "time_s": burst_time,
                    "time_ms": burst_time_ms,
                    "total_time_s": burst_total_time,
                    "total_time_ms": burst_warm_total_ms,
                    "host_total_time_ms": burst_host_total_ms,
                    "processing_time_ms": burst_time_ms,
                    "timing_details": timing,
                    "component_hash": burst_hash,
                }
                print(f"  Components: {burst_components}")
                print(f"  Time: {burst_total_time:.3f}s total")
                print(f"  Time: {burst_time:.3f}s  "
                      f"(cold_start: {timing['cold_start_ms']}ms, "
                      f"stagger: {timing['stagger_ms']}ms included in total)")
                print(f"Burst Time (Host Total / Cold): {burst_host_total_ms:.0f} ms")
                print(f"Burst Time (Load + Execution / Warm): {burst_warm_total_ms:.0f} ms")
                print(f"Burst Processing Time (Distributed Span): {burst_time_ms:.0f} ms")
            except Exception as e:
                print(f"  Error: {e}")
                result["burst"] = {"error": str(e)}
                print(f"Burst Time (Total): FAILED")
                print(f"Burst Processing Time (Distributed Span): FAILED")
        
        # Compute speedups
        st_exec_ms = result.get("standalone", {}).get("execution_time_ms")
        st_total_ms = result.get("standalone", {}).get("total_time_ms")
        bt_ms = result.get("burst", {}).get("time_ms")
        bt_total_ms = result.get("burst", {}).get("total_time_ms")
        if st_exec_ms and bt_ms and bt_ms > 0:
            algo_speedup = st_exec_ms / bt_ms
            print(f"\nAlgorithmic Speedup: {algo_speedup:.2f}x")
            if algo_speedup > 1.0:
                print("✓ Burst is faster!")
            else:
                print("✗ Standalone is faster")
        if st_total_ms and bt_total_ms and bt_total_ms > 0:
            total_speedup = st_total_ms / bt_total_ms
            print(f"Overall Speedup (Total): {total_speedup:.2f}x")
        
        # Validate
        if "standalone" in result and "burst" in result:
            standalone_comp = result["standalone"]["num_components"]
            burst_comp = result["burst"].get("num_components")
            standalone_hash = result["standalone"].get("component_hash")
            burst_hash = result["burst"].get("component_hash")

            if standalone_comp == burst_comp and standalone_hash and burst_hash and standalone_hash == burst_hash:
                print(f"\n✅ VALIDATION PASSED: Components and canonical partition hash match")
                result["validation"] = "PASSED"
            else:
                print(
                    f"\n❌ VALIDATION FAILED: "
                    f"Standalone components/hash={standalone_comp}/{standalone_hash}, "
                    f"Burst components/hash={burst_comp}/{burst_hash}"
                )
                result["validation"] = "FAILED"

        print(f"{BENCHMARK_JSON_PREFIX}{json.dumps(build_benchmark_summary(result), sort_keys=True)}")
        
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
