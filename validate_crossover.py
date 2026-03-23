#!/usr/bin/env python3
"""
Validate crossover point for Union-Find: Standalone vs Burst.

Runs strategic test points to find where burst becomes faster than standalone.
Based on known data (5M: standalone ~1.76s, burst ~5.3s), crossover is estimated
around 10-15M nodes. We test points around that range.
"""
import subprocess
import json
import os
import statistics
import sys
import time
from datetime import datetime

# Strategic test points around estimated crossover
TEST_POINTS = [
    5000000,    # 5M  - Standalone wins clearly
    8000000,    # 8M  - Getting closer
    10000000,   # 10M - Near crossover
    12000000,   # 12M - Near crossover
    15000000,   # 15M - Burst should win
]
RUNS = 5

PARTITIONS = 4
MEMORY = 2048
S3_ENDPOINT = os.environ.get("S3_WORKER_ENDPOINT", "http://minio-service.default.svc.cluster.local:9000")
LOCAL_ENDPOINT = os.environ.get("S3_HOST_ENDPOINT", "http://localhost:9000")
BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
EDGES_PER_NODE = 5
COMPONENTS = 10
PYTHON_CMD = os.environ.get("VALIDATION_PYTHON", sys.executable)
FLUSH_CMD = os.environ.get("UF_FLUSH_CMD", "kubectl exec pod/dragonfly -- redis-cli FLUSHALL")


def log(message):
    """Print with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def generate_graph(nodes):
    """Generate graph data (local file + S3)."""
    log(f"Generating {nodes/1e6:.1f}M node graph...")

    result = subprocess.run([
        PYTHON_CMD, "setup_large_uf_data.py",
        "--nodes", str(nodes),
        "--partitions", str(PARTITIONS),
        "--bucket", BUCKET,
        "--endpoint", LOCAL_ENDPOINT,
        "--edges-per-node", str(EDGES_PER_NODE),
        "--components", str(COMPONENTS),
    ], capture_output=True, text=True, timeout=600)

    if result.returncode != 0:
        log(f"❌ Failed to generate graph: {result.stderr}")
        return False

    log(f"✅ Graph generated successfully")
    return True


def run_benchmark(nodes, skip_generate=False):
    """Run benchmark for given size and return parsed results."""
    log(f"\n{'='*80}")
    log(f"BENCHMARKING: {nodes/1e6:.1f}M nodes")
    log(f"{'='*80}")

    # Generate graph
    if not skip_generate and not generate_graph(nodes):
        return None

    graph_file = f"uf_graph_{nodes}.tsv"

    # Flush Redis/Dragonfly for clean state
    if FLUSH_CMD:
        subprocess.run(FLUSH_CMD.split(), capture_output=True, timeout=30)

    # Run benchmark
    log(f"Running benchmark (Standalone + Burst)...")
    # We use a piped process to stream output in real-time
    process = subprocess.Popen([
        PYTHON_CMD, "benchmark_uf.py",
        "--ow-host", "localhost",
        "--ow-port", "31001",
        "--uf-endpoint", S3_ENDPOINT,
        "--local-endpoint", LOCAL_ENDPOINT,
        "--partitions", str(PARTITIONS),
        "--bucket", BUCKET,
        "--sizes", str(nodes),
        "--runtime-memory", str(MEMORY),
        "--backend", "redis-list",
        "--chunk-size", "262144",
        "--graph-file", graph_file,
        "--output", f"uf_crossover_{nodes}.json",
    ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    output = []
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            print(line, end='', flush=True)
            output.append(line)

    process.wait()
    full_output = "".join(output)

    if process.returncode != 0:
        log(f"❌ Benchmark failed for {nodes/1e6:.1f}M nodes")
        return None

    # Parse canonical timing lines
    standalone_time = None
    burst_time = None

    for line in full_output.split('\n'):
        if 'Standalone Processing Time (Execution):' in line:
            try:
                standalone_time = float(line.split(':')[1].strip().split()[0])
            except Exception:
                pass
        if 'Burst Processing Time (Distributed Span):' in line:
            try:
                burst_time = float(line.split(':')[1].strip().split()[0])
            except Exception:
                pass

    if standalone_time and burst_time:
        speedup = standalone_time / burst_time
        log(f"\n📊 Results for {nodes/1e6:.1f}M nodes:")
        log(f"   Standalone: {standalone_time:.2f} ms")
        log(f"   Burst:      {burst_time:.2f} ms")
        log(f"   Speedup:    {speedup:.2f}x")

        winner = "Burst" if speedup > 1.0 else "Standalone"
        log(f"   Winner:     {winner} ✅")

        return {
            'nodes': nodes,
            'standalone_ms': standalone_time,
            'burst_ms': burst_time,
            'speedup': speedup,
            'winner': winner
        }

    log(f"⚠️  Could not parse results")
    return None


def main():
    """Run crossover validation."""
    log("=" * 80)
    log("UNION-FIND CROSSOVER VALIDATION BENCHMARK")
    log("=" * 80)
    log(f"Testing {len(TEST_POINTS)} strategic points")
    log(f"Test points: {[f'{n/1e6:.1f}M' for n in TEST_POINTS]}")
    log(f"Runs per point: {RUNS}")
    log(f"Config: partitions={PARTITIONS}, memory={MEMORY}MB, "
        f"edges/node={EDGES_PER_NODE}, components={COMPONENTS}")
    log(f"Python runner: {PYTHON_CMD}")
    log("=" * 80)

    results = []

    for nodes in TEST_POINTS:
        sa_times = []
        bs_times = []

        for run_idx in range(RUNS):
            log(f"\n▶ Run {run_idx + 1}/{RUNS} — {nodes/1e6:.1f}M nodes")
            result = run_benchmark(nodes, skip_generate=run_idx > 0)
            if result:
                sa_times.append(result["standalone_ms"])
                bs_times.append(result["burst_ms"])
            else:
                log(f"⚠️  Run {run_idx + 1} failed, skipping")
            if run_idx < RUNS - 1:
                time.sleep(3)

        if not sa_times:
            log(f"⚠️  All runs failed for {nodes/1e6:.1f}M, skipping point")
            continue

        sa_mean = statistics.mean(sa_times)
        bs_mean = statistics.mean(bs_times)
        sa_std = statistics.stdev(sa_times) if len(sa_times) > 1 else 0.0
        bs_std = statistics.stdev(bs_times) if len(bs_times) > 1 else 0.0
        speedup = sa_mean / bs_mean if bs_mean > 0 else 0.0
        winner = "Burst" if speedup > 1.0 else "Standalone"

        log(f"\n📊 Aggregate {nodes/1e6:.1f}M ({len(sa_times)} runs):")
        log(f"   Standalone: {sa_mean:.1f} ± {sa_std:.1f} ms  (runs: {[f'{v:.0f}' for v in sa_times]})")
        log(f"   Burst span: {bs_mean:.1f} ± {bs_std:.1f} ms  (runs: {[f'{v:.0f}' for v in bs_times]})")
        log(f"   Speedup:    {speedup:.2f}x  →  {winner}")

        results.append({
            "nodes": nodes,
            "standalone_ms": round(sa_mean, 2),
            "standalone_std_ms": round(sa_std, 2),
            "standalone_runs_ms": sa_times,
            "burst_ms": round(bs_mean, 2),
            "burst_std_ms": round(bs_std, 2),
            "burst_runs_ms": bs_times,
            "speedup": round(speedup, 4),
            "winner": winner,
        })

        time.sleep(5)

    # Summary
    log("\n" + "=" * 80)
    log("CROSSOVER VALIDATION SUMMARY (MEAN OVER RUNS)")
    log("=" * 80)
    log(f"{'Nodes':>12} {'SA mean':>12} {'SA std':>9} {'BS mean':>12} {'BS std':>9} {'Speedup':>10} {'Winner':>12}")
    log("-" * 80)

    crossover_found = False
    crossover_point = None

    for i, r in enumerate(results):
        log(
            f"{r['nodes']/1e6:>10.1f}M "
            f"{r['standalone_ms']:>10.1f}ms "
            f"{r['standalone_std_ms']:>7.1f}ms "
            f"{r['burst_ms']:>10.1f}ms "
            f"{r['burst_std_ms']:>7.1f}ms "
            f"{r['speedup']:>9.2f}x "
            f"{r['winner']:>12}"
        )

        # Detect crossover via linear interpolation
        if i > 0 and not crossover_found:
            prev = results[i - 1]
            if prev['speedup'] < 1.0 and r['speedup'] >= 1.0:
                crossover_found = True
                m = (r['speedup'] - prev['speedup']) / (r['nodes'] - prev['nodes'])
                b = prev['speedup'] - m * prev['nodes']
                crossover_point = (1.0 - b) / m
                log("-" * 80)
                log(f"📍 CROSSOVER DETECTED between "
                    f"{prev['nodes']/1e6:.1f}M and {r['nodes']/1e6:.1f}M")
                log(f"📍 Refined estimate: {crossover_point/1e6:.2f}M nodes")
                log("-" * 80)

    log("=" * 80)

    if not crossover_found and results:
        last = results[-1]
        if last['speedup'] < 1.0:
            log("⚠️  Crossover NOT reached at largest test point. "
                "Try larger sizes or more partitions.")
        else:
            log("ℹ️  Burst already wins at smallest test point. "
                "Crossover is below tested range.")

    # Save results
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'test_points': TEST_POINTS,
        'runs_per_point': RUNS,
        'results': results,
        'crossover_estimate': crossover_point,
        'configuration': {
            'partitions': PARTITIONS,
            'memory_mb': MEMORY,
            'edges_per_node': EDGES_PER_NODE,
            'components': COMPONENTS
        }
    }

    with open('uf_crossover_validation_results.json', 'w') as f:
        json.dump(output_data, f, indent=2)

    log(f"💾 Results saved: uf_crossover_validation_results.json")
    log("✅ CROSSOVER VALIDATION COMPLETE")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        log(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
