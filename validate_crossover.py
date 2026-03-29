#!/usr/bin/env python3
"""
Validate Union-Find on the common graph-size grid used in the TFM.

Runs the same size progression as the other comparable graph algorithms so the
experimental section can discuss them on a homogeneous scale up to 5M nodes.
"""
import subprocess
import json
import os
import statistics
import sys
import time
from datetime import datetime


def env_test_points(default: list[int]) -> list[int]:
    raw = os.environ.get("TFM_TEST_POINTS", "").strip()
    if not raw:
        return default
    return [int(token.strip()) for token in raw.split(",") if token.strip()]


def env_runs(default: int) -> int:
    raw = os.environ.get("TFM_RUNS", "").strip()
    return int(raw) if raw else default


TEST_POINTS = env_test_points([
    100_000,
    500_000,
    1_000_000,
    2_000_000,
    3_000_000,
    5_000_000,
])
RUNS = env_runs(5)

PARTITIONS = 4
MEMORY = 2048
S3_ENDPOINT = os.environ.get("S3_WORKER_ENDPOINT", "http://minio-service.default.svc.cluster.local:9000")
LOCAL_ENDPOINT = os.environ.get("S3_HOST_ENDPOINT", "http://localhost:9000")
BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
EDGES_PER_NODE = 5
COMPONENTS = 10
PYTHON_CMD = os.environ.get("VALIDATION_PYTHON", sys.executable)
FLUSH_CMD = os.environ.get("UF_FLUSH_CMD", "kubectl exec pod/dragonfly -- redis-cli FLUSHALL")
BENCHMARK_JSON_PREFIX = "BENCHMARK_RESULT_JSON:"
OUTPUT_FILE = "uf_crossover_validation_results.json"


def log(message):
    """Print with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def winner_for(speedup):
    if speedup is None:
        return None
    return "Burst" if speedup > 1.0 else "Standalone"


def crossing_intervals(results, x_key):
    intervals = []
    for idx in range(1, len(results)):
        prev = results[idx - 1]
        curr = results[idx]
        prev_speedup = prev.get("speedup")
        curr_speedup = curr.get("speedup")
        if prev_speedup is None or curr_speedup is None:
            continue
        if (prev_speedup - 1.0) * (curr_speedup - 1.0) <= 0 and prev_speedup != curr_speedup:
            intervals.append((prev, curr))
    return intervals


def estimate_crossover(results, x_key):
    intervals = crossing_intervals(results, x_key)
    upward = [(prev, curr) for prev, curr in intervals if prev["speedup"] < 1.0 <= curr["speedup"]]
    if len(upward) != 1:
        return None, intervals
    prev, curr = upward[0]
    m = (curr["speedup"] - prev["speedup"]) / (curr[x_key] - prev[x_key])
    b = prev["speedup"] - m * prev[x_key]
    return (1.0 - b) / m, intervals


def save_checkpoint(results):
    if not results:
        return
    ordered_results = sorted(results, key=lambda entry: entry["nodes"])
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'test_points': TEST_POINTS,
        'runs_per_point': RUNS,
        'results': ordered_results,
        'crossover_estimate': None,
        'crossing_intervals': [],
        'crossover_warning': None,
        'configuration': {
            'partitions': PARTITIONS,
            'memory_mb': MEMORY,
            'edges_per_node': EDGES_PER_NODE,
            'components': COMPONENTS
        }
    }
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)
    log(f"💾 Checkpoint saved: {OUTPUT_FILE}")


def load_checkpoint():
    if not os.path.exists(OUTPUT_FILE):
        return []
    try:
        with open(OUTPUT_FILE) as f:
            data = json.load(f)
        results = data.get("results", [])
        if not isinstance(results, list):
            log("⚠️  Invalid checkpoint format, ignoring partial file")
            return []
        return results
    except Exception as exc:
        log(f"⚠️  Could not load checkpoint, ignoring partial file: {exc}")
        return []


def completed_run_count(entry):
    if not entry:
        return 0
    standalone_runs = len(entry.get("standalone_runs_ms", []))
    burst_runs = len(entry.get("burst_runs_ms", []))
    return min(standalone_runs, burst_runs)


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

    benchmark_summary = None
    for line in full_output.splitlines():
        if line.startswith(BENCHMARK_JSON_PREFIX):
            try:
                benchmark_summary = json.loads(line[len(BENCHMARK_JSON_PREFIX):])
            except json.JSONDecodeError as exc:
                log(f"⚠️  Could not decode benchmark JSON: {exc}")
            break

    if benchmark_summary is None:
        log("⚠️  Benchmark did not emit structured JSON summary")
        return None

    standalone_exec = benchmark_summary.get("standalone", {}).get("execution_time_ms")
    standalone_time = benchmark_summary.get("standalone", {}).get("total_time_ms")
    burst_span = benchmark_summary.get("burst", {}).get("processing_time_ms")
    burst_time = benchmark_summary.get("burst", {}).get("total_time_ms")
    burst_host_total = benchmark_summary.get("burst", {}).get("host_total_time_ms")
    if burst_host_total is None:
        burst_host_total = benchmark_summary.get("burst", {}).get("timing_details", {}).get("total_ms")

    if standalone_exec and standalone_time and burst_time:
        speedup_span = standalone_exec / burst_span if burst_span not in (None, 0) else None
        speedup = standalone_time / burst_time
        speedup_total = standalone_time / burst_host_total if burst_host_total not in (None, 0) else None
        winner_span = winner_for(speedup_span)
        winner_warm = winner_for(speedup)
        winner_total = winner_for(speedup_total)
        winner_primary = winner_total if winner_total is not None else winner_warm if winner_warm is not None else winner_span
        log(f"\n📊 Results for {nodes/1e6:.1f}M nodes:")
        log(f"   Standalone (load + exec): {standalone_time:.2f} ms")
        log(f"   Burst warm total:         {burst_time:.2f} ms")
        if burst_span is not None:
            log(f"   Burst span:               {burst_span:.2f} ms")
        if speedup_total is not None:
            log(f"   Speedup (cold): {speedup_total:.2f}x  →  {winner_total} ✅")
        log(f"   Speedup (warm): {speedup:.2f}x")
        if speedup_span is not None:
            log(f"   Speedup (span): {speedup_span:.2f}x")

        return {
            'nodes': nodes,
            'standalone_ms': standalone_exec,
            'standalone_exec_ms': standalone_exec,
            'standalone_total_ms': standalone_time,
            'burst_ms': burst_span,
            'burst_total_ms': burst_host_total,
            'burst_warm_ms': burst_time,
            'burst_span_ms': burst_span,
            'speedup': speedup_span,
            'speedup_total': speedup_total,
            'speedup_warm': speedup,
            'speedup_span': speedup_span,
            'winner_span': winner_span,
            'winner_warm': winner_warm,
            'winner_total': winner_total,
            'winner_primary': winner_primary,
            'primary_metric': 'total' if winner_total is not None else 'warm',
            'winner': winner_primary,
            'validation': benchmark_summary.get("validation", {}),
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

    results = load_checkpoint()
    results_by_node = {entry["nodes"]: entry for entry in results}
    completed_nodes = {
        nodes for nodes, entry in results_by_node.items()
        if completed_run_count(entry) >= RUNS
    }
    if completed_nodes:
        log(f"♻️  Resuming from checkpoint: completed nodes = {sorted(completed_nodes)}")

    for nodes in TEST_POINTS:
        existing_entry = results_by_node.get(nodes)
        existing_runs = completed_run_count(existing_entry)
        if nodes in completed_nodes:
            log(f"⏭️  Skipping {nodes/1e6:.1f}M nodes (already checkpointed)")
            continue
        if existing_runs:
            log(f"♻️  Resuming incomplete point {nodes/1e6:.1f}M with {existing_runs}/{RUNS} runs already saved")

        sa_exec_times = list(existing_entry.get("standalone_runs_ms", [])) if existing_entry else []
        bs_span_times = list(existing_entry.get("burst_runs_ms", [])) if existing_entry else []
        sa_total_times = list(existing_entry.get("standalone_total_runs_ms", [])) if existing_entry else []
        if existing_entry and not sa_total_times and existing_entry.get("standalone_total_ms") is not None and sa_exec_times:
            sa_total_times.extend([existing_entry["standalone_total_ms"]] * existing_runs)
        bs_warm_times = list(existing_entry.get("burst_warm_runs_ms", [])) if existing_entry else []
        if existing_entry and not bs_warm_times and existing_entry.get("burst_warm_ms") is not None and bs_span_times:
            bs_warm_times.extend([existing_entry["burst_warm_ms"]] * existing_runs)
        bs_total_times = list(existing_entry.get("burst_total_runs_ms", [])) if existing_entry else []
        if existing_entry and not bs_total_times and existing_entry.get("burst_total_ms") is not None and bs_span_times:
            bs_total_times.extend([existing_entry["burst_total_ms"]] * existing_runs)
        validation_state = existing_entry.get("validation") if existing_entry else None

        for run_idx in range(existing_runs, RUNS):
            log(f"\n▶ Run {run_idx + 1}/{RUNS} — {nodes/1e6:.1f}M nodes")
            result = run_benchmark(nodes, skip_generate=run_idx > 0 or existing_runs > 0)
            if result:
                sa_exec_times.append(result["standalone_ms"])
                bs_span_times.append(result["burst_ms"])
                if result.get("standalone_total_ms") is not None:
                    sa_total_times.append(result["standalone_total_ms"])
                if result.get("burst_warm_ms") is not None:
                    bs_warm_times.append(result["burst_warm_ms"])
                if result.get("burst_total_ms") is not None:
                    bs_total_times.append(result["burst_total_ms"])
                validation_state = result.get("validation")
            else:
                log(f"⚠️  Run {run_idx + 1} failed, skipping")
            if run_idx < RUNS - 1:
                time.sleep(3)

        if not sa_exec_times or not sa_total_times or not bs_warm_times:
            log(f"⚠️  All runs failed for {nodes/1e6:.1f}M, skipping point")
            continue

        sa_exec_mean = statistics.mean(sa_exec_times)
        bs_span_mean = statistics.mean(bs_span_times)
        sa_exec_std = statistics.stdev(sa_exec_times) if len(sa_exec_times) > 1 else 0.0
        bs_span_std = statistics.stdev(bs_span_times) if len(bs_span_times) > 1 else 0.0
        sa_total_mean = statistics.mean(sa_total_times)
        bs_warm_mean = statistics.mean(bs_warm_times)
        sa_total_std = statistics.stdev(sa_total_times) if len(sa_total_times) > 1 else 0.0
        bs_warm_std = statistics.stdev(bs_warm_times) if len(bs_warm_times) > 1 else 0.0
        bs_total_mean = statistics.mean(bs_total_times) if bs_total_times else None
        speedup = sa_exec_mean / bs_span_mean if bs_span_mean > 0 else 0.0
        speedup_warm = sa_total_mean / bs_warm_mean if bs_warm_mean > 0 else 0.0
        speedup_total = None
        if sa_total_mean is not None and bs_total_mean not in (None, 0):
            speedup_total = sa_total_mean / bs_total_mean
        winner_span = winner_for(speedup)
        winner_warm = winner_for(speedup_warm)
        winner_total = winner_for(speedup_total)
        winner_primary = winner_total if winner_total is not None else winner_warm if winner_warm is not None else winner_span

        log(f"\n📊 Aggregate {nodes/1e6:.1f}M ({len(sa_total_times)} runs):")
        log(f"   Standalone total: {sa_total_mean:.1f} ± {sa_total_std:.1f} ms  (runs: {[f'{v:.0f}' for v in sa_total_times]})")
        log(f"   Burst warm total: {bs_warm_mean:.1f} ± {bs_warm_std:.1f} ms  (runs: {[f'{v:.0f}' for v in bs_warm_times]})")
        if speedup_total is not None:
            log(f"   Speedup (cold): {speedup_total:.2f}x  →  {winner_total}")
        log(f"   Speedup (warm): {speedup_warm:.2f}x")
        log(f"   Span secondary: SA exec {sa_exec_mean:.1f} ± {sa_exec_std:.1f} ms vs Burst span {bs_span_mean:.1f} ± {bs_span_std:.1f} ms")

        results = [entry for entry in results if entry["nodes"] != nodes]
        updated_entry = {
            "nodes": nodes,
            "standalone_ms": round(sa_exec_mean, 2),
            "standalone_exec_ms": round(sa_exec_mean, 2),
            "standalone_std_ms": round(sa_exec_std, 2),
            "standalone_runs_ms": sa_exec_times,
            "burst_ms": round(bs_span_mean, 2),
            "burst_span_ms": round(bs_span_mean, 2),
            "burst_std_ms": round(bs_span_std, 2),
            "burst_runs_ms": bs_span_times,
            "speedup": round(speedup, 4),
            "speedup_span": round(speedup, 4),
            "standalone_total_ms": round(sa_total_mean, 2) if sa_total_mean is not None else None,
            "standalone_total_runs_ms": sa_total_times,
            "burst_total_ms": round(bs_total_mean, 2) if bs_total_mean is not None else None,
            "burst_total_runs_ms": bs_total_times,
            "speedup_total": round(speedup_total, 4) if speedup_total is not None else None,
            "burst_warm_ms": round(bs_warm_mean, 2),
            "burst_warm_runs_ms": bs_warm_times,
            "speedup_warm": round(speedup_warm, 4),
            "winner_span": winner_span,
            "winner_warm": winner_warm,
            "winner_total": winner_total,
            "winner_primary": winner_primary,
            "primary_metric": "total" if winner_total is not None else "warm",
            "winner": winner_primary,
            "validation": validation_state or {},
        }
        results.append(updated_entry)
        results_by_node[nodes] = updated_entry
        save_checkpoint(results)

        time.sleep(5)

    # Summary
    log("\n" + "=" * 80)
    log("CROSSOVER VALIDATION SUMMARY (MEAN OVER RUNS)")
    log("=" * 80)
    log(f"{'Nodes':>12} {'SA mean':>12} {'SA std':>9} {'BS mean':>12} {'BS std':>9} {'Speedup':>10} {'Winner':>12}")
    log("-" * 80)

    results = sorted(results, key=lambda entry: entry["nodes"])
    crossover_point, intervals = estimate_crossover(results, "nodes")

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

    if crossover_point is not None:
        prev, curr = [pair for pair in intervals if pair[0]["speedup"] < 1.0 <= pair[1]["speedup"]][0]
        log("-" * 80)
        log(f"📍 CROSSOVER DETECTED between "
            f"{prev['nodes']/1e6:.1f}M and {curr['nodes']/1e6:.1f}M")
        log(f"📍 Refined estimate: {crossover_point/1e6:.2f}M nodes")
        log("-" * 80)
    elif len(intervals) > 1:
        log("⚠️  Multiple speedup sign changes detected; crossover estimate omitted as ambiguous")

    log("=" * 80)

    crossover_found = crossover_point is not None
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
        'crossing_intervals': [[prev['nodes'], curr['nodes']] for prev, curr in intervals],
        'crossover_warning': 'multiple_sign_changes' if len(intervals) > 1 else None,
        'configuration': {
            'partitions': PARTITIONS,
            'memory_mb': MEMORY,
            'edges_per_node': EDGES_PER_NODE,
            'components': COMPONENTS
        }
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)

    log(f"💾 Results saved: {OUTPUT_FILE}")
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
