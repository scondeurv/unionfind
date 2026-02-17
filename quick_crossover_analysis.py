#!/usr/bin/env python3
"""
Quick crossover analysis for Union-Find using existing data and interpolation.

Uses known benchmark data points to estimate the crossover point where
burst (distributed) execution becomes faster than standalone.
"""
import json
import matplotlib.pyplot as plt
import numpy as np

# ── Known data points from benchmarks ──
# (nodes, standalone_ms, burst_total_ms, burst_span_ms)
# UPDATE THESE with your actual benchmark results.
DATA_POINTS = [
    (5000000,   1760,  8000,  5300),   # 5M
    (10000000,  3520,  9500,  6100),   # 10M  (estimated)
    (15000000,  5280,  10800, 6900),   # 15M  (estimated)
]


def calculate_speedup(standalone, burst):
    """Calculate speedup ratio."""
    return standalone / burst if burst > 0 else 0


def interpolate_crossover():
    """Find crossover point using linear interpolation on total times."""
    # Find two points that straddle the crossover (standalone_ms < burst_total_ms -> >=)
    for i in range(1, len(DATA_POINTS)):
        prev_nodes, prev_st, prev_bt, _ = DATA_POINTS[i - 1]
        curr_nodes, curr_st, curr_bt, _ = DATA_POINTS[i]

        prev_speedup = calculate_speedup(prev_st, prev_bt)
        curr_speedup = calculate_speedup(curr_st, curr_bt)

        print(f"Point: {prev_nodes:>12,} nodes -> total speedup {prev_speedup:.2f}x")

        if prev_speedup < 1.0 and curr_speedup >= 1.0:
            # Linear interpolation
            m = (curr_speedup - prev_speedup) / (curr_nodes - prev_nodes)
            b = prev_speedup - m * prev_nodes
            crossover = (1.0 - b) / m
            print(f"Point: {curr_nodes:>12,} nodes -> total speedup {curr_speedup:.2f}x")
            print(f"\nEstimated crossover point: {int(crossover):,} nodes")
            print(f"   (approximately {crossover / 1e6:.2f} million nodes)")
            return int(crossover)

    # Print last point
    last_nodes, last_st, last_bt, _ = DATA_POINTS[-1]
    last_speedup = calculate_speedup(last_st, last_bt)
    print(f"Point: {last_nodes:>12,} nodes -> total speedup {last_speedup:.2f}x")

    if last_speedup < 1.0:
        print("\nCrossover NOT reached. Standalone still wins at largest test point.")
        # Extrapolate
        n1, s1, b1, _ = DATA_POINTS[-2]
        n2, s2, b2, _ = DATA_POINTS[-1]
        sp1, sp2 = s1 / b1, s2 / b2
        if sp2 != sp1:
            m = (sp2 - sp1) / (n2 - n1)
            b = sp1 - m * n1
            crossover = (1.0 - b) / m
            print(f"   Extrapolated estimate: ~{crossover / 1e6:.2f}M nodes")
            return int(crossover)
    else:
        # Burst already wins at first point
        print(f"\nBurst already wins at smallest test point ({DATA_POINTS[0][0]:,} nodes).")
        return DATA_POINTS[0][0]

    return None


def plot_crossover_analysis(crossover):
    """Generate comprehensive 4-panel crossover visualization."""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

    nodes_points = [p[0] / 1e6 for p in DATA_POINTS]
    standalone_s = [p[1] / 1000 for p in DATA_POINTS]
    burst_total_s = [p[2] / 1000 for p in DATA_POINTS]
    burst_span_s = [p[3] / 1000 for p in DATA_POINTS]

    # Interpolated curves
    nodes_data = np.array([p[0] for p in DATA_POINTS])
    standalone_data = np.array([p[1] for p in DATA_POINTS])
    burst_data = np.array([p[2] for p in DATA_POINTS])
    nodes_range = np.linspace(nodes_data.min(), nodes_data.max(), 100)

    st_fit = np.polyfit(nodes_data, standalone_data, 1)
    bt_fit = np.polyfit(nodes_data, burst_data, 1)
    st_curve = np.polyval(st_fit, nodes_range)
    bt_curve = np.polyval(bt_fit, nodes_range)

    # Plot 1: Execution Time Comparison
    ax1.plot(nodes_range / 1e6, st_curve / 1000, '-', linewidth=2,
             label='Standalone (interpolated)', color='#3498db', alpha=0.6)
    ax1.plot(nodes_range / 1e6, bt_curve / 1000, '-', linewidth=2,
             label='Burst Total (interpolated)', color='#e74c3c', alpha=0.6)
    ax1.plot(nodes_points, standalone_s, 'o', markersize=12,
             label='Standalone (measured)', color='#2980b9', zorder=5)
    ax1.plot(nodes_points, burst_total_s, 's', markersize=12,
             label='Burst Total (measured)', color='#c0392b', zorder=5)
    ax1.plot(nodes_points, burst_span_s, '^', markersize=10,
             label='Burst Span (measured)', color='#e67e22', zorder=5)

    if crossover is not None:
        ax1.axvline(x=crossover / 1e6, color='green', linestyle='--', linewidth=2.5,
                    label=f'Crossover: {crossover / 1e6:.1f}M', alpha=0.8)

    ax1.set_xlabel('Graph Size (millions of nodes)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax1.set_title('Standalone vs Burst: Performance Comparison', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=9, loc='upper left')
    ax1.grid(True, alpha=0.3)

    # Plot 2: Speedup Over Graph Size
    speedup_total = [s / b for s, b in zip(standalone_s, burst_total_s)]
    speedup_span = [s / b for s, b in zip(standalone_s, burst_span_s)]

    ax2.plot(nodes_points, speedup_total, 'o-', linewidth=2, markersize=10,
             label='Total Speedup', color='purple')
    ax2.plot(nodes_points, speedup_span, 's-', linewidth=2, markersize=10,
             label='Algorithmic Speedup (vs Span)', color='darkviolet')
    ax2.axhline(y=1.0, color='black', linestyle='--', linewidth=2, label='Break-even (1x)')

    for n, s in zip(nodes_points, speedup_total):
        ax2.annotate(f'{s:.2f}x', xy=(n, s), xytext=(5, 5),
                     textcoords='offset points', fontsize=10, fontweight='bold')

    ax2.set_xlabel('Graph Size (millions of nodes)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Speedup (Standalone / Burst)', fontsize=12, fontweight='bold')
    ax2.set_title('Burst Speedup vs Graph Size', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=9)
    ax2.grid(True, alpha=0.3)

    # Plot 3: Algorithmic Speedup (excluding OW overhead)
    algo_speedup = [s / a for s, a in zip(standalone_s, burst_span_s)]
    colors = ['red' if sp < 1 else 'green' for sp in algo_speedup]
    bars = ax3.bar(nodes_points, algo_speedup, width=0.8, color=colors,
                   alpha=0.7, edgecolor='black', linewidth=2)
    ax3.axhline(y=1.0, color='black', linestyle='--', linewidth=2)
    ax3.set_xlabel('Graph Size (millions of nodes)', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Algorithmic Speedup', fontsize=12, fontweight='bold')
    ax3.set_title('Pure Algorithm Performance (no OpenWhisk overhead)', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')

    for bar, val in zip(bars, algo_speedup):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width() / 2., height, f'{val:.2f}x',
                 ha='center', va='bottom', fontsize=11, fontweight='bold')

    # Plot 4: OpenWhisk Overhead
    overheads = [(b - a) / b * 100 for b, a in zip(burst_total_s, burst_span_s)]
    bars = ax4.bar(nodes_points, overheads, width=0.8, color='orange', alpha=0.7,
                   edgecolor='black', linewidth=2)
    ax4.set_xlabel('Graph Size (millions of nodes)', fontsize=12, fontweight='bold')
    ax4.set_ylabel('OpenWhisk Overhead (%)', fontsize=12, fontweight='bold')
    ax4.set_title('OpenWhisk Infrastructure Overhead', fontsize=14, fontweight='bold')
    ax4.grid(True, alpha=0.3, axis='y')
    ax4.set_ylim(0, 100)

    for bar, val in zip(bars, overheads):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width() / 2., height, f'{val:.1f}%',
                 ha='center', va='bottom', fontsize=11, fontweight='bold')

    plt.tight_layout()
    plt.savefig('uf_crossover_analysis.png', dpi=150, bbox_inches='tight')
    print("\nSaved: uf_crossover_analysis.png")


def generate_report(crossover):
    """Generate text report."""
    print("\n" + "=" * 80)
    print("UNION-FIND CROSSOVER ANALYSIS REPORT")
    print("=" * 80)

    print(f"\n{'Nodes':<15} {'Standalone':<15} {'Burst Total':<15} {'Burst Span':<15} {'Speedup':<15}")
    print("-" * 80)

    for nodes, standalone, burst_total, burst_span in DATA_POINTS:
        speedup = standalone / burst_total
        print(f"{nodes:>13,}  {standalone / 1000:>13.2f}s  {burst_total / 1000:>13.2f}s  "
              f"{burst_span / 1000:>13.2f}s  {speedup:>13.2f}x")

    if crossover:
        print(f"\nCROSSOVER POINT: ~{crossover:,} nodes ({crossover / 1e6:.2f}M)")
    else:
        print("\nCROSSOVER POINT: Not reached in tested range")

    print(f"\nINSIGHTS:")
    print(f"   Union-Find is algorithmically very fast (O(alpha(n)) per operation)")
    print(f"   Standalone is sub-second even for 1M nodes")
    print(f"   Burst overhead is dominated by infrastructure (S3 I/O, middleware, OW scheduling)")
    algo_speedup_last = DATA_POINTS[-1][1] / DATA_POINTS[-1][3]
    overhead_last = (DATA_POINTS[-1][2] - DATA_POINTS[-1][3]) / DATA_POINTS[-1][2] * 100
    print(f"   Algorithmic speedup at {DATA_POINTS[-1][0] / 1e6:.0f}M: {algo_speedup_last:.2f}x")
    print(f"   OpenWhisk overhead: ~{overhead_last:.0f}% of total burst time")

    # Save JSON
    output = {
        'crossover_nodes': crossover,
        'crossover_millions': round(crossover / 1e6, 2) if crossover else None,
        'data_points': [
            {
                'nodes': p[0],
                'standalone_ms': p[1],
                'burst_total_ms': p[2],
                'burst_span_ms': p[3],
                'speedup_total': p[1] / p[2],
                'speedup_algo': p[1] / p[3],
                'overhead_pct': (p[2] - p[3]) / p[2] * 100
            } for p in DATA_POINTS
        ]
    }

    with open('uf_crossover_data.json', 'w') as f:
        json.dump(output, f, indent=2)
    print("\nSaved: uf_crossover_data.json")


def main():
    print("=" * 80)
    print("  UNION-FIND CROSSOVER ANALYSIS")
    print("=" * 80)
    print()

    crossover = interpolate_crossover()

    print("\nGenerating visualizations...")
    plot_crossover_analysis(crossover)

    generate_report(crossover)

    print("\n" + "=" * 80)
    print("  ANALYSIS COMPLETE")
    print("=" * 80)
    print("\nGenerated files:")
    print("  uf_crossover_analysis.png - 4-panel visualization")
    print("  uf_crossover_data.json    - Raw data and metrics")


if __name__ == '__main__':
    main()
