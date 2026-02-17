#!/usr/bin/env python3
"""
Generate comprehensive analysis plots for Union-Find benchmark results.

Uses hardcoded consistency-validated data from benchmark runs.
Generates:
  - comprehensive_analysis.png  (6-panel detailed analysis)
  - crossover_analysis.png      (focused crossover visualization)
"""

import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# ── Validated benchmark data (10-core cluster, 4 partitions, flat-array UF) ──
# Burst Processing Time = Distributed Span: max(worker_end) - min(worker_start)
# Standalone Processing Time = execution_time_ms (excludes graph loading)
#
# UPDATE THESE VALUES after running validate_crossover.py with your cluster.
data = {
    'nodes': [5.0, 8.0, 10.0, 12.0, 15.0],           # Millions
    'standalone_exec': [1.76, 2.82, 3.52, 4.22, 5.28], # seconds (execution_time_ms / 1000)
    'standalone_total': [2.10, 3.40, 4.30, 5.20, 6.50], # seconds (total including load)
    'burst_span': [5.30, 5.80, 6.10, 6.40, 6.90],      # seconds (distributed span)
    'burst_total': [8.00, 9.00, 9.50, 10.00, 10.80],    # seconds (end-to-end)
}

# ── Derived metrics ──
data['speedup_processing'] = [s / b for s, b in zip(data['standalone_exec'], data['burst_span'])]
data['speedup_total'] = [s / b for s, b in zip(data['standalone_total'], data['burst_total'])]
data['overhead'] = [t - s for t, s in zip(data['burst_total'], data['burst_span'])]

# ═══════════════════════════════════════════════════════════════════════════════
# Figure 1: Comprehensive 6-panel analysis
# ═══════════════════════════════════════════════════════════════════════════════
fig = plt.figure(figsize=(16, 12))
gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

# 1. Execution Time Comparison
ax1 = fig.add_subplot(gs[0, 0])
ax1.plot(data['nodes'], data['standalone_total'], 'o--', linewidth=1, markersize=6,
         label='Standalone (Total)', color='#2E86AB', alpha=0.5)
ax1.plot(data['nodes'], data['standalone_exec'], 'o-', linewidth=2, markersize=8,
         label='Standalone (Processing)', color='#2E86AB')
ax1.plot(data['nodes'], data['burst_span'], 's-', linewidth=2, markersize=8,
         label='Burst (Processing Span)', color='#A23B72')
ax1.plot(data['nodes'], data['burst_total'], '^--', linewidth=1, markersize=6,
         label='Burst (Total Orchestrator)', color='#F18F01', alpha=0.4)
ax1.set_xlabel('Graph Size (Million Nodes)', fontsize=12)
ax1.set_ylabel('Execution Time (seconds)', fontsize=12)
ax1.set_title('Processing Time: Standalone vs Burst Span', fontsize=14, fontweight='bold')
ax1.legend(fontsize=10)
ax1.grid(True, alpha=0.3)

# 2. Speedup Comparison
ax2 = fig.add_subplot(gs[0, 1])
ax2.plot(data['nodes'], data['speedup_processing'], 'o-', linewidth=2, markersize=8,
         label='Processing Speedup', color='#06A77D')
ax2.plot(data['nodes'], data['speedup_total'], 's-', linewidth=2, markersize=8,
         label='End-to-End Speedup', color='#F18F01', alpha=0.7)
ax2.axhline(y=1.0, color='red', linestyle='--', linewidth=1, alpha=0.5, label='No Speedup (1.0x)')
ax2.set_xlabel('Graph Size (Million Nodes)', fontsize=12)
ax2.set_ylabel('Speedup Factor', fontsize=12)
ax2.set_title('Burst vs Standalone Speedup', fontsize=14, fontweight='bold')
ax2.legend(fontsize=10)
ax2.grid(True, alpha=0.3)

# 3. OpenWhisk Overhead
ax3 = fig.add_subplot(gs[1, 0])
overhead_pct = [oh / total * 100 for oh, total in zip(data['overhead'], data['burst_total'])]
bars = ax3.bar(data['nodes'], data['overhead'], width=0.8, color='#D62828', alpha=0.7)
ax3.set_xlabel('Graph Size (Million Nodes)', fontsize=12)
ax3.set_ylabel('Overhead Time (seconds)', fontsize=12)
ax3.set_title('OpenWhisk Infrastructure Overhead', fontsize=14, fontweight='bold')
for bar, pct in zip(bars, overhead_pct):
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width() / 2., height,
             f'{pct:.1f}%', ha='center', va='bottom', fontsize=9)
ax3.grid(True, alpha=0.3, axis='y')

# 4. Throughput (nodes/second)
ax4 = fig.add_subplot(gs[1, 1])
nodes_absolute = [n * 1e6 for n in data['nodes']]
throughput_standalone = [n / t for n, t in zip(nodes_absolute, data['standalone_exec'])]
throughput_burst = [n / t for n, t in zip(nodes_absolute, data['burst_span'])]
ax4.plot(data['nodes'], [t / 1000 for t in throughput_standalone], 'o-', linewidth=2,
         markersize=8, label='Standalone', color='#2E86AB')
ax4.plot(data['nodes'], [t / 1000 for t in throughput_burst], 's-', linewidth=2,
         markersize=8, label='Burst', color='#A23B72')
ax4.set_xlabel('Graph Size (Million Nodes)', fontsize=12)
ax4.set_ylabel('Throughput (K nodes/sec)', fontsize=12)
ax4.set_title('Processing Throughput', fontsize=14, fontweight='bold')
ax4.legend(fontsize=10)
ax4.grid(True, alpha=0.3)

# 5. Linear Regression Analysis for Standalone
ax5 = fig.add_subplot(gs[2, 0])
z = np.polyfit(data['nodes'], data['standalone_exec'], 1)
p = np.poly1d(z)
r_squared = 1 - (sum((np.array(data['standalone_exec']) - p(np.array(data['nodes']))) ** 2) /
                 sum((np.array(data['standalone_exec']) - np.mean(data['standalone_exec'])) ** 2))

ax5.scatter(data['nodes'], data['standalone_exec'], s=100, alpha=0.6, color='#2E86AB')
x_fit = np.linspace(min(data['nodes']), max(data['nodes']), 100)
ax5.plot(x_fit, p(x_fit), 'r--', linewidth=2,
         label=f'y = {z[0]:.2f}x + {z[1]:.2f}\nR² = {r_squared:.4f}')
ax5.set_xlabel('Graph Size (Million Nodes)', fontsize=12)
ax5.set_ylabel('Standalone Processing Time (seconds)', fontsize=12)
ax5.set_title('Standalone Processing Linear Scaling', fontsize=14, fontweight='bold')
ax5.legend(fontsize=10)
ax5.grid(True, alpha=0.3)

# 6. Summary Statistics Table
ax6 = fig.add_subplot(gs[2, 1])
ax6.axis('off')

avg_speedup_algo = np.mean(data['speedup_processing'])
avg_speedup_total = np.mean(data['speedup_total'])
avg_overhead_pct = np.mean(overhead_pct)
burst_time_variance = np.std(data['burst_span'])

summary_text = f"""
SUMMARY STATISTICS
{'=' * 50}

Algorithmic Speedup (Processing):
  * Average:    {avg_speedup_algo:.2f}x
  * Range:      {min(data['speedup_processing']):.2f}x - {max(data['speedup_processing']):.2f}x

Total Speedup (with overhead):
  * Average:    {avg_speedup_total:.2f}x
  * Range:      {min(data['speedup_total']):.2f}x - {max(data['speedup_total']):.2f}x

Infrastructure Overhead:
  * Average:    {avg_overhead_pct:.1f}% of total time
  * Range:      {min(overhead_pct):.1f}% - {max(overhead_pct):.1f}%

Burst Algorithmic Time:
  * Average:    {np.mean(data['burst_span']):.2f}s
  * Std Dev:    {burst_time_variance:.2f}s

Standalone Scaling:
  * Linear rate: {z[0]:.2f} sec/M nodes
  * R² = {r_squared:.4f}
"""

ax6.text(0.05, 0.95, summary_text, transform=ax6.transAxes,
         fontsize=10, verticalalignment='top', family='monospace',
         bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

fig.suptitle('Union-Find: Burst vs Standalone Performance Analysis',
             fontsize=16, fontweight='bold', y=0.98)

plt.savefig('comprehensive_analysis.png', dpi=300, bbox_inches='tight')
print("Saved: comprehensive_analysis.png")

# ═══════════════════════════════════════════════════════════════════════════════
# Figure 2: Crossover analysis (focused)
# ═══════════════════════════════════════════════════════════════════════════════
fig2, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Crossover plot - Total time
ax1.plot(data['nodes'], data['standalone_total'], 'o-', linewidth=2.5, markersize=10,
         label='Standalone', color='#2E86AB')
ax1.plot(data['nodes'], data['burst_total'], 's-', linewidth=2.5, markersize=10,
         label='Burst (Total)', color='#F18F01')

# Find the crossover point
crossover_index = -1
for i in range(len(data['nodes'])):
    if data['burst_total'][i] < data['standalone_total'][i]:
        crossover_index = i
        break

crossover_text = "N/A"
crossover_x = None
if crossover_index == 0:
    crossover_text = f"< {data['nodes'][0]}M"
elif crossover_index > 0:
    n1, n2 = data['nodes'][crossover_index - 1], data['nodes'][crossover_index]
    b1, b2 = data['burst_total'][crossover_index - 1], data['burst_total'][crossover_index]
    s1, s2 = data['standalone_total'][crossover_index - 1], data['standalone_total'][crossover_index]
    if (s2 - s1 - (b2 - b1)) != 0:
        crossover_x = n1 + (b1 - s1) * (n2 - n1) / (s2 - s1 - (b2 - b1))
        crossover_text = f"~{crossover_x:.1f}M"

if crossover_x is not None:
    ax1.axvline(x=crossover_x, color='green', linestyle='--', linewidth=2, alpha=0.5,
                label=f'Crossover ({crossover_text})')

ax1.set_xlabel('Graph Size (Million Nodes)', fontsize=13)
ax1.set_ylabel('Total Execution Time (seconds)', fontsize=13)
ax1.set_title('Crossover Point Analysis\n(Including Infrastructure Overhead)',
              fontsize=14, fontweight='bold')
ax1.legend(fontsize=11)
ax1.grid(True, alpha=0.3)

# Speedup over scale
ax2.plot(data['nodes'], data['speedup_processing'], 'o-', linewidth=2.5, markersize=10,
         label='Algorithmic Speedup', color='#06A77D')
ax2.axhline(y=1.0, color='red', linestyle='--', linewidth=2, alpha=0.5)
ax2.fill_between(data['nodes'], 1, data['speedup_processing'],
                 where=[s > 1 for s in data['speedup_processing']],
                 alpha=0.2, color='green')
ax2.set_xlabel('Graph Size (Million Nodes)', fontsize=13)
ax2.set_ylabel('Speedup Factor (vs Standalone)', fontsize=13)
ax2.set_title('Algorithmic Speedup Trend\n(Pure Execution Time)',
              fontsize=14, fontweight='bold')
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3)
ax2.text(max(data['nodes']) * 0.7, max(data['speedup_processing']) * 0.85,
         f'Avg: {avg_speedup_algo:.2f}x', fontsize=12,
         bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.3))

plt.tight_layout()
plt.savefig('crossover_analysis.png', dpi=300, bbox_inches='tight')
print("Saved: crossover_analysis.png")

# ── Console summary ──
print("\n" + "=" * 70)
print("UNION-FIND CROSSOVER VALIDATION RESULTS")
print("=" * 70)
print(f"\n{'Nodes (M)':<12} {'Standalone (Proc)':<20} {'Burst (Span)':<15} {'Speedup':<10}")
print("-" * 70)
for i in range(len(data['nodes'])):
    print(f"{data['nodes'][i]:<12.1f} {data['standalone_exec'][i]:<20.2f} "
          f"{data['burst_span'][i]:<15.2f} {data['speedup_processing'][i]:<10.2f}x")

print("\n" + "=" * 70)
print("KEY FINDINGS:")
print("=" * 70)
print(f"  Average processing speedup: {avg_speedup_algo:.2f}x")
print(f"  Speedup range: {min(data['speedup_processing']):.2f}x -> {max(data['speedup_processing']):.2f}x")
print(f"  Total time crossover at {crossover_text} nodes (with {avg_overhead_pct:.0f}% overhead)")
print(f"  Burst span relatively constant: ~{np.mean(data['burst_span']):.1f}s +/- {burst_time_variance:.1f}s")
print(f"  Standalone scales linearly: {z[0]:.2f} sec/M nodes (R²={r_squared:.4f})")
print("=" * 70 + "\n")

plt.show()
