import json
import os
import matplotlib.pyplot as plt
import glob

def generate_plots():
    results = []
    
    # Matching pattern for all result files generated during this session
    files = glob.glob("*results.json")
    
    for f in files:
        try:
            with open(f, 'r') as jf:
                data = json.load(jf)
                # Ensure we handle the list format
                if isinstance(data, list):
                    results.extend(data)
                else:
                    results.append(data)
        except Exception as e:
            print(f"Error reading {f}: {e}")

    if not results:
        print("No results found.")
        return

    # Filter and sort results by node count
    # Avoid duplicates by taking the latest/best run if needed, 
    # but here we just sort and plot all.
    unique_results = {}
    for r in results:
        nodes = r['nodes']
        # If we have multiple for the same size, we keep the one with better labels or just the last one
        unique_results[nodes] = r
    
    sorted_nodes = sorted(unique_results.keys())
    
    nodes_plot = []
    standalone_times = []
    burst_times = []
    speedups = []

    for n in sorted_nodes:
        r = unique_results[n]
        nodes_plot.append(n / 1_000_000) # Millions
        s_time = r['standalone']['time_s']
        b_time = r['burst']['time_s']
        standalone_times.append(s_time)
        burst_times.append(b_time)
        speedups.append(s_time / b_time)

    # Plot 1: Execution Time
    plt.figure(figsize=(10, 6))
    plt.plot(nodes_plot, standalone_times, marker='o', label='Standalone (Total)')
    plt.plot(nodes_plot, burst_times, marker='s', label='Burst (Distributed Span)')
    plt.xlabel('Nodes (Millions)')
    plt.ylabel('Time (seconds)')
    plt.title('Union-Find Benchmark: Standalone vs Burst')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.savefig('uf_performance_comparison.png')
    print("Saved uf_performance_comparison.png")

    # Plot 2: Speedup
    plt.figure(figsize=(10, 6))
    plt.plot(nodes_plot, speedups, marker='^', color='green', label='Speedup (Standalone / Burst)')
    plt.axhline(y=1.0, color='red', linestyle='--', label='Crossover Point (Speedup = 1.0)')
    plt.xlabel('Nodes (Millions)')
    plt.ylabel('Speedup')
    plt.title('Union-Find Speedup: Burst Mode over Standalone')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.savefig('uf_speedup.png')
    print("Saved uf_speedup.png")

    # Save a clean summary markdown table
    print("\nSummary Table:")
    print("| Nodes (M) | Standalone (s) | Burst (s) | Speedup |")
    print("|-----------|----------------|-----------|---------|")
    for n, s, b, sp in zip(nodes_plot, standalone_times, burst_times, speedups):
        print(f"| {n:9.2f} | {s:14.3f} | {b:9.3f} | {sp:7.2f}x |")

if __name__ == "__main__":
    generate_plots()
