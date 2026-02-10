//! # Graph Generator for Union-Find Testing
//!
//! Generates random graphs with known connected components for testing.

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <output_file> <num_nodes> <num_edges> [num_components]", args[0]);
        eprintln!("");
        eprintln!("Arguments:");
        eprintln!("  output_file     Output TSV file path");
        eprintln!("  num_nodes       Total number of nodes");
        eprintln!("  num_edges       Number of edges to generate");
        eprintln!("  num_components  Target number of components (optional, default=1)");
        std::process::exit(1);
    }

    let output_file = &args[1];
    let num_nodes: u32 = args[2].parse().expect("Invalid num_nodes");
    let num_edges: u32 = args[3].parse().expect("Invalid num_edges");
    let num_components: u32 = args.get(4)
        .map(|s| s.parse().expect("Invalid num_components"))
        .unwrap_or(1);

    eprintln!("Generating graph with {} nodes, {} edges, ~{} components", 
              num_nodes, num_edges, num_components);

    let file = File::create(output_file).expect("Could not create output file");
    let mut writer = BufWriter::new(file);

    // Simple LCG random number generator (deterministic for reproducibility)
    let mut seed: u64 = 12345;
    let lcg_next = |s: &mut u64| -> u64 {
        *s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        *s
    };

    // Divide nodes into components
    let nodes_per_component = num_nodes / num_components;
    
    // First, create a spanning tree within each component to ensure connectivity
    for c in 0..num_components {
        let start = c * nodes_per_component;
        let end = if c == num_components - 1 { num_nodes } else { (c + 1) * nodes_per_component };
        
        // Connect each node to a previous node in the component (creates a tree)
        for node in (start + 1)..end {
            let target = start + ((lcg_next(&mut seed) as u32) % (node - start));
            writeln!(writer, "{}\t{}", node, target).unwrap();
        }
    }

    // Add random edges within components
    let edges_per_component = (num_edges / num_components).saturating_sub(nodes_per_component);
    for c in 0..num_components {
        let start = c * nodes_per_component;
        let end = if c == num_components - 1 { num_nodes } else { (c + 1) * nodes_per_component };
        let component_size = end - start;
        
        if component_size < 2 {
            continue;
        }

        for _ in 0..edges_per_component {
            let src = start + ((lcg_next(&mut seed) as u32) % component_size);
            let dst = start + ((lcg_next(&mut seed) as u32) % component_size);
            if src != dst {
                writeln!(writer, "{}\t{}", src, dst).unwrap();
            }
        }
    }

    eprintln!("Graph written to {}", output_file);
}
