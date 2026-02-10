//! # Union-Find CLI
//!
//! Command-line interface for running the Union-Find algorithm on graph files.

use union_find::{run_union_find_edges, UnionFindResult};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <graph_file> <num_nodes>", args[0]);
        eprintln!("");
        eprintln!("Arguments:");
        eprintln!("  graph_file  TSV file with edges (src\\tdst)");
        eprintln!("  num_nodes   Total number of nodes in the graph");
        std::process::exit(1);
    }

    let graph_file = &args[1];
    let num_nodes: u32 = args[2].parse().expect("Invalid num_nodes");

    // Load graph
    let start_load = Instant::now();
    let edges = load_graph(graph_file);
    let load_duration = start_load.elapsed();

    eprintln!("Loaded {} edges from {}", edges.len(), graph_file);

    // Run Union-Find
    let res = run_union_find_edges(&edges, num_nodes);

    eprintln!("Found {} connected components", res.num_components);

    // Output results in JSON format
    let result = UnionFindResult {
        parent: res.parent,
        num_components: res.num_components,
        load_time_ms: load_duration.as_millis(),
        execution_time_ms: res.execution_time_ms,
        total_time_ms: load_duration.as_millis() + res.execution_time_ms,
    };

    println!("{}", serde_json::to_string(&result).unwrap());
}

/// Load graph from TSV file
/// Format: src\tdst (optionally with a third column that's ignored)
fn load_graph(path: &str) -> Vec<(u32, u32)> {
    let file = File::open(path).expect("Could not open graph file");
    let reader = BufReader::new(file);

    let mut edges: Vec<(u32, u32)> = Vec::new();

    for line in reader.lines() {
        let line = line.expect("Could not read line");
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 2 {
            continue;
        }

        let src: u32 = parts[0].parse().expect("Invalid src node");
        let dst: u32 = parts[1].parse().expect("Invalid dst node");

        edges.push((src, dst));
    }

    edges
}
