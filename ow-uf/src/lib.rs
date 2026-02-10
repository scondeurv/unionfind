//! # Optimized Distributed Union-Find (2-Phase Merge)
//!
//! This module implements an optimized distributed Union-Find algorithm
//! designed for serverless environments.
//!
//! ## Algorithm Overview
//!
//! 1. **Phase 1: Local UF**: Each worker processes its assigned partitions locally 
//!    using path compression and union by rank.
//! 2. **Phase 2: Global Merge**: Workers identify "boundary" roots (roots of elements 
//!    that appeared in their partitions but might be connected in others). 
//!    They send root equivalences to the root worker.
//! 3. **Phase 3: Final Consolidation**: The root worker merges everything and broadcasts 
//!    the final component mappings.
//!
//! This reduces communication from O(log N) sync rounds to exactly 2 rounds.

use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, HashSet};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use burst_communication_middleware::{Middleware, MiddlewareActorHandle};
use bytes::Bytes;
use futures::future::join_all;
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};

const ROOT_WORKER: u32 = 0;

/// Root mapping for final broadcast (Node ID -> Root ID)
#[derive(Debug, Clone, PartialEq)]
pub struct ParentMessage(pub Vec<(u32, u32)>);

impl From<Bytes> for ParentMessage {
    fn from(bytes: Bytes) -> Self {
        let mut result = Vec::with_capacity(bytes.len() / 8);
        for chunk in bytes.chunks_exact(8) {
            let u = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
            let v = u32::from_le_bytes(chunk[4..8].try_into().unwrap());
            result.push((u, v));
        }
        ParentMessage(result)
    }
}

impl From<ParentMessage> for Bytes {
    fn from(val: ParentMessage) -> Self {
        let mut bytes = Vec::with_capacity(val.0.len() * 8);
        for (u, v) in val.0 {
            bytes.extend_from_slice(&u.to_le_bytes());
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        Bytes::from(bytes)
    }
}

/// Local Union-Find structure with path compression and union by rank (Sparse)
struct LocalUnionFind {
    parent: HashMap<u32, u32>,
    rank: HashMap<u32, u32>,
}

impl LocalUnionFind {
    fn new() -> Self {
        LocalUnionFind {
            parent: HashMap::new(),
            rank: HashMap::new(),
        }
    }

    fn find(&mut self, i: u32) -> u32 {
        let mut root = i;
        while let Some(&p) = self.parent.get(&root) {
            if p == root { break; }
            root = p;
        }

        // Path compression
        let mut curr = i;
        while let Some(&p) = self.parent.get(&curr) {
            if p == root { break; }
            self.parent.insert(curr, root);
            curr = p;
        }
        root
    }

    fn union(&mut self, i: u32, j: u32) {
        let root_i = self.find(i);
        let root_j = self.find(j);
        if root_i != root_j {
            let rank_i = *self.rank.get(&root_i).unwrap_or(&0);
            let rank_j = *self.rank.get(&root_j).unwrap_or(&0);

            if rank_i < rank_j {
                self.parent.insert(root_i, root_j);
            } else if rank_i > rank_j {
                self.parent.insert(root_j, root_i);
            } else {
                self.parent.insert(root_i, root_j);
                self.rank.insert(root_j, rank_i + 1);
            }
        }
    }
}

/// Input parameters for the Union-Find action
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    input_data: S3InputParams,
    num_nodes: u32,
    partitions: u32,
    granularity: u32,
    timeout_seconds: Option<u64>,
}

/// S3 configuration for fetching graph data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S3InputParams {
    bucket: String,
    key: String,
    region: String,
    endpoint: Option<String>,
    aws_access_key_id: String,
    aws_secret_access_key: String,
    aws_session_token: Option<String>,
}

/// Resulting output of the action execution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    bucket: String,
    key: String,
    timestamps: Vec<Timestamp>,
    num_components: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Timestamp {
    key: String,
    value: String,
}

fn timestamp(key: &str) -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    Timestamp {
        key: key.to_string(),
        value: now.to_string(),
    }
}

/// Edge list for this worker's partition
struct EdgePartition {
    edges: Vec<(u32, u32)>,
}

async fn load_partition(
    params: &Input,
    s3_client: &S3Client,
    worker_id: u32,
) -> EdgePartition {
    let start_part = worker_id * params.granularity;
    let end_part = (worker_id + 1) * params.granularity;

    let mut fetch_futures = Vec::new();
    for p in start_part..end_part {
        let part_key = format!("{}/part-{:05}", params.input_data.key, p);
        fetch_futures.push(async move {
            (p, s3_client
                .get_object()
                .bucket(&params.input_data.bucket)
                .key(&part_key)
                .send()
                .await)
        });
    }

    let results = join_all(fetch_futures).await;
    let mut edges = Vec::new();

    for (p, result) in results {
        match result {
            Ok(output) => {
                match output.body.collect().await {
                    Ok(data) => {
                        let bytes = data.to_vec();
                        match std::str::from_utf8(&bytes) {
                            Ok(body_str) => {
                                for line in body_str.lines() {
                                    if line.trim().is_empty() { continue; }
                                    let mut it = line.split('\t');
                                    let src = it.next().and_then(|s| s.parse::<u32>().ok());
                                    let dst = it.next().and_then(|s| s.parse::<u32>().ok());
                                    if let (Some(s), Some(d)) = (src, dst) {
                                        if s < params.num_nodes && d < params.num_nodes {
                                            edges.push((s, d));
                                        }
                                    }
                                }
                            }
                            Err(e) => eprintln!("[Worker {}] UTF8 error in partition {}: {}", worker_id, p, e),
                        }
                    }
                    Err(e) => eprintln!("[Worker {}] Error collecting body for partition {}: {}", worker_id, p, e),
                }
            }
            Err(e) => eprintln!("[Worker {}] S3 error fetching partition {}: {}", worker_id, p, e),
        }
    }

    println!("[Worker {}] Loaded {} edges", worker_id, edges.len());
    EdgePartition { edges }
}

/// Optimized 2-Phase Union-Find
async fn union_find_optimized(
    params: Input,
    middleware: &MiddlewareActorHandle<ParentMessage>,
) -> Output {
    let mut timestamps = vec![timestamp("worker_start")];
    let worker = middleware.info.worker_id;

    // Initialize S3 client (same as before)
    let credentials_provider = Credentials::from_keys(
        params.input_data.aws_access_key_id.clone(),
        params.input_data.aws_secret_access_key.clone(),
        params.input_data.aws_session_token.clone(),
    );

    let config = match params.input_data.endpoint.clone() {
        Some(endpoint) => aws_sdk_s3::config::Builder::new()
            .endpoint_url(endpoint)
            .credentials_provider(credentials_provider)
            .region(Region::new(params.input_data.region.clone()))
            .force_path_style(true)
            .build(),
        None => aws_sdk_s3::config::Builder::new()
            .credentials_provider(credentials_provider)
            .region(Region::new(params.input_data.region.clone()))
            .build(),
    };
    let s3_client = S3Client::from_conf(config);

    // Phase 1: Local UF
    timestamps.push(timestamp("get_input"));
    let partition = load_partition(&params, &s3_client, worker).await;
    timestamps.push(timestamp("get_input_end"));

    timestamps.push(timestamp("local_uf_start"));
    let mut uf = LocalUnionFind::new(); // Sparse
    let mut observed_nodes = HashSet::new();
    
    for &(u, v) in &partition.edges {
        uf.union(u, v);
        observed_nodes.insert(u);
        observed_nodes.insert(v);
    }
    timestamps.push(timestamp("local_uf_end"));

    // Phase 2: Gather Root Equivalences
    // Instead of sending the whole parent array, we only send the mapping
    // for nodes that we actually saw in our edges.
    let mut root_equivalences = Vec::new();
    for node in observed_nodes {
        let root = uf.find(node);
        // We only care if node != root, otherwise it's redundant information
        if node != root {
            root_equivalences.push((node, root));
        }
    }

    timestamps.push(timestamp("reduce_start"));
    // ROOT_WORKER collects all equivalences
    let all_equivalences = middleware
        .reduce(ParentMessage(root_equivalences), |mut left, right| {
            left.0.extend(right.0);
            left
        })
        .unwrap();
    timestamps.push(timestamp("reduce_end"));

    // Phase 3: Global Merge (Root only)
    let global_roots = if worker == ROOT_WORKER {
        timestamps.push(timestamp("global_merge_start"));
        let mut global_uf = LocalUnionFind::new(); // Sparse
        let mut active_nodes = HashSet::new();

        if let Some(msg) = all_equivalences {
            for (u, v) in msg.0 {
                global_uf.union(u, v);
                active_nodes.insert(u);
                active_nodes.insert(v);
            }
        }
        
        // Prepare final mapping to broadcast
        // Optimization: only broadcast roots for nodes that were actually merged
        let mut mapping = Vec::new();
        let mut seen_roots = HashSet::new();
        
        for &node in &active_nodes {
            let r = global_uf.find(node);
            if r != node {
                mapping.push((node, r));
            }
            seen_roots.insert(r);
        }

        let num_comp = (params.num_nodes as usize - active_nodes.len()) + seen_roots.len();
        println!("[Worker {}] Global merge complete. Components: {}", worker, num_comp);
        
        timestamps.push(timestamp("global_merge_end"));
        Some(ParentMessage(mapping))
    } else {
        None
    };

    timestamps.push(timestamp("broadcast_start"));
    let final_mapping = middleware.broadcast(global_roots, ROOT_WORKER).unwrap();
    timestamps.push(timestamp("broadcast_end"));

    // Final result reporting
    let (num_components, results_report) = if worker == ROOT_WORKER {
        // Calculate num_components without O(N)
        let mut active_nodes = HashSet::new();
        let mut final_roots = HashSet::new();
        let mut component_sizes: HashMap<u32, usize> = HashMap::new();

        for (u, r) in &final_mapping.0 {
            active_nodes.insert(*u);
            final_roots.insert(*r);
            // Note: This size only reflects nodes that were part of cross-partition merges
            *component_sizes.entry(*r).or_insert(1) += 1;
        }
        
        // Any node in final_roots that isn't in final_mapping keys is also an active node
        for r in &final_roots {
            active_nodes.insert(*r);
        }

        let num_comp = (params.num_nodes as usize - active_nodes.len()) + final_roots.len();

        let mut report = String::new();
        report.push_str("\n=== Optimized Union-Find Results ===\n");
        report.push_str(&format!("Total nodes: {}\n", params.num_nodes));
        report.push_str(&format!("Connected components: {}\n", num_comp));
        report.push_str("(Note: Size distribution below only accounts for nodes involved in global merges)\n");
        
        let mut sizes: Vec<usize> = component_sizes.values().cloned().collect();
        sizes.sort_by(|a, b| b.cmp(a));
        
        report.push_str("\nLargest merged components:\n");
        for (i, size) in sizes.iter().take(10).enumerate() {
            report.push_str(&format!("  #{}: {} nodes\n", i + 1, size));
        }
        report.push_str("============================\n");

        (Some(num_comp), Some(report))
    } else {
        (None, None)
    };

    timestamps.push(timestamp("worker_end"));

    Output {
        bucket: params.input_data.bucket,
        key: params.input_data.key,
        timestamps,
        num_components,
        results: results_report,
    }
}

pub fn main(args: Value, burst_middleware: Middleware<ParentMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    let handle = burst_middleware.get_actor_handle();
    
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let result = rt.block_on(union_find_optimized(input, &handle));
    serde_json::to_value(result).map_err(|e| e.into())
}

