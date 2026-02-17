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

// ─── Flat-array Union-Find (cache-friendly, O(α(n)) per op) ───────────────

/// Find root with path halving (flat array)
fn flat_find(parent: &mut [u32], mut i: u32) -> u32 {
    while parent[i as usize] != i {
        parent[i as usize] = parent[parent[i as usize] as usize];
        i = parent[i as usize];
    }
    i
}

/// Union by rank (flat array)
/// NOTE: `u8` rank is sufficient — rank is bounded by O(log n), so u8 supports up to 2^255 nodes.
fn flat_union(parent: &mut [u32], rank: &mut [u8], a: u32, b: u32) {
    let ra = flat_find(parent, a);
    let rb = flat_find(parent, b);
    if ra != rb {
        if rank[ra as usize] < rank[rb as usize] {
            parent[ra as usize] = rb;
        } else if rank[ra as usize] > rank[rb as usize] {
            parent[rb as usize] = ra;
        } else {
            parent[rb as usize] = ra;
            rank[ra as usize] += 1;
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

/// Optimized 2-Phase Union-Find (sync - middleware calls must not be in tokio context)
fn union_find_optimized(
    params: Input,
    middleware: &MiddlewareActorHandle<ParentMessage>,
) -> Output {
    let mut timestamps = vec![timestamp("worker_start")];
    let worker = middleware.info.worker_id;

    // Create tokio runtime for async S3 operations only
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Initialize S3 client
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

    // Phase 1: Local UF — use rt.block_on() ONLY for async S3 loading
    timestamps.push(timestamp("get_input"));
    let partition = rt.block_on(load_partition(&params, &s3_client, worker));
    timestamps.push(timestamp("get_input_end"));

    // Phase 1: Local UF with flat arrays (cache-friendly, no hashing)
    timestamps.push(timestamp("local_uf_start"));
    let n = params.num_nodes as usize;
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let mut rank: Vec<u8> = vec![0; n];
    let mut seen = vec![false; n];

    for &(u, v) in &partition.edges {
        seen[u as usize] = true;
        seen[v as usize] = true;
        flat_union(&mut parent, &mut rank, u, v);
    }
    timestamps.push(timestamp("local_uf_end"));

    // Phase 2: Collect (node, root) only for non-root seen nodes.
    // Self-pairs (root, root) are omitted — the global UF already initialises
    // parent[i] = i, so they carry no new information. This reduces the
    // volume of data transferred during the reduce step.
    let mut node_root_pairs: Vec<(u32, u32)> = Vec::new();
    for i in 0..n {
        if seen[i] {
            let root = flat_find(&mut parent, i as u32);
            if root != i as u32 {
                node_root_pairs.push((i as u32, root));
            }
        }
    }

    println!(
        "[Worker {}] Sending {} node-root pairs for global merge",
        worker, node_root_pairs.len()
    );

    timestamps.push(timestamp("reduce_start"));
    // ROOT_WORKER collects all equivalences
    let all_equivalences = middleware
        .reduce(ParentMessage(node_root_pairs), |mut left, right| {
            left.0.extend(right.0);
            left
        })
        .unwrap();
    timestamps.push(timestamp("reduce_end"));

    // Phase 3: Global Merge with flat-array UF (Root worker only)
    //
    // For each (node, root) pair received, flat_union(node, root) connects them.
    // Nodes that appear in multiple workers with different roots get merged
    // transitively through the shared node. O(N·α(N)) ≈ O(N) total.
    
    let global_roots = if worker == ROOT_WORKER {
        timestamps.push(timestamp("global_merge_start"));
        
        let gn = params.num_nodes as usize;
        let mut gparent: Vec<u32> = (0..gn as u32).collect();
        let mut grank: Vec<u8> = vec![0; gn];
        let mut gseen = vec![false; gn];
        
        if let Some(ref msg) = all_equivalences {
            for &(node, root) in &msg.0 {
                gseen[node as usize] = true;
                gseen[root as usize] = true;
                flat_union(&mut gparent, &mut grank, node, root);
            }
        }
        
        // Count components in a single pass
        let mut global_roots_set: HashSet<u32> = HashSet::new();
        let mut num_unseen: usize = 0;
        for i in 0..gn {
            if gseen[i] {
                global_roots_set.insert(flat_find(&mut gparent, i as u32));
            } else {
                num_unseen += 1;
            }
        }
        let num_comp = global_roots_set.len() + num_unseen;
        
        println!(
            "[Worker {}] Global merge: {} seen nodes, {} global roots, {} isolated → {} components",
            worker, gn - num_unseen, global_roots_set.len(), num_unseen, num_comp
        );
        
        timestamps.push(timestamp("global_merge_end"));
        // Broadcast a minimal message (non-root workers don't use it)
        Some((ParentMessage(vec![]), num_comp))
    } else {
        None
    };
    
    // Extract num_components before broadcast (ROOT_WORKER calculated it)
    let num_components_computed = global_roots.as_ref().map(|(_, nc)| *nc);

    timestamps.push(timestamp("broadcast_start"));
    let _final_remap = middleware.broadcast(
        global_roots.map(|(msg, _)| msg), 
        ROOT_WORKER
    ).unwrap();
    timestamps.push(timestamp("broadcast_end"));

    // Final result reporting (simplified - ROOT_WORKER already computed everything)
    let (num_components, results_report) = if worker == ROOT_WORKER {
        let num_comp = num_components_computed.unwrap_or(0);
        
        let mut report = String::new();
        report.push_str("\n=== Optimized Union-Find Results ===\n");
        report.push_str(&format!("Total nodes: {}\n", params.num_nodes));
        report.push_str(&format!("Connected components: {}\n", num_comp));
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
    let result = union_find_optimized(input, &handle);
    serde_json::to_value(result).map_err(|e| e.into())
}

