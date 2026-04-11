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

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

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

/// Sparse find with path compression (HashMap-based UF)
fn sparse_find(parent: &mut HashMap<u32, u32>, node: u32) -> u32 {
    let p = *parent.get(&node).unwrap_or(&node);
    if p == node {
        parent.entry(node).or_insert(node);
        return node;
    }
    let root = sparse_find(parent, p);
    parent.insert(node, root);
    root
}

/// Sparse union by rank (HashMap-based UF)
fn sparse_union(parent: &mut HashMap<u32, u32>, rank: &mut HashMap<u32, u8>, a: u32, b: u32) {
    parent.entry(a).or_insert(a);
    parent.entry(b).or_insert(b);

    let ra = sparse_find(parent, a);
    let rb = sparse_find(parent, b);
    if ra == rb {
        return;
    }

    let rank_a = *rank.get(&ra).unwrap_or(&0);
    let rank_b = *rank.get(&rb).unwrap_or(&0);

    if rank_a < rank_b {
        parent.insert(ra, rb);
    } else if rank_a > rank_b {
        parent.insert(rb, ra);
    } else {
        parent.insert(rb, ra);
        rank.insert(ra, rank_a + 1);
    }
}

/// Input parameters for the Union-Find action
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    input_data: S3InputParams,
    num_nodes: u32,
    partitions: u32,
    granularity: u32,
    #[serde(default)]
    group_id: Option<u32>,
    timeout_seconds: Option<u64>,
}

/// S3 configuration for fetching graph data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S3InputParams {
    bucket: String,
    key: String,
    #[serde(default)]
    format: Option<String>,
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
    component_hash: Option<String>,
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

fn canonical_component_hash(parent: &[u32]) -> String {
    let mut root_to_component: HashMap<u32, u32> = HashMap::new();
    let mut next_component = 0u32;
    let mut hash: u64 = 0xcbf29ce484222325;

    for &root in parent {
        let component = *root_to_component.entry(root).or_insert_with(|| {
            let id = next_component;
            next_component += 1;
            id
        });
        hash ^= component as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }

    format!("{hash:016x}")
}

fn looks_like_tsv(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .any(|&b| b == b'\t' || b == b'\n' || b == b'\r')
}

fn parse_binary_edges_into_uf(
    bytes: &[u8],
    num_nodes: u32,
    parent: &mut [u32],
    rank: &mut [u8],
    seen: &mut [bool],
) -> usize {
    let mut parsed = 0usize;
    for chunk in bytes.chunks_exact(8) {
        let src = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
        let dst = u32::from_le_bytes(chunk[4..8].try_into().unwrap());
        if src < num_nodes && dst < num_nodes {
            seen[src as usize] = true;
            seen[dst as usize] = true;
            flat_union(parent, rank, src, dst);
            parsed += 1;
        }
    }
    parsed
}

fn parse_tsv_edges_into_uf(
    bytes: &[u8],
    num_nodes: u32,
    parent: &mut [u32],
    rank: &mut [u8],
    seen: &mut [bool],
) -> usize {
    let mut parsed = 0usize;
    if let Ok(body_str) = std::str::from_utf8(bytes) {
        for line in body_str.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Some((src, dst)) = line.split_once('\t') {
                if let (Ok(s), Ok(d)) = (src.parse::<u32>(), dst.parse::<u32>()) {
                    if s < num_nodes && d < num_nodes {
                        seen[s as usize] = true;
                        seen[d as usize] = true;
                        flat_union(parent, rank, s, d);
                        parsed += 1;
                    }
                }
            }
        }
    }
    parsed
}

/// Download all assigned partitions from S3 and return raw byte buffers.
/// This separates the S3 load phase from the UF computation phase so each
/// can be timed independently.
async fn download_partitions(
    params: &Input,
    s3_client: &S3Client,
    worker_id: u32,
) -> Result<Vec<Vec<u8>>, String> {
    let logical_worker = logical_worker_id(worker_id, params.granularity, params.group_id);
    let parts = partition_range(logical_worker, params.granularity);

    let mut fetch_futures = Vec::new();
    for p in parts.clone() {
        let part_key = format!("{}/part-{:05}", params.input_data.key, p);
        fetch_futures.push(async move {
            (
                p,
                part_key.clone(),
                s3_client
                    .get_object()
                    .bucket(&params.input_data.bucket)
                    .key(&part_key)
                    .send()
                    .await,
            )
        });
    }

    let results = join_all(fetch_futures).await;
    let mut buffers: Vec<Vec<u8>> = Vec::new();

    for (p, part_key, result) in results {
        match result {
            Ok(output) => match output.body.collect().await {
                Ok(data) => buffers.push(data.to_vec()),
                Err(e) => {
                    return Err(format!(
                        "[Worker {}|logical {}] Error collecting partition {} ({}): {}",
                        worker_id, logical_worker, p, part_key, e
                    ));
                }
            },
            Err(e) => {
                return Err(format!(
                    "[Worker {}|logical {}] Missing or unreadable partition {} ({}): {}",
                    worker_id, logical_worker, p, part_key, e
                ));
            }
        }
    }
    Ok(buffers)
}

/// Process pre-downloaded byte buffers into Union-Find state.
fn process_buffers_into_uf(
    buffers: &[Vec<u8>],
    preferred_format: Option<&str>,
    num_nodes: u32,
    parent: &mut [u32],
    rank: &mut [u8],
    seen: &mut [bool],
) -> usize {
    let mut parsed_edges = 0usize;
    for bytes in buffers {
        let use_binary = match preferred_format {
            Some("binary") => true,
            Some("tsv") | Some("text") => false,
            _ => bytes.len() % 8 == 0 && !looks_like_tsv(bytes),
        };
        let parsed_in_partition = if use_binary {
            parse_binary_edges_into_uf(bytes, num_nodes, parent, rank, seen)
        } else {
            parse_tsv_edges_into_uf(bytes, num_nodes, parent, rank, seen)
        };
        parsed_edges += parsed_in_partition;
    }
    parsed_edges
}

fn logical_worker_id(worker_id: u32, granularity: u32, group_id: Option<u32>) -> u32 {
    group_id.unwrap_or_else(|| worker_id / granularity.max(1))
}

fn partition_range(logical_worker: u32, granularity: u32) -> std::ops::Range<u32> {
    let start = logical_worker * granularity;
    start..(start + granularity)
}

/// Optimized 2-Phase Union-Find (sync - middleware calls must not be in tokio context)
fn union_find_optimized(
    params: Input,
    middleware: &MiddlewareActorHandle<ParentMessage>,
) -> Output {
    let mut timestamps = vec![timestamp("worker_start")];
    let worker = middleware.info.worker_id;
    let logical_worker = logical_worker_id(worker, params.granularity, params.group_id);

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

    // Phase 1a: Download S3 partitions into memory buffers (load phase).
    timestamps.push(timestamp("get_input"));
    let buffers = rt
        .block_on(download_partitions(&params, &s3_client, worker))
        .unwrap_or_else(|err| panic!("{err}"));
    timestamps.push(timestamp("get_input_end"));

    // Phase 1b: Run local Union-Find on the buffered data (compute phase).
    timestamps.push(timestamp("local_uf_start"));
    let n = params.num_nodes as usize;
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let mut rank: Vec<u8> = vec![0; n];
    let mut seen = vec![false; n];

    let processed_edges = process_buffers_into_uf(
        &buffers,
        params.input_data.format.as_deref(),
        params.num_nodes,
        &mut parent,
        &mut rank,
        &mut seen,
    );
    timestamps.push(timestamp("local_uf_end"));

    let seen_count = seen.iter().filter(|&&v| v).count();
    println!(
        "[Worker {}] Local UF done: {} processed edges, {} seen nodes",
        logical_worker, processed_edges, seen_count
    );

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
        logical_worker,
        node_root_pairs.len()
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

    // Phase 3: Global Merge with sparse UF (Root worker only)
    //
    // For each (node, root) pair received, flat_union(node, root) connects them.
    // Nodes that appear in multiple workers with different roots get merged
    // transitively through the shared node. O(N·α(N)) ≈ O(N) total.

    let root_summary = if worker == ROOT_WORKER {
        timestamps.push(timestamp("global_merge_start"));

        let gn = params.num_nodes as usize;
        let mut gparent: HashMap<u32, u32> = HashMap::new();
        let mut grank: HashMap<u32, u8> = HashMap::new();
        let mut gseen: HashSet<u32> = HashSet::new();

        if let Some(ref msg) = all_equivalences {
            for &(node, root) in &msg.0 {
                if node < params.num_nodes && root < params.num_nodes {
                    gseen.insert(node);
                    gseen.insert(root);
                    sparse_union(&mut gparent, &mut grank, node, root);
                }
            }
        }

        // Count components in a single pass
        let mut global_roots_set: HashSet<u32> = HashSet::new();
        for &node in &gseen {
            global_roots_set.insert(sparse_find(&mut gparent, node));
        }
        let num_unseen = gn.saturating_sub(gseen.len());
        let nc = global_roots_set.len() + num_unseen;

        println!(
            "[Worker {}] Global merge: {} seen nodes, {} global roots, {} isolated → {} components",
            worker,
            gseen.len(),
            global_roots_set.len(),
            num_unseen,
            nc
        );

        timestamps.push(timestamp("global_merge_end"));
        let mut full_parent = vec![0u32; gn];
        for (node, slot) in full_parent.iter_mut().enumerate() {
            let node_u32 = node as u32;
            *slot = if gseen.contains(&node_u32) {
                sparse_find(&mut gparent, node_u32)
            } else {
                node_u32
            };
        }
        let component_hash = canonical_component_hash(&full_parent);
        Some((nc, component_hash))
    } else {
        None
    };

    // Broadcast result from ROOT_WORKER to all workers.
    // Encoded as ParentMessage: [(u32::MAX, num_components), (hash_high, hash_low)]
    // u32::MAX is a sentinel that distinguishes this from reduce data (node ids < u32::MAX).
    timestamps.push(timestamp("broadcast_start"));
    let broadcast_payload = if worker == ROOT_WORKER {
        let (nc, ref hash_str) = root_summary.clone().unwrap();
        let hash_val = u64::from_str_radix(hash_str, 16).unwrap_or(0);
        let hash_high = (hash_val >> 32) as u32;
        let hash_low = hash_val as u32;
        Some(ParentMessage(vec![
            (u32::MAX, nc as u32),
            (hash_high, hash_low),
        ]))
    } else {
        None
    };
    let received = middleware
        .broadcast(broadcast_payload, ROOT_WORKER)
        .unwrap();
    timestamps.push(timestamp("broadcast_end"));

    let (num_components, component_hash, results_report) = {
        let pairs = &received.0;
        let nc = pairs[0].1 as usize;
        let hash_val = ((pairs[1].0 as u64) << 32) | (pairs[1].1 as u64);
        let hash_str = format!("{:016x}", hash_val);
        let report = if worker == ROOT_WORKER {
            let mut r = String::new();
            r.push_str("\n=== Optimized Union-Find Results ===\n");
            r.push_str(&format!("Total nodes: {}\n", params.num_nodes));
            r.push_str(&format!("Connected components: {}\n", nc));
            r.push_str(&format!("Component hash: {}\n", hash_str));
            r.push_str("============================\n");
            Some(r)
        } else {
            None
        };
        (Some(nc), Some(hash_str), report)
    };

    timestamps.push(timestamp("worker_end"));

    Output {
        bucket: params.input_data.bucket,
        key: params.input_data.key,
        timestamps,
        num_components,
        component_hash,
        results: results_report,
    }
}

pub fn main(args: Value, burst_middleware: Middleware<ParentMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    let handle = burst_middleware.get_actor_handle();
    let result = union_find_optimized(input, &handle);
    serde_json::to_value(result).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_range_from_group() {
        assert_eq!(logical_worker_id(3, 2, Some(1)), 1);
        assert_eq!(partition_range(1, 2), 2..4);
    }

    #[test]
    fn test_component_hash_is_stable() {
        let parent = vec![0, 0, 2, 2, 4];
        assert_eq!(
            canonical_component_hash(&parent),
            canonical_component_hash(&parent)
        );
    }
}
