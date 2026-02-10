# Union-Find (Connected Components)

This directory contains implementations of the Union-Find algorithm for finding connected components in a graph:

1. **Standalone (ufst/)**: Single-node Rust implementation using path compression + union by rank
2. **Burst (ow-uf/)**: Distributed Rust implementation using Shiloach-Vishkin algorithm on OpenWhisk

## Algorithm Overview

Union-Find identifies connected components in an undirected graph. Two nodes are in the same component if there's a path between them.

### Standalone Version
- Classic Union-Find with path compression and union by rank
- O(α(n)) amortized time per operation (nearly constant)

### Distributed Version (Shiloach-Vishkin)
- Iterative pointer-jumping algorithm
- Each node maintains a parent pointer
- Converges in O(log n) iterations
- Workers synchronize parent arrays via reduce + broadcast

## Usage

### Standalone
```bash
cd ufst
cargo build --release
./target/release/union-find <graph_file.tsv> <num_nodes>
```

### Burst (OpenWhisk)
```bash
# Compile for cluster
./compile_uf_cluster.sh

# Run
python unionfind.py \
  --ow-host localhost \
  --ow-port 31001 \
  --uf-endpoint http://minio-service.default:9000 \
  --partitions 8 \
  --num-nodes 10000 \
  --bucket test-bucket \
  --key graphs/test \
  --backend redis-list
```

### Validate Results
```bash
python validate_results.py \
  --standalone ufst_output.json \
  --burst unionfind-burst.json \
  --num-nodes 10000
```

## Input Format

TSV file with edges (same format as Label Propagation):
```
src_node\tdst_node
0	1
1	2
2	3
```

## Output

JSON with parent array and component count:
```json
{
  "parent": [0, 0, 0, 3, 3],
  "num_components": 2,
  "execution_time_ms": 123
}
```
