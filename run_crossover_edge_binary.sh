#!/bin/bash
set -euo pipefail

SIZES=(5000000 8000000 10000000 12000000 15000000)
PARTITIONS=4
MEMORY=2048

for N in "${SIZES[@]}"; do
  echo ""
  echo "===== SIZE ${N} ====="

  /home/sergio/src/unionfind/.venv/bin/python setup_large_uf_data.py \
    --nodes "$N" \
    --partitions "$PARTITIONS" \
    --bucket test-bucket \
    --endpoint http://localhost:9000 \
    --edges-per-node 5 \
    --components 10 \
    --format binary \
    --partition-mode edge \
    --output "uf_graph_${N}.tsv"

  kubectl exec pod/dragonfly -- redis-cli FLUSHALL >/dev/null 2>&1 || true

  /home/sergio/src/unionfind/.venv/bin/python benchmark_uf.py \
    --ow-host localhost \
    --ow-port 31001 \
    --uf-endpoint http://minio-service.default.svc.cluster.local:9000 \
    --local-endpoint http://localhost:9000 \
    --partitions "$PARTITIONS" \
    --bucket test-bucket \
    --sizes "$N" \
    --runtime-memory "$MEMORY" \
    --backend redis-list \
    --chunk-size 262144 \
    --graph-file "uf_graph_${N}.tsv" \
    --input-format binary \
    --output "uf_crossover_${N}.json"
done