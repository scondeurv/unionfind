#!/bin/bash

# Configuration
NODES=5000000
PARTITIONS=4
MEMORY=4096

echo "Running Union-Find benchmark with $NODES nodes, flat-array optimization..."

# Ensure zip is compiled (updated)
# The user said "el zip está listo", so we assume it is. 
# But to be safe and follow "script similar to run_labelpropagation" pattern which calls python script:

python3 benchmark_uf.py \
  --ow-host localhost \
  --ow-port 31001 \
  --uf-endpoint http://minio-service.default.svc.cluster.local:9000 \
  --local-endpoint http://localhost:9000 \
  --partitions $PARTITIONS \
  --sizes $NODES \
  --backend redis-list \
  --runtime-memory $MEMORY \
  --output uf_benchmark_5M_flat.json

