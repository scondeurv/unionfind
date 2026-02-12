#!/bin/bash
# Script to run Union-Find burst with optional validation
# Usage: ./run_unionfind.sh [--skip-validation] [--nodes N]

SKIP_VALIDATION=false
NUM_NODES=10000
PARTITIONS=4
BUCKET="test-bucket"
KEY="uf-graphs/uf-10000"

# Parse arguments
for arg in "$@"
do
    case $arg in
        --skip-validation)
        SKIP_VALIDATION=true
        shift
        ;;
        --nodes)
        NUM_NODES=$2
        KEY="uf-graphs/uf-${NUM_NODES}"
        shift 2
        ;;
        --partitions)
        PARTITIONS=$2
        shift 2
        ;;
    esac
done

echo "=========================================="
echo "Running Union-Find (burst)"
echo "  Nodes:      $NUM_NODES"
echo "  Partitions: $PARTITIONS"
echo "  S3 key:     $KEY"
echo "=========================================="

# Run the Union-Find burst
PYTHONPATH=. python3 unionfind.py \
  --ow-host localhost \
  --ow-port 31001 \
  --uf-endpoint http://minio-service.default.svc.cluster.local:9000 \
  --partitions $PARTITIONS \
  --num-nodes $NUM_NODES \
  --bucket $BUCKET \
  --key $KEY \
  --granularity 1 \
  --backend redis-list \
  --chunk-size 1024 \
  --runtime-memory 2048

UF_EXIT_CODE=$?

# Run validation if not skipped and UF succeeded
if [ "$SKIP_VALIDATION" = false ] && [ $UF_EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "Running correctness validation..."
    echo "=========================================="

    python3 validate_results.py \
        --standalone ufst_output.json \
        --graph uf_graph_${NUM_NODES}.tsv \
        --bucket $BUCKET \
        --key $KEY \
        --endpoint http://minio-service.default.svc.cluster.local:9000 \
        --num-nodes $NUM_NODES

    VALIDATION_EXIT_CODE=$?

    if [ $VALIDATION_EXIT_CODE -ne 0 ]; then
        echo "ERROR: Validation failed!"
        exit $VALIDATION_EXIT_CODE
    fi

    echo "Validation passed successfully"
fi

exit $UF_EXIT_CODE
