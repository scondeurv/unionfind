#!/bin/bash
# run_all_benchmarks.sh - Run Union-Find benchmarks across multiple graph sizes
#
# Usage: ./run_all_benchmarks.sh
#
# Generates graphs, runs both standalone and burst, extracts timing to CSV.

NODES_LIST=(1000000 2000000 5000000 8000000 10000000 12500000 15000000)
PARTITIONS=4
MEMORY=2048
EDGES_PER_NODE=5
COMPONENTS=10
S3_ENDPOINT="http://minio-service.default.svc.cluster.local:9000"
LOCAL_ENDPOINT="http://localhost:9000"
BUCKET="test-bucket"

LOG_FILE="uf_benchmark_all.log"
CSV_FILE="uf_benchmark_all.csv"

echo "Nodes,Standalone_ms,Burst_ms,Speedup" > $CSV_FILE
echo "Starting Union-Find benchmarks..." > $LOG_FILE
echo "Config: partitions=$PARTITIONS, memory=${MEMORY}MB, edges/node=$EDGES_PER_NODE" | tee -a $LOG_FILE

for NODES in "${NODES_LIST[@]}"
do
    echo "------------------------------------------" | tee -a $LOG_FILE
    echo "Benchmarking $NODES nodes..." | tee -a $LOG_FILE

    # Generate graph (local + S3)
    python3 setup_large_uf_data.py \
        --nodes $NODES \
        --partitions $PARTITIONS \
        --bucket $BUCKET \
        --endpoint $LOCAL_ENDPOINT \
        --edges-per-node $EDGES_PER_NODE \
        --components $COMPONENTS 2>&1 | tee -a $LOG_FILE

    GRAPH_FILE="uf_graph_${NODES}.tsv"

    # Flush Redis/Dragonfly for clean state
    kubectl exec pod/dragonfly -- redis-cli FLUSHALL > /dev/null 2>&1

    # Run benchmark
    OUTPUT=$(python3 benchmark_uf.py \
        --ow-host localhost \
        --ow-port 31001 \
        --uf-endpoint $S3_ENDPOINT \
        --local-endpoint $LOCAL_ENDPOINT \
        --partitions $PARTITIONS \
        --bucket $BUCKET \
        --sizes $NODES \
        --runtime-memory $MEMORY \
        --backend redis-list \
        --chunk-size 1024 \
        --graph-file $GRAPH_FILE \
        --output uf_benchmark_${NODES}.json 2>&1)

    echo "$OUTPUT" >> $LOG_FILE

    # Extract canonical timing lines
    ST_TIME=$(echo "$OUTPUT" | grep "Standalone Processing Time (Execution):" | awk -F: '{print $2}' | awk '{print $1}')
    BT_TIME=$(echo "$OUTPUT" | grep "Burst Processing Time (Distributed Span):" | awk -F: '{print $2}' | awk '{print $1}')

    if [ -z "$ST_TIME" ]; then ST_TIME="0"; fi
    if [ -z "$BT_TIME" ]; then BT_TIME="0"; fi

    # Calculate speedup
    if [ "$BT_TIME" != "0" ] && [ "$ST_TIME" != "0" ]; then
        SPEEDUP=$(python3 -c "print(f'{$ST_TIME / $BT_TIME:.2f}')")
    else
        SPEEDUP="0"
    fi

    echo "$NODES,$ST_TIME,$BT_TIME,$SPEEDUP" >> $CSV_FILE

    echo "Done: Standalone=${ST_TIME}ms, Burst=${BT_TIME}ms, Speedup=${SPEEDUP}x" | tee -a $LOG_FILE

    # Clean up large graph file to save disk space
    rm -f $GRAPH_FILE

    # Brief pause
    sleep 3
done

echo "==========================================" | tee -a $LOG_FILE
echo "Benchmarks completed. Results:" | tee -a $LOG_FILE
echo "  CSV:  $CSV_FILE" | tee -a $LOG_FILE
echo "  Log:  $LOG_FILE" | tee -a $LOG_FILE
cat $CSV_FILE | tee -a $LOG_FILE
