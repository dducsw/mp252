#!/bin/bash

# Configuration
RAW_TABLE="catalog_iceberg.bus_bronze.buswaypoint_raw"
TEST_TABLE="catalog_iceberg.bus_bronze.buswaypoint_test"
QUERY_FILE="pipelines/test/query.sql"
SPATIAL_QUERY_FILE="pipelines/test/spatial_query.sql"
RESULT_LOG="pipelines/test/benchmark_results.log"

rm -f $RESULT_LOG

echo "===========================================" | tee -a $RESULT_LOG
echo "ICEBERG FULL OPTIMIZATION BENCHMARK" | tee -a $RESULT_LOG
echo "Date: $(date)" | tee -a $RESULT_LOG
echo "===========================================" | tee -a $RESULT_LOG

run_trino_query() {
    local query=$1
    docker exec -t trino trino --execute "$query" --output-format CSV
}

get_stats() {
    local label=$1
    echo "--- Stats: $label ---" | tee -a $RESULT_LOG
    local stats_query="SELECT count(*) as file_count, sum(record_count) as total_records, sum(file_size_in_bytes)/1024 as size_kb FROM \"${TEST_TABLE}\$files\""
    run_trino_query "$stats_query" | tee -a $RESULT_LOG
}

measure_perf() {
    local label=$1
    local file=$2
    echo "--- Measuring Performance ($label) using $file ---" | tee -a $RESULT_LOG
    
    local start_time=$(date +%s%N)
    docker exec -t trino trino --file "/opt/spark/apps/$file" > /dev/null 2>&1
    local end_time=$(date +%s%N)
    
    local duration=$(( (end_time - start_time) / 1000000 ))
    echo "Execution Time: ${duration}ms" | tee -a $RESULT_LOG
}

# [STEP 0] RESET
echo -e "\n[STEP 0] RESETTING TEST ENVIRONMENT..." | tee -a $RESULT_LOG
run_trino_query "DROP TABLE IF EXISTS $TEST_TABLE"
run_trino_query "CREATE TABLE $TEST_TABLE AS SELECT * FROM $RAW_TABLE"

# [STEP 1] INITIAL
echo -e "\n[STEP 1] INITIAL STATE" | tee -a $RESULT_LOG
get_stats "INITIAL"
measure_perf "INITIAL_NORMAL" "$QUERY_FILE"
measure_perf "INITIAL_SPATIAL" "$SPATIAL_QUERY_FILE"

# [STEP 2] TRINO MAINTENANCE
echo -e "\n[STEP 2] RUNNING TRINO OPTIMIZE (Compaction)..." | tee -a $RESULT_LOG
run_trino_query "ALTER TABLE $TEST_TABLE EXECUTE optimize"
run_trino_query "ALTER TABLE $TEST_TABLE EXECUTE expire_snapshots(retention_threshold => '0s')"
get_stats "POST_MAINTENANCE"
measure_perf "POST_MAINT_NORMAL" "$QUERY_FILE"

# [STEP 3] HIDDEN PARTITION
echo -e "\n[STEP 3] EVOLVING TO HIDDEN PARTITION (Spark)..." | tee -a $RESULT_LOG
docker exec -it spark-master spark-submit /opt/spark/apps/pipelines/test/hidden_partition.py "$TEST_TABLE"
measure_perf "POST_HIDDEN_PARTITION" "$QUERY_FILE"

# [STEP 4] SORT ORDER
echo -e "\n[STEP 4] APPLYING HIERARCHICAL SORT ORDER (Spark)..." | tee -a $RESULT_LOG
docker exec -it spark-master spark-submit /opt/spark/apps/pipelines/test/sort_order.py "$TEST_TABLE"
measure_perf "POST_SORT_NORMAL" "$QUERY_FILE"

# [STEP 5] Z-ORDER (SPATIAL)
echo -e "\n[STEP 5] APPLYING SPATIAL Z-ORDER ON (X, Y) (Spark)..." | tee -a $RESULT_LOG
docker exec -it spark-master spark-submit /opt/spark/apps/pipelines/test/z_order.py "$TEST_TABLE"
get_stats "POST_ZORDER"
measure_perf "POST_ZORDER_SPATIAL" "$SPATIAL_QUERY_FILE"

# FINAL SUMMARY
echo -e "\n===========================================" | tee -a $RESULT_LOG
echo "BENCHMARK COMPLETED" | tee -a $RESULT_LOG
echo "==========================================="
