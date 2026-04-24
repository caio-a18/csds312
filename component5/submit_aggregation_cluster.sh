#!/usr/bin/env bash
# submit_aggregation_cluster.sh — Submit Component 5 (results aggregation) on the cluster.
#
# Prerequisites:
#   - Component 4 normalization output exists on HDFS (one line per book: book_id\tjson)
#   - SSH session on CSDS 312 / Markov cluster with hadoop streaming available
#
# Usage:
#   bash submit_aggregation_cluster.sh <hdfs_normalized_input_dir> [hdfs_output_dir]
#
# Example:
#   bash submit_aggregation_cluster.sh /user/mxf477/books/output/normalized /user/mxf477/books/output/aggregated
#
# Environment:
#   HADOOP_HOME  — optional, used by some clusters
#   NUM_REDUCERS — default 1 (required for single global summary JSON)

set -euo pipefail

NUM_REDUCERS="${NUM_REDUCERS:-1}"
INPUT_DIR="${1:?Usage: $0 <hdfs_normalized_input_dir> [hdfs_output_dir]}"

if [[ -n "${2:-}" ]]; then
  OUTPUT_DIR="$2"
else
  OUTPUT_DIR="/user/${USER}/aggregation_output_$(date +%Y%m%d_%H%M%S)"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAPPER="${SCRIPT_DIR}/aggregator_mapper.py"
REDUCER="${SCRIPT_DIR}/aggregator_reducer.py"

STREAMING_JAR=$(find /usr -name "hadoop-streaming*.jar" 2>/dev/null | head -1 || true)
if [[ -z "$STREAMING_JAR" ]]; then
  STREAMING_JAR="$(find "${HADOOP_HOME:-/usr/local/hadoop}" -name 'hadoop-streaming*.jar' 2>/dev/null | head -1 || true)"
fi
if [[ -z "$STREAMING_JAR" ]] || [[ ! -f "$STREAMING_JAR" ]]; then
  STREAMING_JAR="/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar"
fi

if [[ ! -f "$MAPPER" ]] || [[ ! -f "$REDUCER" ]]; then
  echo "ERROR: Mapper or reducer not found in $SCRIPT_DIR"
  exit 1
fi
if [[ ! -f "$STREAMING_JAR" ]]; then
  echo "ERROR: Hadoop streaming JAR not found. Set STREAMING_JAR or install Hadoop client."
  exit 1
fi

echo "=== Component 5: Results Aggregation ==="
echo "Input:   $INPUT_DIR"
echo "Output:  $OUTPUT_DIR"
echo "Reducers: $NUM_REDUCERS"
echo ""

hdfs dfs -test -d "$INPUT_DIR" || { echo "ERROR: HDFS input dir missing: $INPUT_DIR"; exit 1; }

hdfs dfs -rm -r -f "$OUTPUT_DIR" || true

hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="CSDS312_Aggregation_${USER}" \
  -D mapreduce.job.reduces="$NUM_REDUCERS" \
  -files "$MAPPER,$REDUCER" \
  -mapper  "python3 aggregator_mapper.py" \
  -reducer "python3 aggregator_reducer.py" \
  -input   "$INPUT_DIR" \
  -output  "$OUTPUT_DIR"

echo ""
echo "=== Job complete ==="
echo "Output HDFS path: $OUTPUT_DIR"
echo ""
echo "View result:"
echo "  hdfs dfs -cat '$OUTPUT_DIR/part-*'"
echo ""
echo "Download merged:"
echo "  hdfs dfs -getmerge '$OUTPUT_DIR' ./aggregation_summary.json"
