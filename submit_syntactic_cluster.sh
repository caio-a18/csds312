#!/usr/bin/env bash
# submit_syntactic_cluster.sh
# Submits Component 3 (Syntactic Complexity) to the Markov HPC cluster
# via Hadoop Streaming.
#
# Usage: bash submit_syntactic_cluster.sh <username>
# Example: bash submit_syntactic_cluster.sh hmr63

set -euo pipefail

USERNAME="${1:?Usage: $0 <username>}"
ACCOUNT="csds312"
HADOOP_STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar"
HDFS_BASE="/user/${USERNAME}"
INPUT_DIR="${HDFS_BASE}/books"
OUTPUT_DIR="${HDFS_BASE}/syntactic_output"
STORAGE_DIR="/mnt/vstor/courses/${ACCOUNT}/${USERNAME}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAPPER="${SCRIPT_DIR}/mapper.py"
REDUCER="${SCRIPT_DIR}/reducer.py"

echo "=== Component 3: Syntactic Complexity Cluster Submission ==="
echo "User     : $USERNAME"
echo "Input    : $INPUT_DIR"
echo "Output   : $OUTPUT_DIR"
echo ""

# Remove stale output directory if it exists
hdfs dfs -rm -r -f "$OUTPUT_DIR" && echo "Removed existing output dir." || true

# Submit Hadoop Streaming job
hadoop jar $HADOOP_STREAMING_JAR \
  -D mapreduce.job.name="Component3_SyntacticComplexity_${USERNAME}" \
  -D mapreduce.map.memory.mb=2048 \
  -D mapreduce.reduce.memory.mb=2048 \
  -D mapreduce.job.reduces=4 \
  -D stream.non.zero.exit.is.failure=true \
  -files "${MAPPER},${REDUCER}" \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input  "${INPUT_DIR}/*.txt" \
  -output "${OUTPUT_DIR}"

echo ""
echo "=== Job complete. Fetching results ==="
mkdir -p "${STORAGE_DIR}/syntactic_output"
hdfs dfs -getmerge "${OUTPUT_DIR}" "${STORAGE_DIR}/syntactic_output/syntactic_output.tsv"
echo "Results saved to: ${STORAGE_DIR}/syntactic_output/syntactic_output.tsv"

echo ""
echo "=== Preview (first 30 lines) ==="
head -30 "${STORAGE_DIR}/syntactic_output/syntactic_output.tsv"
