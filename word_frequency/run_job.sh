#!/usr/bin/env bash
# run_job.sh — Submit the word-frequency MapReduce job to the CSDS 312 cluster.
#
# Usage:
#   bash run_job.sh <hdfs_input_dir> <hdfs_output_dir>
#
# Example:
#   bash run_job.sh hdfs:///books/input hdfs:///books/output/word_freq
#
# Environment variables (override defaults as needed):
#   HADOOP_HOME  — path to Hadoop installation (default: /usr/local/hadoop)
#   NUM_REDUCERS — number of reducer tasks       (default: 4)
#
# Author: Caio Albuquerque (Group 9, CSDS 312)

set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
STREAMING_JAR="$(find "$HADOOP_HOME" -name 'hadoop-streaming*.jar' | head -1)"
NUM_REDUCERS="${NUM_REDUCERS:-4}"

INPUT_DIR="${1:?Usage: $0 <hdfs_input_dir> <hdfs_output_dir>}"
OUTPUT_DIR="${2:?Usage: $0 <hdfs_input_dir> <hdfs_output_dir>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "$STREAMING_JAR" ]]; then
    echo "ERROR: Could not find hadoop-streaming JAR under $HADOOP_HOME"
    exit 1
fi

echo "=== Word Frequency MapReduce Job ==="
echo "Input:       $INPUT_DIR"
echo "Output:      $OUTPUT_DIR"
echo "Reducers:    $NUM_REDUCERS"
echo "Streaming:   $STREAMING_JAR"
echo ""

# Remove previous output if it exists (Hadoop will fail otherwise)
hadoop fs -rm -r -f "$OUTPUT_DIR" || true

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="BookScript-WordFreq-CaioAlbuquerque" \
    -D mapreduce.job.reduces="$NUM_REDUCERS" \
    -files  "$SCRIPT_DIR/mapper.py,$SCRIPT_DIR/reducer.py,$SCRIPT_DIR/preprocess.py,$SCRIPT_DIR/stopwords.txt" \
    -mapper  "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input   "$INPUT_DIR" \
    -output  "$OUTPUT_DIR"

echo ""
echo "=== Job complete. Top 20 words: ==="
hadoop fs -cat "$OUTPUT_DIR/part-*" \
    | sort -t$'\t' -k2 -rn \
    | head -20
