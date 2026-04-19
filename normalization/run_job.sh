#!/usr/bin/env bash
# run_job.sh — Submit the intermediate data normalization job to the CSDS 312 cluster.
#
# Consumes outputs from Components 1, 2, and 3 and normalizes them into a
# unified per-book JSON schema for Component 5 (Results Aggregation).
#
# Usage:
#   bash run_job.sh <wordfreq_dir> <sentiment_dir> <syntactic_dir> <metadata_file> <hdfs_output_dir>
#
# Example:
#   bash run_job.sh \
#     hdfs:///books/output/wordfreq \
#     hdfs:///books/output/sentiment \
#     hdfs:///books/output/syntactic \
#     hdfs:///books/metadata/books_metadata.csv \
#     hdfs:///books/output/normalized
#
# Environment variables:
#   HADOOP_HOME  — path to Hadoop installation (default: /usr/local/hadoop)
#   NUM_REDUCERS — number of reducer tasks       (default: 4)
#
# Author: Zeynep Bastas

set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
STREAMING_JAR="$(find "$HADOOP_HOME" -name 'hadoop-streaming*.jar' | head -1)"
NUM_REDUCERS="${NUM_REDUCERS:-4}"

WORDFREQ_DIR="${1:?Usage: $0 <wordfreq_dir> <sentiment_dir> <syntactic_dir> <metadata_file> <output_dir>}"
SENTIMENT_DIR="${2:?Usage: $0 <wordfreq_dir> <sentiment_dir> <syntactic_dir> <metadata_file> <output_dir>}"
SYNTACTIC_DIR="${3:?Usage: $0 <wordfreq_dir> <sentiment_dir> <syntactic_dir> <metadata_file> <output_dir>}"
METADATA_FILE="${4:?Usage: $0 <wordfreq_dir> <sentiment_dir> <syntactic_dir> <metadata_file> <output_dir>}"
OUTPUT_DIR="${5:?Usage: $0 <wordfreq_dir> <sentiment_dir> <syntactic_dir> <metadata_file> <output_dir>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "$STREAMING_JAR" ]]; then
    echo "ERROR: Could not find hadoop-streaming JAR under $HADOOP_HOME"
    exit 1
fi

echo "=== Intermediate Data Normalization Job ==="
echo "Word Freq Input:  $WORDFREQ_DIR"
echo "Sentiment Input:  $SENTIMENT_DIR"
echo "Syntactic Input:  $SYNTACTIC_DIR"
echo "Metadata Input:   $METADATA_FILE"
echo "Output:           $OUTPUT_DIR"
echo "Reducers:         $NUM_REDUCERS"
echo ""

hadoop fs -rm -r -f "$OUTPUT_DIR" || true

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="BookScript-Normalization-ZeynepBastas" \
    -D mapreduce.job.reduces="$NUM_REDUCERS" \
    -files "$SCRIPT_DIR/mapper.py,$SCRIPT_DIR/reducer.py" \
    -mapper  "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input   "$WORDFREQ_DIR" \
    -input   "$SENTIMENT_DIR" \
    -input   "$SYNTACTIC_DIR" \
    -input   "$METADATA_FILE" \
    -output  "$OUTPUT_DIR"

echo ""
echo "=== Job complete. Sample output (first 5 records): ==="
hadoop fs -cat "$OUTPUT_DIR/part-*" | head -5
