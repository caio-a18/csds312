#!/usr/bin/env bash
# submit_sentiment_cluster.sh
# ──────────────────────────────────────────────────────────────────────────────
# Submit the sentiment analysis Hadoop Streaming job to the CSDS 312 cluster.
#
# Prerequisites:
#   1. You are logged into the CSDS 312 cluster (ssh <username>@<cluster-host>)
#   2. setup_hdfs.sh has been run (book is in HDFS)
#   3. sentiment_mapper.py, sentiment_reducer.py, and AFINN-111.txt are in the same directory
#
# Usage:
#   bash submit_sentiment_cluster.sh <your-username>
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
USERNAME="${1:-$USER}"
HDFS_INPUT="/user/${USERNAME}/gutenberg"
HDFS_OUTPUT="/user/${USERNAME}/sentiment_output_$(date +%Y%m%d_%H%M%S)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAPPER="${SCRIPT_DIR}/sentiment_mapper.py"
REDUCER="${SCRIPT_DIR}/sentiment_reducer.py"
AFINN="${SCRIPT_DIR}/AFINN-111.txt"

# Path to Hadoop Streaming JAR
STREAMING_JAR=$(find /usr -name "hadoop-streaming*.jar" 2>/dev/null | head -1 || true)
if [[ -z "$STREAMING_JAR" ]]; then
    STREAMING_JAR="/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar"
fi

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [[ ! -f "$MAPPER" ]]; then
    echo "ERROR: Mapper not found: $MAPPER"
    exit 1
fi

if [[ ! -f "$REDUCER" ]]; then
    echo "ERROR: Reducer not found: $REDUCER"
    exit 1
fi

if [[ ! -f "$AFINN" ]]; then
    echo "ERROR: AFINN-111.txt not found: $AFINN"
    exit 1
fi

if [[ ! -f "$STREAMING_JAR" ]]; then
    echo "ERROR: Hadoop Streaming JAR not found."
    echo "       Set STREAMING_JAR manually in this script."
    exit 1
fi

# ── Verify HDFS input exists ──────────────────────────────────────────────────
echo "Checking HDFS input directory: $HDFS_INPUT"
if ! hdfs dfs -test -d "$HDFS_INPUT"; then
    echo "ERROR: HDFS directory $HDFS_INPUT does not exist."
    echo "       Run setup_hdfs.sh first."
    exit 1
fi

# ── Submit job ────────────────────────────────────────────────────────────────
echo ""
echo "=== Submitting Hadoop Streaming Sentiment Analysis Job ==="
echo "User:         $USERNAME"
echo "HDFS Input:   $HDFS_INPUT"
echo "HDFS Output:  $HDFS_OUTPUT"
echo "Streaming JAR: $STREAMING_JAR"
echo ""

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="CSDS312_Sentiment_${USERNAME}" \
    -D mapreduce.job.reduces=1 \
    -files "$MAPPER","$REDUCER","$AFINN" \
    -mapper  "python3 sentiment_mapper.py" \
    -reducer "python3 sentiment_reducer.py" \
    -input   "$HDFS_INPUT" \
    -output  "$HDFS_OUTPUT"

echo ""
echo "=== Job complete! ==="
echo "Output location: $HDFS_OUTPUT"
echo ""

# ── Show results from HDFS ────────────────────────────────────────────────────
echo "Sentiment Analysis Results:"
echo "──────────────────────────"
hdfs dfs -cat "${HDFS_OUTPUT}/part-*" \
    | awk -F'\t' '{printf "  %-25s %s\n", $1, $2}'

echo ""
echo "To view full output:"
echo "  hdfs dfs -cat ${HDFS_OUTPUT}/part-*"
echo ""
echo "To download output locally:"
echo "  hdfs dfs -getmerge ${HDFS_OUTPUT} ./cluster_sentiment_output.txt"