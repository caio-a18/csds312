#!/usr/bin/env bash
# submit_wordfreq_cluster.sh
# ──────────────────────────────────────────────────────────────────────────────
# Submit the word frequency Hadoop Streaming job to the CSDS 312 cluster.
#
# Prerequisites:
#   1. You are logged into the CSDS 312 cluster (ssh <username>@<cluster-host>)
#   2. setup_hdfs.sh has been run (book is in HDFS)
#   3. wordfreq_mapper.py and wordfreq_reducer.py are in the same directory
#
# Usage:
#   bash submit_wordfreq_cluster.sh <your-username>
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Configuration — edit these for your cluster ────────────────────────────────
USERNAME="${1:-$USER}"                          # pass your CWRUid as $1 or rely on $USER
HDFS_INPUT="/user/${USERNAME}/gutenberg"
HDFS_OUTPUT="/user/${USERNAME}/wordfreq_output_$(date +%Y%m%d_%H%M%S)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAPPER="${SCRIPT_DIR}/wordfreq_mapper.py"
REDUCER="${SCRIPT_DIR}/wordfreq_reducer.py"

# Path to Hadoop Streaming JAR — adjust if your cluster has a different version
# Common locations on CDH / HDP clusters:
STREAMING_JAR=$(find /usr -name "hadoop-streaming*.jar" 2>/dev/null | head -1 || true)
if [[ -z "$STREAMING_JAR" ]]; then
    # Fallback: common HDP path
    STREAMING_JAR="/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar"
fi

# ── Sanity checks ──────────────────────────────────────────────────────────────
if [[ ! -f "$MAPPER" ]]; then
    echo "ERROR: Mapper not found: $MAPPER"
    exit 1
fi

if [[ ! -f "$REDUCER" ]]; then
    echo "ERROR: Reducer not found: $REDUCER"
    exit 1
fi

if [[ ! -f "$STREAMING_JAR" ]]; then
    echo "ERROR: Hadoop Streaming JAR not found."
    echo "       Set STREAMING_JAR manually in this script."
    exit 1
fi

# ── Verify HDFS input exists ───────────────────────────────────────────────────
echo "Checking HDFS input directory: $HDFS_INPUT"
if ! hdfs dfs -test -d "$HDFS_INPUT"; then
    echo "ERROR: HDFS directory $HDFS_INPUT does not exist."
    echo "       Run setup_hdfs.sh first."
    exit 1
fi

# ── Submit job ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Submitting Hadoop Streaming Word Frequency Job ==="
echo "User:         $USERNAME"
echo "HDFS Input:   $HDFS_INPUT"
echo "HDFS Output:  $HDFS_OUTPUT"
echo "Streaming JAR: $STREAMING_JAR"
echo ""

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="CSDS312_WordFreq_${USERNAME}" \
    -D mapreduce.job.reduces=1 \
    -files "$MAPPER","$REDUCER" \
    -mapper  "python3 wordfreq_mapper.py" \
    -reducer "python3 wordfreq_reducer.py" \
    -input   "$HDFS_INPUT" \
    -output  "$HDFS_OUTPUT"

echo ""
echo "=== Job complete! ==="
echo "Output location: $HDFS_OUTPUT"
echo ""

# ── Show top 20 results from HDFS ─────────────────────────────────────────────
echo "Top 20 words by frequency:"
echo "──────────────────────────"
hdfs dfs -cat "${HDFS_OUTPUT}/part-*" \
    | sort -t$'\t' -k2 -rn \
    | head -20 \
    | awk -F'\t' '{printf "  %-20s %s\n", $1, $2}'

echo ""
echo "To view full output:"
echo "  hdfs dfs -cat ${HDFS_OUTPUT}/part-*"
echo ""
echo "To download output locally:"
echo "  hdfs dfs -getmerge ${HDFS_OUTPUT} ./cluster_wordfreq_output.txt"
