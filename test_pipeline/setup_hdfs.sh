#!/usr/bin/env bash
# setup_hdfs.sh
# ──────────────────────────────────────────────────────────────────────────────
# Set up HDFS directories, upload the test book, and optionally run a
# quick word count job on the CSDS 312 cluster.
#
# Usage:
#   bash setup_hdfs.sh <your-username>
#
# Run this ONCE after downloading the book locally and copying it to the cluster.
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

USERNAME="${1:-$USER}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_BOOK="${SCRIPT_DIR}/1342_pride_and_prejudice.txt"

HDFS_BASE="/user/${USERNAME}"
HDFS_INPUT="${HDFS_BASE}/gutenberg"
HDFS_BOOK="${HDFS_INPUT}/1342_pride_and_prejudice.txt"

echo "=== HDFS Setup for CSDS 312 Word Frequency Pipeline ==="
echo "Username:     $USERNAME"
echo "HDFS base:    $HDFS_BASE"
echo "HDFS input:   $HDFS_INPUT"
echo ""

# ── Check local book exists ────────────────────────────────────────────────────
if [[ ! -f "$LOCAL_BOOK" ]]; then
    echo "ERROR: Local book file not found: $LOCAL_BOOK"
    echo "       Run: python3 download_test_book.py"
    exit 1
fi

# ── Create HDFS directories ────────────────────────────────────────────────────
echo "Step 1: Creating HDFS directories..."

# Create base user directory if it doesn't exist
hdfs dfs -mkdir -p "$HDFS_BASE" 2>/dev/null || true

# Create gutenberg input directory
if hdfs dfs -test -d "$HDFS_INPUT"; then
    echo "  Directory already exists: $HDFS_INPUT"
else
    hdfs dfs -mkdir -p "$HDFS_INPUT"
    echo "  Created: $HDFS_INPUT"
fi

# ── Upload book to HDFS ────────────────────────────────────────────────────────
echo ""
echo "Step 2: Uploading book to HDFS..."

if hdfs dfs -test -f "$HDFS_BOOK"; then
    echo "  File already exists in HDFS: $HDFS_BOOK"
    echo "  Skipping upload. To re-upload, run:"
    echo "    hdfs dfs -rm $HDFS_BOOK"
    echo "    bash setup_hdfs.sh $USERNAME"
else
    hdfs dfs -put "$LOCAL_BOOK" "$HDFS_BOOK"
    echo "  Uploaded: $LOCAL_BOOK → $HDFS_BOOK"
fi

# ── Verify upload ──────────────────────────────────────────────────────────────
echo ""
echo "Step 3: Verifying HDFS contents..."
hdfs dfs -ls "$HDFS_INPUT"
echo ""

HDFS_SIZE=$(hdfs dfs -du -s "$HDFS_BOOK" | awk '{print $1}')
LOCAL_SIZE=$(wc -c < "$LOCAL_BOOK")
echo "  Local file size:  $LOCAL_SIZE bytes"
echo "  HDFS file size:   $HDFS_SIZE bytes"

if [[ "$HDFS_SIZE" == "$LOCAL_SIZE" ]]; then
    echo "  Size check: PASSED"
else
    echo "  WARNING: Size mismatch. Upload may be incomplete."
fi

# ── Print first few lines from HDFS to confirm content ────────────────────────
echo ""
echo "Step 4: Spot-checking HDFS file content (first 5 lines)..."
hdfs dfs -cat "$HDFS_BOOK" 2>/dev/null | head -5 || true

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo "  Run cluster job:  bash submit_wordfreq_cluster.sh $USERNAME"
echo ""
echo "Useful HDFS commands:"
echo "  List input dir:   hdfs dfs -ls $HDFS_INPUT"
echo "  View book:        hdfs dfs -cat $HDFS_BOOK | head -50"
echo "  Remove input:     hdfs dfs -rm -r $HDFS_INPUT"
