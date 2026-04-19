#!/usr/bin/env bash
# test_wordfreq_local.sh
# ──────────────────────────────────────────────────────────────────────────────
# Local test for the word frequency pipeline (no Hadoop required).
# Simulates the Hadoop Streaming pipeline:
#   cat <input> | mapper | sort | reducer
#
# Usage:
#   bash test_wordfreq_local.sh [path/to/book.txt]
#
# If no book path is given, looks for 1342_pride_and_prejudice.txt in the
# same directory as this script.
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Determine input file
if [[ $# -ge 1 ]]; then
    INPUT_FILE="$1"
else
    INPUT_FILE="${SCRIPT_DIR}/1342_pride_and_prejudice.txt"
fi

MAPPER="${SCRIPT_DIR}/wordfreq_mapper.py"
REDUCER="${SCRIPT_DIR}/wordfreq_reducer.py"
OUTPUT_FILE="${SCRIPT_DIR}/local_wordfreq_output.txt"

# ── Sanity checks ──────────────────────────────────────────────────────────────
if [[ ! -f "$INPUT_FILE" ]]; then
    echo "ERROR: Input file not found: $INPUT_FILE"
    echo "       Run: python3 download_test_book.py"
    exit 1
fi

if [[ ! -f "$MAPPER" ]]; then
    echo "ERROR: Mapper not found: $MAPPER"
    exit 1
fi

if [[ ! -f "$REDUCER" ]]; then
    echo "ERROR: Reducer not found: $REDUCER"
    exit 1
fi

# ── Run pipeline ───────────────────────────────────────────────────────────────
echo "=== Local Word Frequency Test ==="
echo "Input:   $INPUT_FILE"
echo "Mapper:  $MAPPER"
echo "Reducer: $REDUCER"
echo ""

echo "Running pipeline..."
# Mimic Hadoop Streaming: pipe → mapper → sort (shuffle) → reducer
cat "$INPUT_FILE" \
    | python3 "$MAPPER" \
    | sort \
    | python3 "$REDUCER" \
    > "$OUTPUT_FILE"

echo "Output written to: $OUTPUT_FILE"
echo ""

# ── Show top 20 words by frequency ────────────────────────────────────────────
echo "Top 20 words by frequency (sorted by count descending):"
echo "────────────────────────────────────────────────────────"
# Sort output by count (field 2) numerically descending, show top 20
sort -t$'\t' -k2 -rn "$OUTPUT_FILE" | head -20 | awk -F'\t' '{printf "  %-20s %s\n", $1, $2}'

echo ""
TOTAL=$(wc -l < "$OUTPUT_FILE")
echo "Total unique words: $TOTAL"
echo ""
echo "Full output: $OUTPUT_FILE"
