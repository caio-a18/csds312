#!/usr/bin/env bash
# test_syntactic_local.sh
# Runs Component 3 locally against every .txt file in ../data_acquisition/books/
# Usage: bash test_syntactic_local.sh [books_dir]
#
# Mimics what Hadoop Streaming does:
#   - sets mapreduce_map_input_file env var per file
#   - pipes file through mapper, sorts, then pipes through reducer

set -euo pipefail

BOOKS_DIR="${1:-../data_acquisition/books}"
MAPPER="$(dirname "$0")/mapper.py"
REDUCER="$(dirname "$0")/reducer.py"
OUTPUT_DIR="$(dirname "$0")/output_local"

if [[ ! -d "$BOOKS_DIR" ]]; then
  echo "ERROR: books directory not found at $BOOKS_DIR"
  echo "Run data_acquisition/download_books.py first, or pass the correct path."
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

TMP_MAPPED="$(mktemp /tmp/component3_mapped.XXXXXX)"

echo "=== Component 3: Syntactic Complexity (local test) ==="
echo "Books dir : $BOOKS_DIR"
echo "Output dir: $OUTPUT_DIR"
echo ""

total=0
for BOOK_PATH in "$BOOKS_DIR"/*.txt; do
  [[ -f "$BOOK_PATH" ]] || continue
  export mapreduce_map_input_file="$BOOK_PATH"
  python3 "$MAPPER" < "$BOOK_PATH" >> "$TMP_MAPPED"
  ((total++)) || true
done

echo "Mapped $total book(s). Sorting and reducing..."

sort "$TMP_MAPPED" | python3 "$REDUCER" > "$OUTPUT_DIR/syntactic_output.tsv"

rm -f "$TMP_MAPPED"

echo ""
echo "=== Results ==="
column -t -s $'\t' "$OUTPUT_DIR/syntactic_output.tsv"
echo ""
echo "Full output written to: $OUTPUT_DIR/syntactic_output.tsv"
