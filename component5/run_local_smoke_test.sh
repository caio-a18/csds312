#!/usr/bin/env bash
# Local smoke test: pipe synthetic normalized lines through mapper | sort | reducer.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

cat <<'EOF' >"$TMP"
1342	{"book_id": "1342", "metadata": {"title": "Pride and Prejudice", "author": "Jane Austen", "is_bestseller": "1"}, "sentiment": {"polarity_score": 0.1, "positive_count": 10.0, "negative_count": 5.0, "total_scored_words": 100.0}, "syntactic": {"avg_sentence_length": 20.0}, "total_word_count": 1000, "unique_word_count": 400, "top_words": [["the", 50]]}
84	{"book_id": "84", "metadata": {"title": "Frankenstein", "author": "Mary Shelley", "is_bestseller": "1"}, "sentiment": {"polarity_score": -0.05, "positive_count": 8.0, "negative_count": 8.0, "total_scored_words": 80.0}, "total_word_count": 800, "unique_word_count": 300, "top_words": []}
219	{"book_id": "219", "metadata": {"title": "Heart of Darkness", "author": "Joseph Conrad", "is_bestseller": "0"}, "sentiment": {"polarity_score": 0.0, "positive_count": 5.0, "negative_count": 5.0, "total_scored_words": 50.0}, "total_word_count": 500, "unique_word_count": 200, "top_words": []}
EOF

python3 "$SCRIPT_DIR/aggregator_mapper.py" <"$TMP" \
  | sort -t$'\t' -k1,1 \
  | python3 "$SCRIPT_DIR/aggregator_reducer.py"

echo ""
echo "Smoke test OK."
