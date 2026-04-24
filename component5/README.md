# Component 5 — Results Aggregation

Consumes **Component 4** normalized output (`book_id\t<json>` per line) and produces a **single JSON summary** comparing **bestseller** (`is_bestseller=1`) vs **standard** (`is_bestseller=0`) cohorts.

## I/O contract

**Input (HDFS text, same as normalization output):**

- One record per line: `book_id<TAB>json`
- JSON must include `metadata.is_bestseller` as `"1"` or `"0"` (other values skipped).
- Uses when present: `sentiment.*`, `total_word_count`, `unique_word_count`, `syntactic` (optional dict of floats).

**Output:**

- One pretty-printed JSON document in `part-*` (use **1 reducer** so there is typically one `part-00000`).
- Top-level fields: `by_cohort`, `comparison` (mean deltas bestseller − standard, directions), `books_aggregated`, `input_lines`, `dropped_lines`, `dropped_by_reason` (mapper-side skips: malformed JSON, missing `is_bestseller`, etc.).

## Local smoke test

From repo root (Linux/macOS/Git Bash):

```bash
bash component5/run_local_smoke_test.sh
```

Or with pytest:

```bash
pytest tests/test_component5.py -v
```

---

## Markov / CSDS 312 cluster — full runbook

Replace `<USER>` with your CWRU id (e.g. `mxf477`). Use **`/mnt/vstor/courses/csds312/<USER>`** for large git copies and book downloads so home quota is not exhausted.

### 1) SSH and workspace

```bash
ssh <USER>@markov.case.edu
mkdir -p /mnt/vstor/courses/csds312/<USER>
cd /mnt/vstor/courses/csds312/<USER>
git clone https://github.com/caio-a18/csds312.git csds312
cd csds312
```

### 2) Download books (on cluster or locally then `scp`)

```bash
cd data_acquisition
python3 download_books.py
cd ..
```

### 3) Upload books + metadata to HDFS

```bash
hdfs dfs -mkdir -p /user/<USER>/gutenberg
hdfs dfs -mkdir -p /user/<USER>/metadata
hdfs dfs -put -f data_acquisition/books/*.txt /user/<USER>/gutenberg/
hdfs dfs -put -f data_acquisition/books_metadata.csv /user/<USER>/metadata/books_metadata.csv
hdfs dfs -ls /user/<USER>/gutenberg
```

### 4) Run Component 1 (word frequency)

The stock `submit_wordfreq_cluster.sh` uses `/user/<USER>/gutenberg` — pass your username:

```bash
bash test_pipeline/submit_wordfreq_cluster.sh <USER>
```

Note the printed **`HDFS_OUTPUT`** path (e.g. `/user/<USER>/wordfreq_output_YYYYMMDD_HHMMSS`).

### 5) Run Component 2 (sentiment)

```bash
bash component2/submit_sentiment_cluster.sh <USER>
```

Note the **`HDFS_OUTPUT`** path for sentiment.

### 6) Syntactic output directory (Component 3)

Until Component 3 is deployed, use an **empty** placeholder directory so normalization still runs:

```bash
hdfs dfs -mkdir -p /user/<USER>/books/output/syntactic_empty
```

If your group later writes real syntactic job output, point `SYNT_DIR` there instead.

### 7) Run Component 4 (normalization)

Substitute your actual wordfreq and sentiment output paths from steps 4–5:

```bash
WF=/user/<USER>/wordfreq_output_YYYYMMDD_HHMMSS
SENT=/user/<USER>/sentiment_output_YYYYMMDD_HHMMSS
SYN=/user/<USER>/books/output/syntactic_empty
META=hdfs:///user/<USER>/metadata/books_metadata.csv
OUTN=/user/<USER>/books/output/normalized

bash normalization/run_job.sh "$WF" "$SENT" "$SYN" "$META" "$OUTN"
```

### 8) Run Component 5 (aggregation)

```bash
bash component5/submit_aggregation_cluster.sh "$OUTN" /user/<USER>/books/output/aggregated
```

### 9) Inspect and download results

```bash
hdfs dfs -cat /user/<USER>/books/output/aggregated/part-*
hdfs dfs -getmerge /user/<USER>/books/output/aggregated ./aggregation_summary.json
```

### Quota reminder

```bash
quotagrp csds312
```

---

## Environment variables

| Variable | Effect |
|----------|--------|
| `NUM_REDUCERS` | Default `1` in `submit_aggregation_cluster.sh` (keep at 1 for one combined summary). |

## Troubleshooting

- **Empty aggregation / zero books:** Normalization lines must include `metadata.is_bestseller` exactly `"0"` or `"1"` (strings).
- **Streaming JAR not found:** Set path manually in `submit_aggregation_cluster.sh` after locating `hadoop-streaming*.jar` on the cluster (`find /usr -name 'hadoop-streaming*.jar'`).
