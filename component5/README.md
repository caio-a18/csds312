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

## No-Hadoop fallback (Markov-safe)

If your Markov environment does not provide `hdfs`/`hadoop`, run the same logic
locally on the cluster filesystem using the fallback orchestrator:

```bash
python3 no_hadoop/run_pipeline_no_hadoop.py \
  --metadata data_acquisition/books_metadata.csv \
  --books-dir data_acquisition/books \
  --output-dir /mnt/vstor/courses/csds312/<USER>/pipeline_no_hadoop_output \
  --jobs 8
```

This runs:
- Component 1 mapper/reducer per book
- Component 2 mapper/reducer per book
- Component 4 normalization mapper/reducer
- Component 5 aggregation mapper/reducer

Key outputs:
- `.../pipeline_no_hadoop_output/normalized/part-00000`
- `.../pipeline_no_hadoop_output/aggregated/aggregation_summary.json`

---

## Markov / CSDS 312 cluster — no-Hadoop runbook

Use this workflow when the cluster does not provide `hdfs` / `hadoop`.
It runs the same component logic fully on the cluster filesystem.

Replace `<USER>` with your CWRU id (e.g. `mxf477`). Store large files under
`/mnt/vstor/courses/csds312/<USER>`.

### 1) SSH and workspace

```bash
ssh <USER>@markov.case.edu
mkdir -p /mnt/vstor/courses/csds312/<USER>
cd /mnt/vstor/courses/csds312/<USER>
git clone https://github.com/caio-a18/csds312.git csds312
cd csds312
```

### 2) Ensure Python and data are ready

```bash
python3 --version
cd data_acquisition
python3 download_books.py
cd ..
```

### 3) Run the full fallback pipeline

```bash
python3 no_hadoop/run_pipeline_no_hadoop.py \
  --metadata data_acquisition/books_metadata.csv \
  --books-dir data_acquisition/books \
  --output-dir /mnt/vstor/courses/csds312/<USER>/pipeline_no_hadoop_output \
  --jobs 8
```

This executes, in order:
- Component 1 word frequency mapper/reducer per book
- Component 2 sentiment mapper/reducer per book
- Component 4 normalization mapper/reducer
- Component 5 aggregation mapper/reducer

### 4) Inspect outputs

```bash
OUT=/mnt/vstor/courses/csds312/<USER>/pipeline_no_hadoop_output
ls -lah "$OUT"
ls -lah "$OUT/wordfreq" | head
ls -lah "$OUT/sentiment" | head
cat "$OUT/normalized/part-00000" | head -5
cat "$OUT/aggregated/aggregation_summary.json"
```

### 5) Quota reminder

```bash
quotagrp csds312
```

### Optional: quick smoke test

```bash
python3 no_hadoop/run_pipeline_no_hadoop.py \
  --limit 2 \
  --jobs 1 \
  --output-dir /mnt/vstor/courses/csds312/<USER>/pipeline_no_hadoop_smoke
```

---

## Environment variables

| Variable | Effect |
|----------|--------|
| `--jobs` (CLI flag) | Number of parallel workers for per-book Component 1/2 processing. |
| `--limit` (CLI flag) | Optional partial run size for quick smoke tests. |

## Troubleshooting

- **`hdfs: command not found` on Markov:** Expected in this workflow. Ignore Hadoop commands and use only `no_hadoop/run_pipeline_no_hadoop.py`.
- **Python syntax errors on cluster:** Use `python3 --version`. If it is old, pull latest repo changes (the fallback and component5 scripts were made Python 3.6-compatible).
- **Empty aggregation / zero books:** Normalization lines must include `metadata.is_bestseller` exactly `"0"` or `"1"` (strings).
- **Some books fail during Component 1/2:** The fallback continues and reports failures; re-run after fixing missing files in `data_acquisition/books`.
