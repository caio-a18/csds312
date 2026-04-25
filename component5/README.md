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

### 3) Load Hadoop into your shell (required on Markov)

On **markov.case.edu**, `hdfs` and `hadoop` are usually **not** on your default `PATH` on the login node or on compute nodes. You must load the site **Environment Modules** package first (same idea as `module load matlab` in the HPC quick start).

```bash
module avail hadoop 2>&1 | head -50
# If you see a versioned name, load it exactly, for example:
#   module load hadoop/3.3.6
module load hadoop

which hdfs
which hadoop
hdfs version
```

If `module load hadoop` fails, run `module spider hadoop` and use the full module name it prints. If nothing matches, ask your **CSDS 312 TA** or **hpc-support@case.edu** for the current Hadoop module name on Markov.

**Not Python:** `No module named hadoop` from Python is expected. Hadoop is a **Java CLI** (`hdfs`, `hadoop`), not a Python package. After `module load`, use the shell commands below, not `import hadoop`.

**Slurm / compute nodes:** Put `module load hadoop` (or the exact module line) at the top of any batch script, or add it to `~/.bashrc` on the cluster so every `srun` session inherits it.

### 4) Upload books + metadata to HDFS

```bash
hdfs dfs -mkdir -p /user/<USER>/gutenberg
hdfs dfs -mkdir -p /user/<USER>/metadata
hdfs dfs -put -f data_acquisition/books/*.txt /user/<USER>/gutenberg/
hdfs dfs -put -f data_acquisition/books_metadata.csv /user/<USER>/metadata/books_metadata.csv
hdfs dfs -ls /user/<USER>/gutenberg
```

### 5) Run Component 1 (word frequency)

The stock `submit_wordfreq_cluster.sh` uses `/user/<USER>/gutenberg` — pass your username:

```bash
bash test_pipeline/submit_wordfreq_cluster.sh <USER>
```

Note the printed **`HDFS_OUTPUT`** path (e.g. `/user/<USER>/wordfreq_output_YYYYMMDD_HHMMSS`).

### 6) Run Component 2 (sentiment)

```bash
bash component2/submit_sentiment_cluster.sh <USER>
```

Note the **`HDFS_OUTPUT`** path for sentiment.

### 7) Syntactic output directory (Component 3)

```bash
hdfs dfs -mkdir -p /user/<USER>/books/output/syntactic
```

### 8) Run Component 4 (normalization)

Substitute your actual wordfreq and sentiment output paths from steps 5–6:

```bash
WF=/user/<USER>/wordfreq_output_YYYYMMDD_HHMMSS
SENT=/user/<USER>/sentiment_output_YYYYMMDD_HHMMSS
SYN=/user/<USER>/books/output/syntactic
META=hdfs:///user/<USER>/metadata/books_metadata.csv
OUTN=/user/<USER>/books/output/normalized

bash normalization/run_job.sh "$WF" "$SENT" "$SYN" "$META" "$OUTN"
```

### 9) Run Component 5 (aggregation)

```bash
bash component5/submit_aggregation_cluster.sh "$OUTN" /user/<USER>/books/output/aggregated
```

### 10) Inspect and download results

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

- **`hdfs: command not found` on Markov:** Run **`module load hadoop`** (or the versioned name from `module spider hadoop`) in **this** shell session before any `hdfs` / `hadoop jar` commands. Compute nodes from `srun` start a fresh environment unless you load modules in the job or in `~/.bashrc`.
- **`No module named hadoop` in Python:** Normal. Use the **`hdfs`** and **`hadoop`** shell tools after loading the Hadoop module, not `import hadoop`.
- **Empty aggregation / zero books:** Normalization lines must include `metadata.is_bestseller` exactly `"0"` or `"1"` (strings).
- **Streaming JAR not found:** Set path manually in `submit_aggregation_cluster.sh` after locating `hadoop-streaming*.jar` on the cluster (`find /usr -name 'hadoop-streaming*.jar'`).
