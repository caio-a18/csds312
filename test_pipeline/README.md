# CSDS 312 — Hadoop Streaming Word Frequency Test Pipeline

Minimal test setup using *Pride and Prejudice* (Project Gutenberg) to verify
your Hadoop Streaming pipeline before scaling up.

---

## File Overview

| File | Purpose |
|---|---|
| `download_test_book.py` | Downloads the book from Project Gutenberg |
| `test_metadata.csv` | Book metadata (book_id, title, author, is_bestseller, filename) |
| `wordfreq_mapper.py` | Hadoop Streaming mapper — emits `word\t1` |
| `wordfreq_reducer.py` | Hadoop Streaming reducer — emits `word\ttotal_count` |
| `test_wordfreq_local.sh` | Run the pipeline locally (no Hadoop needed) |
| `submit_wordfreq_cluster.sh` | Submit the job to the CSDS 312 cluster |
| `setup_hdfs.sh` | Create HDFS dirs and upload the book |

---

## Workflow A: Local Test (no cluster needed)

```bash
cd test_pipeline/

# 1. Download the book
python3 download_test_book.py

# 2. Run the pipeline locally (simulates Hadoop with cat | sort | python)
bash test_wordfreq_local.sh
```

The local test script will:
- Run `cat book.txt | python3 wordfreq_mapper.py | sort | python3 wordfreq_reducer.py`
- Write output to `local_wordfreq_output.txt`
- Print the top 20 words by frequency

---

## Workflow B: CSDS 312 Cluster

```bash
# 1. (On your local machine) download the book
python3 download_test_book.py

# 2. Copy the pipeline to the cluster
scp -r test_pipeline/ <username>@<cluster-host>:~/csds312/

# 3. SSH into the cluster
ssh <username>@<cluster-host>

# 4. Navigate to the pipeline directory
cd ~/csds312/test_pipeline/

# 5. Set up HDFS (creates /user/<username>/gutenberg/ and uploads book)
bash setup_hdfs.sh <username>

# 6. Submit the Hadoop Streaming job
bash submit_wordfreq_cluster.sh <username>
```

The submit script will:
- Run the job with 1 reducer (sufficient for a single book)
- Write output to `/user/<username>/wordfreq_output_<timestamp>/`
- Print the top 20 words directly from HDFS

---

## How Hadoop Streaming Works

Hadoop Streaming lets you write mappers and reducers in any language that can
read from stdin and write to stdout.

```
HDFS input files
      │
      ▼ (split into chunks, one chunk per mapper)
┌─────────────┐
│   Mapper    │  reads lines from stdin
│             │  emits: word\t1  (tab-separated key\tvalue)
└─────────────┘
      │
      ▼ (framework sorts all key-value pairs by key — the "shuffle")
┌─────────────┐
│   Reducer   │  reads sorted key\tvalue pairs from stdin
│             │  accumulates counts per word
│             │  emits: word\ttotal_count
└─────────────┘
      │
      ▼
HDFS output  (part-00000, part-00001, ...)
```

Key rule: **tab** (`\t`) separates key from value. Everything before the first
tab is the key (used for sorting and grouping); everything after is the value.

---

## Gutenberg Header/Footer Handling

Project Gutenberg files include license text before and after the actual book.
The mapper skips everything before:
```
*** START OF THE PROJECT GUTENBERG EBOOK PRIDE AND PREJUDICE ***
```
and stops at:
```
*** END OF THE PROJECT GUTENBERG EBOOK PRIDE AND PREJUDICE ***
```

---

## Troubleshooting

**Download fails:** Check your internet connection. The script retries once.
Run manually: `python3 download_test_book.py`

**`hdfs` command not found:** You are not on the cluster, or Hadoop is not in
your PATH. Try: `export PATH=$PATH:/usr/local/hadoop/bin`

**Streaming JAR not found:** Edit `submit_wordfreq_cluster.sh` and set
`STREAMING_JAR` to the correct path. Ask your TA for the cluster-specific path.

**Output directory already exists:** Hadoop will refuse to overwrite. The
submit script uses a timestamp suffix to avoid this, but if needed:
```bash
hdfs dfs -rm -r /user/<username>/wordfreq_output_<timestamp>
```
