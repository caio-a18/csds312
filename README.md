# CSDS 312 Literary Success Analysis

**Group 9**: Caio Albuquerque, Mitchell Fein, Dilan Fajardo, Adam Hamdam, Harish Ragopalan, Zeynep Bastas

This project investigates whether a quantifiable linguistic formula underlies literary success. We analyze 100+ texts from Project Gutenberg, comparing critically acclaimed novels against standard works across three dimensions: word frequency, sentiment polarity, and syntactic complexity. The pipeline is built with the Hadoop ecosystem and MapReduce, targeting parallel execution on the CSDS 312 compute cluster.

---

## Pipeline Overview

```
Books (HDFS)
    │
    ├──► Component 1: Word Frequency       (test_pipeline/)
    ├──► Component 2: Sentiment Analysis   (component2/)
    └──► Component 3: Syntactic Complexity (in progress)
              │
              ▼
         Component 4: Normalization        (normalization/)
              │
              ▼
         Component 5: Results Aggregation  (component5/)
```

---

## Components

| # | Name | Directory | Owner | Status |
|---|------|-----------|-------|--------|
| 1 | Word Frequency MapReduce | `test_pipeline/` | Adam Hamdan and Caio Albuquerque | Done |
| 2 | Sentiment Analysis MapReduce | `component2/` | Dilan Fajardo | Done |
| 3 | Syntactic Complexity MapReduce | — | Harish Ragopalan | In progress |
| 4 | Intermediate Data Normalization | `normalization/` | Zeynep Bastas | Done — mapper, reducer, cluster script, 62 pytest tests; wired Components 1–3 into unified per-book JSON schema for Component 5 |
| 5 | Results Aggregation | `component5/` | Mitchell Fein | Done |
| — | Data Acquisition | `data_acquisition/` | Caio Albuquerque | Done |
| — | Load & Stress Testing | `load_testing/` | Caio Albuquerque | Done |

---

## Repository Layout

```
csds312/
├── test_pipeline/          Component 1 — word frequency mapper/reducer + cluster scripts
├── component2/             Component 2 — sentiment mapper/reducer, AFINN-111 lexicon
├── normalization/          Component 4 — normalization mapper/reducer + cluster script
├── component5/             Component 5 — cohort aggregation mapper/reducer + cluster script
├── no_hadoop/              Fallback orchestrator when Hadoop/HDFS is unavailable
├── data_acquisition/       Book downloader + books_metadata.csv generator
├── load_testing/           Local load and stress test across all pipeline components
└── tests/                  pytest suite for Components 4–5
```

---

## Quick Start

### 1. Download books

```bash
cd data_acquisition
python3 download_books.py
```

Downloads 20 curated Project Gutenberg books (14 bestsellers, 6 standard works) and writes `books_metadata.csv`.

### 2. Run the word-frequency pipeline locally

```bash
cd test_pipeline
bash test_wordfreq_local.sh
```

### 3. Run the load and stress test

```bash
cd load_testing
python3 run_load_test.py
```

Pipes every downloaded book through Components 1 and 2, times each step, validates output, and prints a summary table with an estimated Hadoop parallel speedup.

### 4. Run normalization tests

```bash
pytest tests/test_normalization.py -v
```

### 5. Run results aggregation tests

```bash
pytest tests/test_component5.py -v
```

---

## Cluster Execution

On **Markov** (`markov.case.edu`), load Hadoop before using `hdfs` / `hadoop` (they are not on the default `PATH`): run `module avail hadoop` or `module spider hadoop`, then `module load` the module name shown. Step-by-step: [component5/README.md](component5/README.md) (section *Load Hadoop into your shell*).

Upload books to HDFS first:

```bash
cd test_pipeline
bash setup_hdfs.sh <your-username>
```

For **many books**, prefer uploading all `data_acquisition/books/*.txt` and `books_metadata.csv` to HDFS under `/user/<username>/gutenberg` and `/user/<username>/metadata/` (see [component5/README.md](component5/README.md) for a full Markov runbook).

Then submit each component job:

```bash
bash test_pipeline/submit_wordfreq_cluster.sh <your-username>
bash component2/submit_sentiment_cluster.sh <your-username>
bash normalization/run_job.sh <wordfreq_dir> <sentiment_dir> <syntactic_dir> <metadata_file> <output_dir>
bash component5/submit_aggregation_cluster.sh <normalized_output_dir> [aggregation_output_dir]
```

**Component 5** reads normalized `book_id\tjson` lines and writes a JSON cohort summary (bestseller vs standard). Use **`NUM_REDUCERS=1`** (default in `submit_aggregation_cluster.sh`) so output is a single combined `part-*` file.

If Hadoop/HDFS is not available on your Markov environment, run the no-Hadoop fallback (same component logic) documented in [component5/README.md](component5/README.md):

```bash
python3 no_hadoop/run_pipeline_no_hadoop.py --jobs 8 --output-dir /mnt/vstor/courses/csds312/<your-username>/pipeline_no_hadoop_output
```

---

## Storage Notes

Large files (downloaded books, HDFS outputs) should be stored in `/mnt/vstor/courses/csds312` on the cluster, not in your home directory. Check quota with:

```bash
quotagrp csds312
```
