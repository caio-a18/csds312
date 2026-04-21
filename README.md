# CSDS 312 Literary Success Analysis

This project investigates whether a quantifiable linguistic formula underlies
literary success. We analyze texts from Project Gutenberg and Amazon review
datasets, then compare critically acclaimed novels against standard works.

The data pipeline is built with the Hadoop ecosystem and MapReduce, targeting
parallel execution on the CSDS 312 compute cluster.

## Analysis Components

- `test_pipeline/` (Component 1): word-frequency extraction with Hadoop
  Streaming mapper/reducer scripts.
- `component2/` (Component 2): sentiment scoring pipeline.
- `normalization/` (Component 4): schema normalization that combines component
  outputs into per-book JSON-like records.

## Repository Layout

- `test_pipeline/`: local and cluster scripts for word frequency testing.
- `component2/`: sentiment mapper/reducer and cluster submission script.
- `normalization/`: intermediate normalization mapper/reducer and run script.
- `tests/`: pytest coverage for normalization logic.

## Quick Start

### 1) Run the word-frequency local pipeline

```bash
cd test_pipeline
python3 download_test_book.py
bash test_wordfreq_local.sh
```

### 2) Run normalization tests

```bash
pytest tests/test_normalization.py -v
```

## Cluster Execution

Use component-specific scripts to run on the CSDS 312 cluster:

- `test_pipeline/submit_wordfreq_cluster.sh`
- `component2/submit_sentiment_cluster.sh`
- `normalization/run_job.sh`
