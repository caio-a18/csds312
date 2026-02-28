# Spec: Word Frequency MapReduce — Caio Albuquerque (Group 9)

## Overview

This document specifies Caio's contribution to the CSDS 312 Book Script Project.
The component is a **Hadoop Streaming MapReduce pipeline** that computes
word-frequency distributions across the Project Gutenberg corpus, forming the
foundational linguistic signal used by the rest of the group's analyses
(sentiment polarity, syntactic complexity, etc.).

## Scope (1/5 of full project)

The full project pipeline is:

| # | Component                        | Owner   |
|---|----------------------------------|---------|
| 1 | **Word Frequency MapReduce**     | Caio    |
| 2 | Sentiment Analysis MapReduce     | TBD     |
| 3 | Syntactic Complexity Analysis    | TBD     |
| 4 | Data Acquisition & Corpus Setup  | TBD     |
| 5 | Results Aggregation & Report     | TBD     |

Caio owns component 1 in full, including preprocessing utilities reused by
components 2 and 3.

---

## Goals

1. Accept raw Project Gutenberg plain-text files as input (via HDFS).
2. Strip Gutenberg boilerplate headers/footers.
3. Tokenize, normalize (lowercase, punctuation removal, stop-word filtering).
4. Emit `(word, frequency)` pairs sorted by descending frequency.
5. Produce one output file per book **and** one aggregated corpus-wide file.
6. Be runnable locally (for testing) and on the CSDS 312 cluster via
   `hadoop jar hadoop-streaming.jar`.

---

## Inputs

| Source            | Format           | Notes                                      |
|-------------------|------------------|--------------------------------------------|
| Project Gutenberg | UTF-8 plain text | One `.txt` file per book, stored in HDFS   |
| Stop-word list    | Plain text       | One word per line, `word_frequency/stopwords.txt` |

---

## Outputs

| File                        | Format                        |
|-----------------------------|-------------------------------|
| `output/part-00000` (etc.)  | TSV: `<word>\t<count>`        |
| Sorted by count descending after a secondary sort pass |

---

## Component Files

```
word_frequency/
  mapper.py        # Hadoop Streaming mapper  (stdin → stdout)
  reducer.py       # Hadoop Streaming reducer (stdin → stdout)
  preprocess.py    # Shared text-cleaning utilities
  stopwords.txt    # English stop-word list (~150 words)
  run_job.sh       # Cluster submission script
tests/
  test_word_frequency.py   # pytest suite covering all components
spec/
  caio_word_frequency_spec.md   # This file
```

---

## Mapper Design

**Input** (one line from HDFS split):
```
It was the best of times, it was the worst of times,
```

**Processing**:
1. Strip Gutenberg header/footer sentinel lines.
2. Lowercase the line.
3. Remove punctuation (keep only `[a-z0-9']`).
4. Split on whitespace.
5. Filter stop words and empty tokens.
6. For each surviving token emit: `<word>\t1`

**Output** (key-value pairs, tab-separated):
```
best	1
times	1
worst	1
times	1
```

---

## Reducer Design

**Input**: mapper output, sorted by key (Hadoop guarantee).

**Processing**:
1. For each unique key, sum all values.
2. Emit `<word>\t<total_count>`.

**Output**:
```
best	1
times	2
worst	1
```

---

## Preprocessor Design (`preprocess.py`)

Exported functions (importable by other group members' components):

| Function                        | Description                                            |
|---------------------------------|--------------------------------------------------------|
| `strip_gutenberg(text)`         | Remove Project Gutenberg header/footer boilerplate     |
| `tokenize(text)`                | Lowercase, remove punct, return list of tokens         |
| `remove_stop_words(tokens)`     | Filter tokens against bundled stop-word list           |
| `preprocess_line(line)`         | Convenience: tokenize + remove_stop_words in one call  |

---

## Acceptance Criteria

- [ ] `mapper.py` runs standalone: `echo "Hello world" | python mapper.py`
- [ ] `reducer.py` runs standalone: pipeline test passes
- [ ] End-to-end local simulation produces correct counts
- [ ] All pytest tests pass (`pytest tests/test_word_frequency.py -v`)
- [ ] `run_job.sh` has correct Hadoop Streaming invocation ready for cluster
- [ ] Top-20 words for a sample Gutenberg book match manual spot-check

---

## Non-Goals (out of scope for this component)

- Sentiment tagging (component 2)
- TF-IDF weighting (component 3/5)
- Amazon review data (separate pipeline)
- Visualization / final report

---

## Dependencies

- Python 3.8+ (available on cluster)
- No third-party libraries in mapper/reducer (Hadoop Streaming constraint)
- `pytest` for local testing only
- Hadoop 3.x cluster with HDFS access

---

## Run Instructions

### Local test (simulate MapReduce with pipes)

```bash
cat tests/sample_book.txt | python word_frequency/mapper.py \
  | sort \
  | python word_frequency/reducer.py
```

### Cluster submission

```bash
bash word_frequency/run_job.sh \
  hdfs:///books/input \
  hdfs:///books/output/word_freq
```

---

## Timeline

| Milestone                        | Target  |
|----------------------------------|---------|
| Spec finalized                   | Week 1  |
| mapper.py + reducer.py complete  | Week 2  |
| preprocess.py complete + tests   | Week 2  |
| Integration test on cluster      | Week 3  |
| Hand-off to other components     | Week 3  |
