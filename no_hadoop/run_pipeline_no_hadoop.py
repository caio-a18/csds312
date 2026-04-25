#!/usr/bin/env python3
"""
No-Hadoop fallback pipeline for CSDS 312 literary analysis.

Runs the same logical stages without HDFS/Hadoop:
  Component 1 (wordfreq) -> Component 2 (sentiment) ->
  Component 4 (normalization) -> Component 5 (aggregation)

Outputs are written to a local directory tree so the workflow can run on Markov
even when Hadoop/HDFS commands are unavailable.
"""

import argparse
import csv
import io
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
WORD_MAPPER = REPO_ROOT / "test_pipeline" / "wordfreq_mapper.py"
WORD_REDUCER = REPO_ROOT / "test_pipeline" / "wordfreq_reducer.py"
SENT_MAPPER = REPO_ROOT / "component2" / "sentiment_mapper.py"
SENT_REDUCER = REPO_ROOT / "component2" / "sentiment_reducer.py"
NORM_MAPPER = REPO_ROOT / "normalization" / "mapper.py"
NORM_REDUCER = REPO_ROOT / "normalization" / "reducer.py"
AGG_MAPPER = REPO_ROOT / "component5" / "aggregator_mapper.py"
AGG_REDUCER = REPO_ROOT / "component5" / "aggregator_reducer.py"


def run_component(book_path, mapper, reducer, cwd=None):
    with book_path.open("rb") as fh:
        mapped = subprocess.run(
            [sys.executable, str(mapper)],
            stdin=fh,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd) if cwd else None,
            check=True,
        ).stdout.decode("utf-8", errors="replace")
    sorted_lines = "\n".join(sorted(mapped.splitlines()))
    if sorted_lines:
        sorted_lines += "\n"
    reduced = subprocess.run(
        [sys.executable, str(reducer)],
        input=sorted_lines,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(cwd) if cwd else None,
        check=True,
    ).stdout
    return reduced


def map_for_normalization(component, book_id, text):
    env = os.environ.copy()
    env["NORM_COMPONENT"] = component
    if book_id is not None:
        env["NORM_BOOK_ID"] = str(book_id)
    else:
        env.pop("NORM_BOOK_ID", None)
    return subprocess.run(
        [sys.executable, str(NORM_MAPPER)],
        input=text,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        check=True,
    ).stdout


def parse_args():
    parser = argparse.ArgumentParser(description="Run CSDS 312 pipeline without Hadoop/HDFS.")
    parser.add_argument(
        "--metadata",
        default=str(REPO_ROOT / "data_acquisition" / "books_metadata.csv"),
        help="Path to books_metadata.csv",
    )
    parser.add_argument(
        "--books-dir",
        default=str(REPO_ROOT / "data_acquisition" / "books"),
        help="Directory containing downloaded .txt books",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "no_hadoop" / "output"),
        help="Directory for all intermediate and final outputs",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=max(1, min(8, (os.cpu_count() or 2) // 2)),
        help="Parallel workers for per-book Component 1/2 execution",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional: only process first N books from metadata (0 = all)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    metadata_path = Path(args.metadata).resolve()
    books_dir = Path(args.books_dir).resolve()
    out_root = Path(args.output_dir).resolve()
    out_word = out_root / "wordfreq"
    out_sent = out_root / "sentiment"
    out_norm = out_root / "normalized"
    out_agg = out_root / "aggregated"

    for d in (out_word, out_sent, out_norm, out_agg):
        d.mkdir(parents=True, exist_ok=True)

    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata not found: {metadata_path}")
    if not books_dir.exists():
        raise FileNotFoundError(f"Books dir not found: {books_dir}")

    rows = []  # type: List[Dict[str, str]]
    with metadata_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    if args.limit > 0:
        rows = rows[: args.limit]
    if not rows:
        raise RuntimeError("No metadata rows found.")

    print(f"Processing {len(rows)} books (jobs={args.jobs})")

    def process_row(row):
        book_id = row["book_id"]
        filename = row.get("filename") or f"{book_id}.txt"
        book_path = books_dir / filename
        if not book_path.exists():
            return book_id, False, f"missing file: {book_path}"
        try:
            wf = run_component(book_path, WORD_MAPPER, WORD_REDUCER)
            sent = run_component(book_path, SENT_MAPPER, SENT_REDUCER, cwd=REPO_ROOT / "component2")
        except subprocess.CalledProcessError as e:
            return book_id, False, f"subprocess failed: {e}"

        (out_word / f"{book_id}.tsv").write_text(wf, encoding="utf-8")
        (out_sent / f"{book_id}.tsv").write_text(sent, encoding="utf-8")
        return book_id, True, "ok"

    failures = []  # type: List[Tuple[str, str]]
    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as ex:
        futs = [ex.submit(process_row, row) for row in rows]
        for fut in as_completed(futs):
            book_id, ok, msg = fut.result()
            if ok:
                print(f"[ok]   {book_id}")
            else:
                print(f"[fail] {book_id} — {msg}")
                failures.append((book_id, msg))

    if failures:
        print(f"\nWARNING: {len(failures)} books failed in Component 1/2.")

    # Component 4 mapping stage (local simulation with env overrides)
    mapped_chunks = []  # type: List[str]
    for row in rows:
        book_id = row["book_id"]
        wf_path = out_word / f"{book_id}.tsv"
        sent_path = out_sent / f"{book_id}.tsv"
        if wf_path.exists():
            mapped_chunks.append(map_for_normalization("wordfreq", book_id, wf_path.read_text(encoding="utf-8")))
        if sent_path.exists():
            mapped_chunks.append(map_for_normalization("sentiment", book_id, sent_path.read_text(encoding="utf-8")))

    # Feed metadata only for the rows selected in this run (respects --limit and missing-book filtering).
    meta_buf = io.StringIO()
    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(meta_buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    mapped_chunks.append(map_for_normalization("metadata", None, meta_buf.getvalue()))
    norm_mapped = "".join(mapped_chunks)
    norm_sorted = "\n".join(sorted([ln for ln in norm_mapped.splitlines() if ln.strip()]))
    if norm_sorted:
        norm_sorted += "\n"
    normalized = subprocess.run(
        [sys.executable, str(NORM_REDUCER)],
        input=norm_sorted,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout
    normalized_path = out_norm / "part-00000"
    normalized_path.write_text(normalized, encoding="utf-8")
    print(f"Normalized output: {normalized_path}")

    # Component 5 local simulation
    agg_mapped = subprocess.run(
        [sys.executable, str(AGG_MAPPER)],
        input=normalized,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout
    agg_sorted = "\n".join(sorted([ln for ln in agg_mapped.splitlines() if ln.strip()]))
    if agg_sorted:
        agg_sorted += "\n"
    aggregated = subprocess.run(
        [sys.executable, str(AGG_REDUCER)],
        input=agg_sorted,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout
    agg_path = out_agg / "aggregation_summary.json"
    agg_path.write_text(aggregated, encoding="utf-8")
    print(f"Aggregated summary: {agg_path}")
    print("Done.")


if __name__ == "__main__":
    main()
