#!/usr/bin/env python3
"""
run_load_test.py — Local load and stress test for the CSDS 312 literary analysis pipeline.

Runs Components 1 (word frequency) and 2 (sentiment analysis) against every book in
books_metadata.csv, times each step, validates output, and reports a summary table with
a Hadoop parallel speedup estimate.

The local simulation mirrors what Hadoop Streaming does on the cluster:
    cat book.txt | python3 mapper.py | sort | python3 reducer.py

Usage:
    python3 run_load_test.py
    python3 run_load_test.py --metadata ../data_acquisition/books_metadata.csv
    python3 run_load_test.py --books-dir ../data_acquisition/books
"""

import argparse
import csv
import os
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WORDFREQ_MAPPER  = os.path.join(REPO_ROOT, "test_pipeline", "wordfreq_mapper.py")
WORDFREQ_REDUCER = os.path.join(REPO_ROOT, "test_pipeline", "wordfreq_reducer.py")
SENTIMENT_MAPPER  = os.path.join(REPO_ROOT, "component2", "sentiment_mapper.py")
SENTIMENT_REDUCER = os.path.join(REPO_ROOT, "component2", "sentiment_reducer.py")
SENTIMENT_DIR     = os.path.join(REPO_ROOT, "component2")


def run_component(mapper: str, reducer: str, book_path: str, cwd: str = None) -> tuple[str | None, float]:
    """
    Simulates a single Hadoop Streaming job locally:
        cat book_path | python3 mapper | sort | python3 reducer

    Returns (output_text, elapsed_seconds). Output is None on any subprocess error.
    """
    env = os.environ.copy()
    t0 = time.perf_counter()

    try:
        with open(book_path, "rb") as fh:
            map_result = subprocess.run(
                [sys.executable, mapper],
                stdin=fh,
                capture_output=True,
                cwd=cwd,
                env=env,
            )
    except OSError as e:
        return None, time.perf_counter() - t0

    if map_result.returncode != 0:
        return None, time.perf_counter() - t0

    mapped_lines = map_result.stdout.decode("utf-8", errors="replace").splitlines()
    sorted_input = "\n".join(sorted(mapped_lines)) + "\n"

    red_result = subprocess.run(
        [sys.executable, reducer],
        input=sorted_input,
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env,
    )

    elapsed = time.perf_counter() - t0
    if red_result.returncode != 0:
        return None, elapsed
    return red_result.stdout, elapsed


def parse_wordfreq(output: str | None) -> tuple[bool, int]:
    """Returns (valid, unique_word_count)."""
    if not output:
        return False, 0
    lines = [l for l in output.strip().splitlines() if "\t" in l]
    valid_lines = [l for l in lines if l.split("\t", 1)[1].strip().isdigit()]
    return len(valid_lines) > 0, len(valid_lines)


def parse_sentiment(output: str | None) -> tuple[bool, dict]:
    """Returns (valid, {metric: float})."""
    if not output:
        return False, {}
    metrics = {}
    for line in output.strip().splitlines():
        parts = line.split("\t", 1)
        if len(parts) == 2:
            try:
                metrics[parts[0].strip()] = float(parts[1].strip())
            except ValueError:
                pass
    required = {"polarity_score", "positive_count", "negative_count", "total_scored_words"}
    return required.issubset(metrics), metrics


def col(value, width: int) -> str:
    return str(value)[:width].ljust(width)


def check_scripts() -> list[str]:
    missing = []
    for path in [WORDFREQ_MAPPER, WORDFREQ_REDUCER, SENTIMENT_MAPPER, SENTIMENT_REDUCER]:
        if not os.path.exists(path):
            missing.append(path)
    return missing


def main():
    parser = argparse.ArgumentParser(description="Load and stress test for the CSDS 312 pipeline")
    parser.add_argument("--metadata", default=None,
                        help="Path to books_metadata.csv")
    parser.add_argument("--books-dir", default=None,
                        help="Directory containing downloaded .txt files")
    args = parser.parse_args()

    data_dir = os.path.join(REPO_ROOT, "data_acquisition")
    metadata_path = args.metadata or os.path.join(data_dir, "books_metadata.csv")
    books_dir = args.books_dir or os.path.join(data_dir, "books")

    missing_scripts = check_scripts()
    if missing_scripts:
        print("ERROR: Required pipeline scripts not found:")
        for p in missing_scripts:
            print(f"  {p}")
        sys.exit(1)

    if not os.path.exists(metadata_path):
        print(f"ERROR: Metadata file not found: {metadata_path}")
        print("Run data_acquisition/download_books.py first.")
        sys.exit(1)

    with open(metadata_path, newline="", encoding="utf-8") as f:
        all_books = list(csv.DictReader(f))

    available = []
    skipped = []
    for book in all_books:
        path = os.path.join(books_dir, book["filename"])
        if os.path.exists(path):
            book["_path"] = path
            book["_size_kb"] = os.path.getsize(path) // 1024
            available.append(book)
        else:
            skipped.append(book["title"])

    div = "=" * 78
    print(f"\n{div}")
    print("  CSDS 312 — Literary Analysis Pipeline: Load & Stress Test")
    print(div)
    print(f"  Books in metadata:  {len(all_books)}")
    print(f"  Books on disk:      {len(available)}")
    if skipped:
        print(f"  Skipped (missing):  {len(skipped)}")
    print(f"  Metadata:           {metadata_path}")
    print(f"  Books dir:          {books_dir}")
    print(f"{div}\n")

    if not available:
        print("No books found on disk. Run data_acquisition/download_books.py first.")
        sys.exit(1)

    results = []
    for i, book in enumerate(available, 1):
        short_title = book["title"][:38]
        print(f"  [{i:2d}/{len(available)}] {short_title:<38}", end=" ", flush=True)

        wf_out, wf_time = run_component(WORDFREQ_MAPPER, WORDFREQ_REDUCER, book["_path"])
        wf_ok, unique_words = parse_wordfreq(wf_out)

        sent_out, sent_time = run_component(
            SENTIMENT_MAPPER, SENTIMENT_REDUCER, book["_path"], cwd=SENTIMENT_DIR
        )
        sent_ok, sent_metrics = parse_sentiment(sent_out)

        total = wf_time + sent_time
        polarity = sent_metrics.get("polarity_score", 0.0)
        print(f"  {total:.2f}s  {'OK' if wf_ok and sent_ok else 'ERR'}")

        results.append({
            "title":        book["title"],
            "bestseller":   book.get("is_bestseller", "0"),
            "size_kb":      book["_size_kb"],
            "wf_time":      wf_time,
            "sent_time":    sent_time,
            "total_time":   total,
            "unique_words": unique_words,
            "polarity":     polarity,
            "wf_ok":        wf_ok,
            "sent_ok":      sent_ok,
        })

    results.sort(key=lambda r: -r["total_time"])

    # ── Results table ────────────────────────────────────────────────────────
    print(f"\n{div}")
    print("  Results  (sorted slowest → fastest)")
    print(div)
    HDR = f"  {'Title':<38}  {'B':1}  {'KB':>5}  {'WF(s)':>7}  {'Sent(s)':>7}  {'Total(s)':>8}  {'Words':>8}  {'Polarity':>9}"
    print(HDR)
    print("  " + "-" * 74)
    for r in results:
        wf_str   = f"{r['wf_time']:.3f}"   + ("" if r["wf_ok"]   else "!")
        sent_str = f"{r['sent_time']:.3f}" + ("" if r["sent_ok"] else "!")
        words_str = f"{r['unique_words']:,}" if r["wf_ok"] else "err"
        pol_str   = f"{r['polarity']:+.4f}"  if r["sent_ok"] else "err"
        b_flag    = "Y" if r["bestseller"] == "1" else "N"
        print(
            f"  {r['title'][:38]:<38}  {b_flag:1}  {r['size_kb']:>5}  "
            f"{wf_str:>7}  {sent_str:>7}  {r['total_time']:>8.3f}  "
            f"{words_str:>8}  {pol_str:>9}"
        )

    # ── Summary ──────────────────────────────────────────────────────────────
    total_wall   = sum(r["total_time"] for r in results)
    max_time     = max(r["total_time"] for r in results)
    avg_time     = total_wall / len(results)
    slowest      = max(results, key=lambda r: r["total_time"])
    fastest      = min(results, key=lambda r: r["total_time"])
    speedup      = total_wall / max_time if max_time > 0 else 1.0
    wf_errors    = sum(1 for r in results if not r["wf_ok"])
    sent_errors  = sum(1 for r in results if not r["sent_ok"])

    bestsellers = [r for r in results if r["bestseller"] == "1" and r["sent_ok"]]
    standards   = [r for r in results if r["bestseller"] == "0" and r["sent_ok"]]
    avg_pol_b   = sum(r["polarity"] for r in bestsellers) / len(bestsellers) if bestsellers else 0.0
    avg_pol_s   = sum(r["polarity"] for r in standards)   / len(standards)   if standards   else 0.0

    print(f"\n{div}")
    print("  Summary")
    print(div)
    print(f"  Books tested:               {len(results)}")
    print(f"  Sequential total time:      {total_wall:.2f}s")
    print(f"  Average time per book:      {avg_time:.2f}s")
    print(f"  Fastest book:               {fastest['title']} ({fastest['total_time']:.2f}s)")
    print(f"  Slowest book:               {slowest['title']} ({slowest['total_time']:.2f}s)")
    print()
    print(f"  Estimated Hadoop parallel time:    {max_time:.2f}s  (bottleneck = slowest book)")
    print(f"  Estimated parallel speedup:        {speedup:.1f}x over sequential")
    print()
    print(f"  Word frequency errors:      {wf_errors}")
    print(f"  Sentiment errors:           {sent_errors}")
    print()
    if bestsellers:
        print(f"  Avg polarity — bestsellers ({len(bestsellers)} books):    {avg_pol_b:+.4f}")
    if standards:
        print(f"  Avg polarity — standard   ({len(standards)} books):    {avg_pol_s:+.4f}")
    if bestsellers and standards:
        diff = avg_pol_b - avg_pol_s
        direction = "more positive" if diff > 0 else "more negative"
        print(f"  Polarity gap:               {diff:+.4f}  (bestsellers are {direction})")
    print(f"{div}\n")


if __name__ == "__main__":
    main()
