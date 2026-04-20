#!/usr/bin/env python3
"""
reducer.py — Hadoop Streaming reducer for intermediate data normalization.

Reads sorted book_id\tcomponent\tmetric\tvalue lines from stdin (pre-sorted
by book_id, as guaranteed by Hadoop shuffle), assembles a unified per-book
record, and emits book_id\t<json> to stdout.

Partial records (missing components not yet available) are emitted as-is;
Component 5 filters incomplete records during aggregation.

Usage (local simulation):
    cat mapper_output.txt | sort | python3 reducer.py

Author: Zeynep Bastas
"""

import sys
import json

TOP_WORDS_LIMIT = 20


def parse_line(line: str):
    parts = line.split("\t", 3)
    if len(parts) != 4:
        return None
    return parts[0], parts[1], parts[2], parts[3]


def finalize_record(book_id: str, entries: list) -> dict:
    record = {
        "book_id":        book_id,
        "metadata":       {},
        "word_frequencies": {},
        "sentiment":      {},
        "syntactic":      {},
    }

    for component, metric, value_str in entries:
        if component == "wordfreq":
            try:
                record["word_frequencies"][metric] = int(value_str)
            except ValueError:
                pass
        elif component == "sentiment":
            try:
                record["sentiment"][metric] = float(value_str)
            except ValueError:
                pass
        elif component == "syntactic":
            try:
                record["syntactic"][metric] = float(value_str)
            except ValueError:
                pass
        elif component == "metadata":
            record["metadata"][metric] = value_str

    if record["word_frequencies"]:
        wf = record["word_frequencies"]
        record["total_word_count"] = sum(wf.values())
        record["unique_word_count"] = len(wf)
        record["top_words"] = sorted(wf.items(), key=lambda x: -x[1])[:TOP_WORDS_LIMIT]

    del record["word_frequencies"]

    return record


def main():
    current_book_id = None
    current_entries = []

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parsed = parse_line(line)
        if not parsed:
            continue

        book_id, component, metric, value_str = parsed

        if book_id != current_book_id:
            if current_book_id is not None:
                record = finalize_record(current_book_id, current_entries)
                print(f"{current_book_id}\t{json.dumps(record, ensure_ascii=False)}")
            current_book_id = book_id
            current_entries = []

        current_entries.append((component, metric, value_str))

    if current_book_id is not None:
        record = finalize_record(current_book_id, current_entries)
        print(f"{current_book_id}\t{json.dumps(record, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
