#!/usr/bin/env python3
"""
mapper.py — Hadoop Streaming mapper for intermediate data normalization.

Reads output lines from Components 1 (word frequency), 2 (sentiment), and
3 (syntactic complexity), re-emitting each line tagged with book_id and
component source so the reducer can assemble a unified per-book record.

Component type and book_id are detected from the input file path via the
mapreduce_map_input_file environment variable. Override with NORM_COMPONENT
and NORM_BOOK_ID for local testing.

Usage (local simulation):
    NORM_COMPONENT=wordfreq NORM_BOOK_ID=1342 cat wordfreq_output.txt | python3 mapper.py

Usage (Hadoop Streaming):
    See run_job.sh

Emits: book_id\tcomponent\tmetric\tvalue

Author: Zeynep Bastas
"""

import sys
import os
import re

BOOK_ID_RE = re.compile(r"(\d+)")

COMPONENT_KEYWORDS = {
    "wordfreq":  ["wordfreq", "word_freq", "word-freq", "component1", "comp1"],
    "sentiment": ["sentiment", "component2", "comp2"],
    "syntactic": ["syntactic", "syntax", "component3", "comp3"],
    "metadata":  ["metadata", "meta"],
}


def detect_component(filepath: str) -> str:
    path_lower = filepath.lower()
    for component, keywords in COMPONENT_KEYWORDS.items():
        if any(kw in path_lower for kw in keywords):
            return component
    return "unknown"


def extract_book_id(filepath: str) -> str:
    # Prefer a pure-numeric path segment (e.g. /wordfreq/1342/part-00000)
    for segment in reversed(filepath.replace("\\", "/").split("/")):
        if segment.isdigit():
            return segment
    # Fall back to first number found in the filename
    filename = os.path.basename(filepath)
    match = BOOK_ID_RE.search(filename)
    return match.group(1) if match else "unknown"


def get_context() -> tuple:
    filepath = os.environ.get("mapreduce_map_input_file", "")
    component = os.environ.get("NORM_COMPONENT") or detect_component(filepath)
    book_id = os.environ.get("NORM_BOOK_ID") or extract_book_id(filepath)
    return book_id, component


def parse_kv_line(line: str):
    parts = line.split("\t", 1)
    if len(parts) != 2:
        return None
    return parts[0].strip(), parts[1].strip()


def parse_metadata_line(line: str):
    parts = line.split(",", 4)
    if len(parts) < 4:
        return None
    return {
        "book_id":      parts[0].strip(),
        "title":        parts[1].strip(),
        "author":       parts[2].strip(),
        "is_bestseller": parts[3].strip(),
    }


def main():
    book_id, component = get_context()
    first_line = True

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        if component == "wordfreq":
            kv = parse_kv_line(line)
            if kv:
                word, count_str = kv
                try:
                    int(count_str)
                    print(f"{book_id}\twordfreq\t{word}\t{count_str}")
                except ValueError:
                    pass

        elif component == "sentiment":
            kv = parse_kv_line(line)
            if kv:
                metric, value_str = kv
                try:
                    float(value_str)
                    print(f"{book_id}\tsentiment\t{metric}\t{value_str}")
                except ValueError:
                    pass

        elif component == "syntactic":
            kv = parse_kv_line(line)
            if kv:
                metric, value_str = kv
                try:
                    float(value_str)
                    print(f"{book_id}\tsyntactic\t{metric}\t{value_str}")
                except ValueError:
                    pass

        elif component == "metadata":
            if first_line and "book_id" in line.lower():
                first_line = False
                continue
            first_line = False
            meta = parse_metadata_line(line)
            if meta:
                mid = meta["book_id"]
                print(f"{mid}\tmetadata\ttitle\t{meta['title']}")
                print(f"{mid}\tmetadata\tauthor\t{meta['author']}")
                print(f"{mid}\tmetadata\tis_bestseller\t{meta['is_bestseller']}")


if __name__ == "__main__":
    main()
