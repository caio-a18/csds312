#!/usr/bin/env python3
"""
reducer.py — Hadoop Streaming reducer for word frequency analysis.

Reads tab-separated (word, count) pairs from stdin (pre-sorted by key,
as guaranteed by Hadoop shuffle), accumulates totals, and emits
(word, total_count) to stdout.

Usage (local simulation):
    cat book.txt | python mapper.py | sort | python reducer.py

Usage (Hadoop Streaming):
    See mapper.py header for full invocation.

Author: Caio Albuquerque
"""

import sys


def main():
    current_word = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t", 1)
        if len(parts) != 2:
            # Malformed line — skip silently
            continue

        word, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            continue

        if word == current_word:
            current_count += count
        else:
            if current_word is not None:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    # Flush final word
    if current_word is not None:
        print(f"{current_word}\t{current_count}")


if __name__ == "__main__":
    main()
