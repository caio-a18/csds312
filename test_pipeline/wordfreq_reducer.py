#!/usr/bin/env python3
"""
Hadoop Streaming Reducer: Word Frequency
=========================================
Hadoop Streaming key-value format:
  - Reducer receives stdin lines pre-sorted by key (word)
  - Each line is: <word>\t<count>
  - Reducer emits: <word>\t<total_count> on stdout

Because Hadoop guarantees sort order, we can use a simple "current key"
accumulator pattern — no need to load all data into memory.
"""

import sys


def main():
    current_word = None
    current_count = 0

    for line in sys.stdin:
        line = line.rstrip("\n")

        # Split on tab — Hadoop Streaming uses tab as key/value separator
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue  # skip malformed lines

        word, count_str = parts

        # Guard against non-integer values (shouldn't happen, but be safe)
        try:
            count = int(count_str)
        except ValueError:
            continue

        if word == current_word:
            # Same key: accumulate
            current_count += count
        else:
            # New key: emit the completed previous word
            if current_word is not None:
                # Emit: "<word>\t<total_count>"
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    # Emit the last word after the loop ends
    if current_word is not None:
        print(f"{current_word}\t{current_count}")


if __name__ == "__main__":
    main()
