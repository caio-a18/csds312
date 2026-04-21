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
    current_metric = None
    current_value = 0.0

    for line in sys.stdin:
        line = line.rstrip("\n")

        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue

        metric, value_str = parts

        try:
            value = float(value_str)
        except ValueError:
            continue

        if metric == current_metric:
            current_value += value
        else:
            if current_metric is not None:
                print(f"{current_metric}\t{current_value:.4f}")
            current_metric = metric
            current_value = value

    if current_metric is not None:
        print(f"{current_metric}\t{current_value:.4f}")


if __name__ == "__main__":
    main()
