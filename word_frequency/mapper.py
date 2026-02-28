#!/usr/bin/env python3
"""
mapper.py — Hadoop Streaming mapper for word frequency analysis.

Reads plain text from stdin (one line per record), emits tab-separated
(word, 1) pairs to stdout.

Usage (local simulation):
    cat book.txt | python mapper.py

Usage (Hadoop Streaming):
    hadoop jar $STREAMING_JAR \\
        -files mapper.py,reducer.py,preprocess.py,stopwords.txt \\
        -mapper  "python mapper.py" \\
        -reducer "python reducer.py" \\
        -input   hdfs:///books/input \\
        -output  hdfs:///books/output/word_freq

Author: Caio Albuquerque
"""

import sys
import os

# Allow importing preprocess.py when running inside Hadoop (files are
# distributed to the task working directory).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from preprocess import preprocess_line

# Gutenberg sentinel detection (line-level fast check used in streaming mode;
# full strip_gutenberg() is for batch preprocessing of whole files).
_IN_BODY = True  # streaming mode: assume body unless sentinel seen
_SEEN_START = False


def main():
    global _IN_BODY, _SEEN_START

    for line in sys.stdin:
        line = line.rstrip("\n")

        # Skip blank lines
        if not line.strip():
            continue

        # Gutenberg header: skip until we see the START sentinel
        if "*** START OF" in line.upper() and "GUTENBERG" in line.upper():
            _SEEN_START = True
            _IN_BODY = True
            continue

        # Gutenberg footer: stop processing
        if "*** END OF" in line.upper() and "GUTENBERG" in line.upper():
            _IN_BODY = False
            continue

        # If we haven't seen a START sentinel yet but also no END sentinel,
        # process the line (handles books without Gutenberg markup).
        if not _IN_BODY:
            continue

        tokens = preprocess_line(line)
        for token in tokens:
            print(f"{token}\t1")


if __name__ == "__main__":
    main()
