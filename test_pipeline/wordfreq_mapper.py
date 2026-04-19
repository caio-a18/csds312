#!/usr/bin/env python3
"""
Hadoop Streaming Mapper: Word Frequency
=======================================
Hadoop Streaming key-value format:
  - Mapper reads lines from stdin (one line = one record from HDFS split)
  - Mapper emits: <key>\t<value> on stdout
  - Here: key = word (str), value = 1 (int)
  - The framework sorts all emitted pairs by key before passing to reducer

Gutenberg text handling:
  - Skips the standard Project Gutenberg header (before *** START OF... ***)
  - Stops at the Project Gutenberg footer (*** END OF... ***)
"""

import sys
import re
import string

# ── Stop words ────────────────────────────────────────────────────────────────
# Hardcoded to avoid NLTK dependency. Common English function words that add
# noise to word-frequency analysis.
STOP_WORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "was", "are", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "shall", "not", "no", "nor",
    "so", "yet", "both", "either", "as", "if", "though", "while",
    "that", "this", "these", "those", "it", "its", "i", "he", "she",
    "we", "they", "you", "my", "his", "her", "our", "their", "your",
    "me", "him", "us", "them", "who", "which", "what", "when", "where",
}

# Gutenberg boundary markers
GUTENBERG_START = "*** START OF THE PROJECT GUTENBERG EBOOK"
GUTENBERG_END   = "*** END OF THE PROJECT GUTENBERG EBOOK"

# Regex: keep only alphabetic characters (removes punctuation/digits)
WORD_RE = re.compile(r"[a-z]+")


def tokenize(line):
    """Lowercase and extract alphabetic tokens, filtering stop words."""
    return [w for w in WORD_RE.findall(line.lower()) if w not in STOP_WORDS and len(w) > 1]


def main():
    in_content = False  # True once we pass the Gutenberg header

    for line in sys.stdin:
        line = line.rstrip("\n")

        # Detect start of actual book content
        if not in_content:
            if GUTENBERG_START in line:
                in_content = True
            continue  # skip header lines

        # Detect end of actual book content
        if GUTENBERG_END in line:
            break  # stop processing

        # Emit one key-value pair per word: "<word>\t1"
        # The tab character (\t) is the Hadoop Streaming field separator.
        for word in tokenize(line):
            print(f"{word}\t1")


if __name__ == "__main__":
    main()
