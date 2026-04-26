#!/usr/bin/env python3
"""
Component 3: Syntactic Complexity Mapper
Group 9 - CSDS 312

Reads raw book text from stdin (Hadoop Streaming).
Each line is expected to be prefixed with the filename via
Hadoop's mapreduce.map.input.file env variable, or passed as
tab-separated <filename>\t<text_line> by the cluster script.

For each book, emits intermediate key-value pairs:
  book_id\tsentence_count\t<n>
  book_id\tword_count\t<n>
  book_id\tchar_count\t<n>          (chars in words, for avg word length)
  book_id\tunique_words\t<space-sep word list>
  book_id\tsyllable_count\t<n>      (for Flesch-Kincaid)
  book_id\tcomplex_words\t<n>       (words with 3+ syllables, for Gunning Fog)

Output is tab-separated: book_id\tmetric\tvalue
"""

import sys
import os
import re
import string

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def count_syllables(word: str) -> int:
    """
    Estimate syllable count using vowel-group heuristic.
    Accurate enough for readability scoring without NLTK dependency.
    """
    word = word.lower().strip(string.punctuation)
    if not word:
        return 0

    # Handle common silent-e endings
    if word.endswith("e") and len(word) > 2:
        word = word[:-1]

    vowels = "aeiouy"
    count = 0
    prev_vowel = False
    for ch in word:
        is_vowel = ch in vowels
        if is_vowel and not prev_vowel:
            count += 1
        prev_vowel = is_vowel

    return max(1, count)


def tokenize_sentences(text: str) -> list:
    """
    Split text into sentences on . ! ? followed by whitespace or end.
    Handles common abbreviations (Mr., Dr., etc.) by not splitting on them.
    """
    # Temporarily protect common abbreviations
    abbrevs = [
        "Mr.", "Mrs.", "Ms.", "Dr.", "Prof.", "Sr.", "Jr.",
        "vs.", "etc.", "e.g.", "i.e.", "U.S.", "U.K."
    ]
    placeholder = text
    for ab in abbrevs:
        placeholder = placeholder.replace(ab, ab.replace(".", "<DOT>"))

    sentences = re.split(r"[.!?]+\s+|[.!?]+$", placeholder)
    sentences = [s.strip() for s in sentences if s.strip()]
    return sentences


def tokenize_words(text: str) -> list:
    """Lowercase, strip punctuation, return non-empty tokens."""
    tokens = re.findall(r"\b[a-zA-Z']+\b", text.lower())
    return [t.strip("'") for t in tokens if t.strip("'")]


STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
    "for", "of", "with", "by", "from", "is", "was", "are", "were",
    "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall",
    "that", "this", "these", "those", "it", "its", "as", "if",
    "not", "no", "nor", "so", "yet", "both", "either", "neither",
    "i", "he", "she", "we", "they", "you", "me", "him", "her",
    "us", "them", "my", "his", "our", "your", "their", "up",
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Hadoop sets this env var to the current input file path
    input_file = os.environ.get("mapreduce_map_input_file", "")
    if not input_file:
        input_file = os.environ.get("map_input_file", "unknown")

    # Derive a clean book_id from the filename (strip path and extension)
    book_id = os.path.splitext(os.path.basename(input_file))[0]
    # Sanitise: remove whitespace
    book_id = re.sub(r"\s+", "_", book_id.strip()) or "unknown"

    # Accumulators
    sentence_count = 0
    word_count = 0
    char_count = 0        # total chars across all words (for avg word length)
    syllable_count = 0
    complex_word_count = 0
    unique_words = set()

    in_header = True      # skip Project Gutenberg boilerplate at top
    header_end_pattern = re.compile(
        r"\*\*\* start of (the|this) project gutenberg", re.IGNORECASE
    )
    footer_start_pattern = re.compile(
        r"\*\*\* end of (the|this) project gutenberg", re.IGNORECASE
    )
    in_body = False

    for line in sys.stdin:
        # Strip Gutenberg header/footer
        if not in_body:
            if header_end_pattern.search(line):
                in_body = True
            continue
        if footer_start_pattern.search(line):
            break

        line = line.rstrip("\n")
        if not line.strip():
            continue

        # Sentence-level stats
        sentences = tokenize_sentences(line)
        sentence_count += len(sentences)

        # Word-level stats
        words = tokenize_words(line)
        for w in words:
            word_count += 1
            char_count += len(w)
            syllables = count_syllables(w)
            syllable_count += syllables
            if syllables >= 3:
                complex_word_count += 1
            if w not in STOPWORDS and len(w) > 1:
                unique_words.add(w)

    # Emit one record per metric; reducer aggregates across mappers for same book
    print(f"{book_id}\tsentence_count\t{sentence_count}")
    print(f"{book_id}\tword_count\t{word_count}")
    print(f"{book_id}\tchar_count\t{char_count}")
    print(f"{book_id}\tsyllable_count\t{syllable_count}")
    print(f"{book_id}\tcomplex_word_count\t{complex_word_count}")
    # Emit unique word set as space-separated string so reducer can union them
    print(f"{book_id}\tunique_words\t{' '.join(unique_words)}")


if __name__ == "__main__":
    main()
