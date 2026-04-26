#!/usr/bin/env python3
"""
Component 3: Syntactic Complexity Reducer
Group 9 - CSDS 312

Receives sorted key-value pairs from the mapper:
  book_id\tmetric\tvalue

For each book, aggregates totals across all mapper shards, then computes:
  - avg_sentence_length     : words per sentence
  - avg_word_length         : chars per word
  - type_token_ratio        : unique_words / total_words  (vocabulary richness)
  - flesch_reading_ease     : classic Flesch formula
  - flesch_kincaid_grade    : FK grade-level formula
  - gunning_fog_index       : Gunning Fog readability

Output format (tab-separated, one metric per line):
  book_id\tmetric\tvalue

This matches the schema expected by Component 4 (normalization).
"""

import sys


def flesch_reading_ease(words: int, sentences: int, syllables: int) -> float:
    """
    Flesch Reading Ease score.
    Higher = easier to read. Range roughly 0-100.
    206.835 - 1.015*(words/sentences) - 84.6*(syllables/words)
    """
    if sentences == 0 or words == 0:
        return 0.0
    asl = words / sentences          # average sentence length
    asw = syllables / words          # average syllables per word
    return round(206.835 - 1.015 * asl - 84.6 * asw, 4)


def flesch_kincaid_grade(words: int, sentences: int, syllables: int) -> float:
    """
    Flesch-Kincaid Grade Level.
    Estimates U.S. school grade needed to comprehend the text.
    0.39*(words/sentences) + 11.8*(syllables/words) - 15.59
    """
    if sentences == 0 or words == 0:
        return 0.0
    asl = words / sentences
    asw = syllables / words
    return round(0.39 * asl + 11.8 * asw - 15.59, 4)


def gunning_fog(words: int, sentences: int, complex_words: int) -> float:
    """
    Gunning Fog Index.
    0.4 * ( (words/sentences) + 100*(complex_words/words) )
    """
    if sentences == 0 or words == 0:
        return 0.0
    asl = words / sentences
    pct_complex = 100 * complex_words / words
    return round(0.4 * (asl + pct_complex), 4)


def emit_book(book_id: str, metrics: dict, unique_words: set) -> None:
    """Compute derived metrics and print all output records for one book."""
    words      = metrics.get("word_count", 0)
    sentences  = metrics.get("sentence_count", 0)
    chars      = metrics.get("char_count", 0)
    syllables  = metrics.get("syllable_count", 0)
    complex_w  = metrics.get("complex_word_count", 0)
    n_unique   = len(unique_words)

    avg_sentence_len  = round(words / sentences, 4) if sentences > 0 else 0.0
    avg_word_len      = round(chars / words, 4)     if words > 0 else 0.0
    ttr               = round(n_unique / words, 4)  if words > 0 else 0.0

    fre  = flesch_reading_ease(words, sentences, syllables)
    fkg  = flesch_kincaid_grade(words, sentences, syllables)
    fog  = gunning_fog(words, sentences, complex_w)

    records = [
        ("word_count",            words),
        ("sentence_count",        sentences),
        ("unique_word_count",     n_unique),
        ("avg_sentence_length",   avg_sentence_len),
        ("avg_word_length",       avg_word_len),
        ("type_token_ratio",      ttr),
        ("flesch_reading_ease",   fre),
        ("flesch_kincaid_grade",  fkg),
        ("gunning_fog_index",     fog),
    ]
    for metric, value in records:
        print(f"{book_id}\t{metric}\t{value}")


def main():
    current_book = None
    metrics      = {}
    unique_words = set()

    for line in sys.stdin:
        line = line.rstrip("\n")
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue

        book_id, metric, value = parts

        # New book block — emit previous book
        if book_id != current_book:
            if current_book is not None:
                emit_book(current_book, metrics, unique_words)
            current_book = book_id
            metrics      = {}
            unique_words = set()

        if metric == "unique_words":
            # Union word sets across mapper shards
            if value.strip():
                unique_words.update(value.strip().split())
        else:
            try:
                metrics[metric] = metrics.get(metric, 0) + int(value)
            except ValueError:
                pass  # skip malformed values

    # Emit final book
    if current_book is not None:
        emit_book(current_book, metrics, unique_words)


if __name__ == "__main__":
    main()
