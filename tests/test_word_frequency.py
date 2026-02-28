"""
test_word_frequency.py — pytest suite for Caio's word-frequency component.

Covers:
  - preprocess.py  (tokenize, remove_stop_words, preprocess_line,
                    strip_gutenberg)
  - mapper.py      (end-to-end stdin→stdout via subprocess)
  - reducer.py     (end-to-end stdin→stdout via subprocess)
  - Full pipeline  (mapper | sort | reducer simulation)

Run:
    pytest tests/test_word_frequency.py -v

Author: Caio Albuquerque
"""

import subprocess
import sys
import os
import textwrap

import pytest

# ---------------------------------------------------------------------------
# Path setup — allow imports from word_frequency/ regardless of where pytest
# is invoked from.
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WF_DIR = os.path.join(REPO_ROOT, "word_frequency")
sys.path.insert(0, WF_DIR)

from preprocess import (  # noqa: E402
    tokenize,
    remove_stop_words,
    preprocess_line,
    strip_gutenberg,
    STOP_WORDS,
)

MAPPER_PATH = os.path.join(WF_DIR, "mapper.py")
REDUCER_PATH = os.path.join(WF_DIR, "reducer.py")


# ===========================================================================
# Helpers
# ===========================================================================

def run_script(script_path: str, stdin_text: str) -> str:
    """Run a Python script with given stdin; return stdout as a string."""
    result = subprocess.run(
        [sys.executable, script_path],
        input=stdin_text,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Script exited with code {result.returncode}: {result.stderr}"
    return result.stdout


def simulate_pipeline(text: str) -> dict:
    """
    Simulate the full local MapReduce pipeline:
        mapper | sort | reducer
    Returns a dict of {word: count}.
    """
    mapper_out = run_script(MAPPER_PATH, text)
    sorted_lines = "\n".join(sorted(mapper_out.splitlines()))
    reducer_out = run_script(REDUCER_PATH, sorted_lines)

    result = {}
    for line in reducer_out.splitlines():
        if "\t" in line:
            word, count = line.split("\t", 1)
            result[word] = int(count)
    return result


# ===========================================================================
# preprocess.py — tokenize()
# ===========================================================================

class TestTokenize:
    def test_basic_lowercasing(self):
        assert tokenize("Hello World") == ["hello", "world"]

    def test_punctuation_removal(self):
        assert tokenize("it's a test!") == ["it's", "a", "test"]

    def test_numbers_kept(self):
        assert "2024" in tokenize("Year 2024")

    def test_empty_string(self):
        assert tokenize("") == []

    def test_only_punctuation(self):
        assert tokenize("!!! ???") == []

    def test_mixed_case(self):
        tokens = tokenize("The QUICK brown FOX")
        assert tokens == ["the", "quick", "brown", "fox"]

    def test_hyphenated_word(self):
        # Hyphens are not in [a-z0-9'], so "well-known" splits into two tokens
        tokens = tokenize("well-known")
        assert "well" in tokens and "known" in tokens

    def test_apostrophe_contraction(self):
        tokens = tokenize("they're")
        assert "they're" in tokens


# ===========================================================================
# preprocess.py — remove_stop_words()
# ===========================================================================

class TestRemoveStopWords:
    def test_removes_common_stopwords(self):
        tokens = ["the", "quick", "brown", "fox"]
        cleaned = remove_stop_words(tokens)
        assert "the" not in cleaned
        assert "quick" in cleaned

    def test_removes_single_char_tokens(self):
        tokens = ["a", "i", "word"]
        cleaned = remove_stop_words(tokens)
        assert "a" not in cleaned
        assert "i" not in cleaned
        assert "word" in cleaned

    def test_empty_input(self):
        assert remove_stop_words([]) == []

    def test_all_stopwords(self):
        tokens = ["the", "a", "and", "or"]
        assert remove_stop_words(tokens) == []

    def test_custom_stop_words(self):
        tokens = ["apple", "banana", "cherry"]
        custom = frozenset(["banana"])
        cleaned = remove_stop_words(tokens, stop_words=custom)
        assert "banana" not in cleaned
        assert "apple" in cleaned
        assert "cherry" in cleaned

    def test_no_stopwords_in_result(self):
        tokens = "the quick brown fox jumps over the lazy dog".split()
        cleaned = remove_stop_words(tokens)
        for word in cleaned:
            assert word not in STOP_WORDS, f"Stop word '{word}' not filtered"


# ===========================================================================
# preprocess.py — preprocess_line()
# ===========================================================================

class TestPreprocessLine:
    def test_full_pipeline(self):
        tokens = preprocess_line("The quick brown fox jumps over the lazy dog")
        assert "the" not in tokens
        assert "quick" in tokens
        assert "fox" in tokens
        assert "lazy" in tokens

    def test_uppercase_input(self):
        tokens = preprocess_line("HELLO WORLD AND STUFF")
        assert "hello" in tokens
        assert "world" in tokens
        assert "and" not in tokens

    def test_empty_line(self):
        assert preprocess_line("") == []

    def test_all_stopwords_line(self):
        assert preprocess_line("the and or but") == []

    def test_punctuation_heavy_line(self):
        tokens = preprocess_line("Hello, world! How are you?")
        assert "hello" in tokens
        assert "world" in tokens

    def test_numbers_survive(self):
        tokens = preprocess_line("Chapter 42 begins here")
        assert "42" in tokens or "chapter" in tokens


# ===========================================================================
# preprocess.py — strip_gutenberg()
# ===========================================================================

class TestStripGutenberg:
    SAMPLE_BOOK = textwrap.dedent("""\
        The Project Gutenberg eBook of Moby Dick

        *** START OF THE PROJECT GUTENBERG EBOOK MOBY DICK ***

        Call me Ishmael. Some years ago...

        *** END OF THE PROJECT GUTENBERG EBOOK MOBY DICK ***

        End notes and license text here.
    """)

    def test_removes_header(self):
        body = strip_gutenberg(self.SAMPLE_BOOK)
        assert "The Project Gutenberg eBook" not in body

    def test_removes_footer(self):
        body = strip_gutenberg(self.SAMPLE_BOOK)
        assert "End notes and license text" not in body

    def test_keeps_body(self):
        body = strip_gutenberg(self.SAMPLE_BOOK)
        assert "Call me Ishmael" in body

    def test_no_sentinels_returns_original(self):
        plain = "Just a regular book without any sentinels."
        assert strip_gutenberg(plain) == plain

    def test_only_start_sentinel(self):
        text = "header\n*** START OF THE PROJECT GUTENBERG EBOOK FOO ***\nbody"
        body = strip_gutenberg(text)
        assert "body" in body
        assert "header" not in body


# ===========================================================================
# mapper.py — subprocess tests
# ===========================================================================

class TestMapper:
    def test_basic_output_format(self):
        output = run_script(MAPPER_PATH, "hello world\n")
        lines = [l for l in output.splitlines() if l]
        for line in lines:
            assert "\t" in line, f"Expected tab-separated output, got: {line}"
            word, count = line.split("\t", 1)
            assert count == "1", f"Mapper should always emit 1, got: {count}"

    def test_stop_words_filtered(self):
        output = run_script(MAPPER_PATH, "the and or but\n")
        # All tokens are stop words — output should be empty
        assert output.strip() == ""

    def test_single_word_emitted(self):
        output = run_script(MAPPER_PATH, "literary\n")
        assert "literary\t1" in output

    def test_multiple_occurrences_emitted_separately(self):
        output = run_script(MAPPER_PATH, "book book book\n")
        lines = [l for l in output.splitlines() if l.startswith("book")]
        assert len(lines) == 3, "Mapper emits one pair per occurrence"

    def test_empty_input(self):
        output = run_script(MAPPER_PATH, "")
        assert output.strip() == ""

    def test_blank_lines_skipped(self):
        output = run_script(MAPPER_PATH, "\n\n\nhello\n\n")
        assert "hello\t1" in output

    def test_gutenberg_footer_stops_processing(self):
        text = "word1\n*** END OF THE PROJECT GUTENBERG EBOOK TEST ***\nword2\n"
        output = run_script(MAPPER_PATH, text)
        assert "word2" not in output

    def test_case_normalization(self):
        output = run_script(MAPPER_PATH, "HADOOP MapReduce\n")
        assert "hadoop\t1" in output
        assert "mapreduce\t1" in output

    def test_punctuation_stripped(self):
        output = run_script(MAPPER_PATH, "hello, world!\n")
        assert "hello\t1" in output
        assert "world\t1" in output

    def test_multiline_input(self):
        text = "literary analysis\nsyntax complexity\n"
        output = run_script(MAPPER_PATH, text)
        words = {l.split("\t")[0] for l in output.splitlines() if "\t" in l}
        assert "literary" in words
        assert "analysis" in words
        assert "syntax" in words
        assert "complexity" in words


# ===========================================================================
# reducer.py — subprocess tests
# ===========================================================================

class TestReducer:
    def test_single_word_single_count(self):
        output = run_script(REDUCER_PATH, "hello\t1\n")
        assert "hello\t1" in output

    def test_single_word_multiple_counts(self):
        stdin = "hello\t1\nhello\t1\nhello\t1\n"
        output = run_script(REDUCER_PATH, stdin)
        assert "hello\t3" in output

    def test_multiple_words(self):
        stdin = "apple\t1\napple\t1\nbanana\t1\n"
        output = run_script(REDUCER_PATH, stdin)
        result = {}
        for line in output.splitlines():
            w, c = line.split("\t")
            result[w] = int(c)
        assert result["apple"] == 2
        assert result["banana"] == 1

    def test_empty_input(self):
        output = run_script(REDUCER_PATH, "")
        assert output.strip() == ""

    def test_malformed_lines_skipped(self):
        stdin = "hello\t1\nBAD LINE\nworld\t1\n"
        output = run_script(REDUCER_PATH, stdin)
        assert "hello\t1" in output
        assert "world\t1" in output
        assert "BAD" not in output

    def test_preserves_all_words(self):
        words = ["alpha", "beta", "gamma", "delta"]
        stdin = "".join(f"{w}\t1\n" for w in words)
        output = run_script(REDUCER_PATH, stdin)
        for w in words:
            assert w in output

    def test_large_counts(self):
        stdin = "".join(f"word\t1\n" for _ in range(1000))
        output = run_script(REDUCER_PATH, stdin)
        assert "word\t1000" in output


# ===========================================================================
# Full pipeline integration tests
# ===========================================================================

class TestFullPipeline:
    def test_famous_opening_sentence(self):
        text = "It was the best of times, it was the worst of times.\n"
        counts = simulate_pipeline(text)
        assert counts.get("best") == 1
        assert counts.get("worst") == 1
        # "times" appears twice
        assert counts.get("times") == 2
        # Stop words should be absent
        assert "it" not in counts
        assert "was" not in counts
        assert "the" not in counts

    def test_repeated_literary_word(self):
        text = "literary literary literary analysis\n"
        counts = simulate_pipeline(text)
        assert counts["literary"] == 3
        assert counts["analysis"] == 1

    def test_empty_book(self):
        counts = simulate_pipeline("")
        assert counts == {}

    def test_all_stopwords_book(self):
        text = "the and or but in on at to for of with is are was\n"
        counts = simulate_pipeline(text)
        assert counts == {}

    def test_mixed_case_merged(self):
        text = "Hadoop hadoop HADOOP\n"
        counts = simulate_pipeline(text)
        assert counts.get("hadoop") == 3

    def test_gutenberg_boilerplate_excluded(self):
        text = textwrap.dedent("""\
            HEADER INFORMATION boilerplate title
            *** START OF THE PROJECT GUTENBERG EBOOK TEST ***
            meaningful literary content here
            *** END OF THE PROJECT GUTENBERG EBOOK TEST ***
            footer license information
        """)
        counts = simulate_pipeline(text)
        # Body words should be present
        assert counts.get("meaningful") == 1
        assert counts.get("literary") == 1
        assert counts.get("content") == 1
        # Footer words should NOT appear
        assert "footer" not in counts
        assert "license" not in counts

    def test_word_count_accuracy(self):
        # "analysis" appears 5 times; "sentiment" 3 times; "complexity" 1 time
        text = " ".join(
            ["analysis"] * 5 + ["sentiment"] * 3 + ["complexity"] * 1
        ) + "\n"
        counts = simulate_pipeline(text)
        assert counts["analysis"] == 5
        assert counts["sentiment"] == 3
        assert counts["complexity"] == 1
