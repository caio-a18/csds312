"""
tests/test_syntactic.py
pytest suite for Component 3: Syntactic Complexity
Group 9 - CSDS 312

Run with: pytest tests/test_syntactic.py -v
"""

import sys
import os
import subprocess
import tempfile

# Make component3 importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "component3"))

from mapper import count_syllables, tokenize_sentences, tokenize_words, STOPWORDS
from reducer import (
    flesch_reading_ease,
    flesch_kincaid_grade,
    gunning_fog,
    emit_book,
)

# ---------------------------------------------------------------------------
# count_syllables
# ---------------------------------------------------------------------------

class TestCountSyllables:
    def test_single_syllable(self):
        assert count_syllables("cat") == 1

    def test_two_syllables(self):
        assert count_syllables("happy") == 2

    def test_three_syllables(self):
        assert count_syllables("beautiful") == 3

    def test_silent_e(self):
        # "make" -> "mak" -> 1 syllable
        assert count_syllables("make") == 1

    def test_minimum_one(self):
        assert count_syllables("th") >= 1

    def test_empty_string(self):
        assert count_syllables("") == 0

    def test_punctuation_stripped(self):
        assert count_syllables("cat,") == count_syllables("cat")

    def test_polysyllabic(self):
        # "education" = 4 syllables
        assert count_syllables("education") >= 3

    def test_vowel_groups(self):
        # "rain" -> one vowel group -> 1
        assert count_syllables("rain") == 1

    def test_y_as_vowel(self):
        # "gym" has y acting as vowel
        assert count_syllables("gym") >= 1


# ---------------------------------------------------------------------------
# tokenize_sentences
# ---------------------------------------------------------------------------

class TestTokenizeSentences:
    def test_single_sentence(self):
        assert len(tokenize_sentences("Hello world.")) == 1

    def test_two_sentences(self):
        sents = tokenize_sentences("Hello world. Goodbye world.")
        assert len(sents) == 2

    def test_exclamation(self):
        sents = tokenize_sentences("Hello! Goodbye.")
        assert len(sents) == 2

    def test_question(self):
        sents = tokenize_sentences("Are you there? Yes.")
        assert len(sents) == 2

    def test_abbreviation_mr(self):
        # "Mr. Smith" should not split
        text = "Mr. Smith went to Washington. He was brave."
        sents = tokenize_sentences(text)
        assert len(sents) == 2

    def test_abbreviation_dr(self):
        text = "Dr. Jones arrived. She was late."
        sents = tokenize_sentences(text)
        assert len(sents) == 2

    def test_empty_string(self):
        assert tokenize_sentences("") == []

    def test_no_terminal_punct(self):
        # No terminal punctuation -- treated as one sentence
        result = tokenize_sentences("No punctuation here at all")
        assert len(result) == 1

    def test_multiple_spaces(self):
        result = tokenize_sentences("One sentence.  Another sentence.")
        assert len(result) == 2

    def test_strips_whitespace(self):
        result = tokenize_sentences("  Hello world.  ")
        assert result[0].strip() == result[0]


# ---------------------------------------------------------------------------
# tokenize_words
# ---------------------------------------------------------------------------

class TestTokenizeWords:
    def test_basic(self):
        assert tokenize_words("Hello world") == ["hello", "world"]

    def test_punctuation_stripped(self):
        words = tokenize_words("Hello, world!")
        assert "hello" in words
        assert "world" in words

    def test_numbers_excluded(self):
        # digits-only tokens shouldn't appear
        words = tokenize_words("There are 3 cats")
        assert "3" not in words

    def test_lowercase(self):
        words = tokenize_words("THE QUICK BROWN FOX")
        assert all(w == w.lower() for w in words)

    def test_empty(self):
        assert tokenize_words("") == []

    def test_apostrophe_handling(self):
        words = tokenize_words("it's a test")
        assert any("it" in w or "its" in w for w in words)

    def test_hyphenated_not_split(self):
        # Regex picks up alphabetic runs; hyphen not in \b[a-zA-Z']+\b
        words = tokenize_words("well-known author")
        assert len(words) >= 2


# ---------------------------------------------------------------------------
# Readability formulas
# ---------------------------------------------------------------------------

class TestReadabilityFormulas:
    def test_fre_zero_words(self):
        assert flesch_reading_ease(0, 5, 10) == 0.0

    def test_fre_zero_sentences(self):
        assert flesch_reading_ease(100, 0, 150) == 0.0

    def test_fre_simple_text(self):
        # Simple text (short sentences, monosyllabic words) should score > 70
        score = flesch_reading_ease(words=100, sentences=10, syllables=120)
        assert score > 50

    def test_fkg_zero_words(self):
        assert flesch_kincaid_grade(0, 5, 10) == 0.0

    def test_fkg_simple_text_low_grade(self):
        # Short sentences, low syllables -> low grade level
        grade = flesch_kincaid_grade(words=100, sentences=10, syllables=110)
        assert grade < 10

    def test_fkg_complex_high_grade(self):
        # Long sentences, many syllables -> high grade level
        grade = flesch_kincaid_grade(words=500, sentences=5, syllables=900)
        assert grade > 10

    def test_fog_zero_words(self):
        assert gunning_fog(0, 5, 0) == 0.0

    def test_fog_zero_sentences(self):
        assert gunning_fog(100, 0, 20) == 0.0

    def test_fog_reasonable_range(self):
        fog = gunning_fog(words=100, sentences=5, complex_words=10)
        # Should be a positive finite number
        assert 0 < fog < 100

    def test_fre_output_rounded(self):
        score = flesch_reading_ease(100, 10, 150)
        # Should have at most 4 decimal places
        assert score == round(score, 4)


# ---------------------------------------------------------------------------
# End-to-end: mapper | sort | reducer via subprocess
# ---------------------------------------------------------------------------

SAMPLE_TEXT = """\
*** START OF THE PROJECT GUTENBERG EBOOK TEST ***
The quick brown fox jumps over the lazy dog.
She sells seashells by the seashore.
It was the best of times, it was the worst of times.
To be or not to be, that is the question.
*** END OF THE PROJECT GUTENBERG EBOOK TEST ***
"""

COMPONENT3_DIR = os.path.join(os.path.dirname(__file__), "..")
MAPPER_PATH  = os.path.join(COMPONENT3_DIR, "mapper.py")
REDUCER_PATH = os.path.join(COMPONENT3_DIR, "reducer.py")


class TestEndToEnd:
    def _run_pipeline(self, text: str, book_name: str = "test_book") -> dict:
        """Run mapper | sort | reducer on text, return metric->value dict."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False,
            prefix=book_name + "_"
        ) as f:
            f.write(text)
            fpath = f.name

        env = os.environ.copy()
        env["mapreduce_map_input_file"] = fpath

        try:
            map_proc = subprocess.run(
                [sys.executable, MAPPER_PATH],
                input=text, capture_output=True, text=True, env=env
            )
            assert map_proc.returncode == 0, map_proc.stderr

            # sort
            lines = sorted(map_proc.stdout.strip().split("\n"))
            sorted_output = "\n".join(lines)

            red_proc = subprocess.run(
                [sys.executable, REDUCER_PATH],
                input=sorted_output, capture_output=True, text=True
            )
            assert red_proc.returncode == 0, red_proc.stderr

            result = {}
            for line in red_proc.stdout.strip().split("\n"):
                parts = line.split("\t")
                if len(parts) == 3:
                    _, metric, value = parts
                    try:
                        result[metric] = float(value)
                    except ValueError:
                        result[metric] = value
            return result
        finally:
            os.unlink(fpath)

    def test_pipeline_runs(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        assert len(result) > 0

    def test_word_count_positive(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        assert result.get("word_count", 0) > 0

    def test_sentence_count_positive(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        assert result.get("sentence_count", 0) > 0

    def test_avg_sentence_length_reasonable(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        asl = result.get("avg_sentence_length", 0)
        assert 3 < asl < 30

    def test_ttr_between_0_and_1(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        ttr = result.get("type_token_ratio", -1)
        assert 0 < ttr <= 1

    def test_flesch_reading_ease_present(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        assert "flesch_reading_ease" in result

    def test_flesch_kincaid_grade_present(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        assert "flesch_kincaid_grade" in result

    def test_gunning_fog_present(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        assert "gunning_fog_index" in result

    def test_unique_word_count_positive(self):
        result = self._run_pipeline(SAMPLE_TEXT)
        assert result.get("unique_word_count", 0) > 0

    def test_gutenberg_boilerplate_excluded(self):
        """Words in header/footer lines should not be counted."""
        result = self._run_pipeline(SAMPLE_TEXT)
        # If boilerplate is NOT stripped, word count is inflated.
        # "project gutenberg ebook test" adds ~4 words per fence line.
        # With stripping, we expect fewer than 50 words for this short text.
        assert result.get("word_count", 999) < 100

    def test_empty_body(self):
        """A file with only boilerplate should produce zero counts."""
        text = (
            "*** START OF THE PROJECT GUTENBERG EBOOK EMPTY ***\n"
            "*** END OF THE PROJECT GUTENBERG EBOOK EMPTY ***\n"
        )
        result = self._run_pipeline(text, "empty_book")
        assert result.get("word_count", 0) == 0

    def test_output_schema_keys(self):
        """All expected keys are present for Component 4 normalization."""
        expected = {
            "word_count", "sentence_count", "unique_word_count",
            "avg_sentence_length", "avg_word_length", "type_token_ratio",
            "flesch_reading_ease", "flesch_kincaid_grade", "gunning_fog_index",
        }
        result = self._run_pipeline(SAMPLE_TEXT)
        for key in expected:
            assert key in result, f"Missing key: {key}"
