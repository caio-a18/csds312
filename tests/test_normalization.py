"""
test_normalization.py — pytest suite for Component 4: Intermediate Data Normalization.

Covers:
  - mapper.py  (component detection, book_id extraction, all component types,
                malformed/empty input, env var overrides)
  - reducer.py (per-book aggregation, derived stats, multi-book, partial records,
                malformed lines, empty input)
  - Full pipeline (mapper | sort | reducer simulation)

Run:
    pytest tests/test_normalization.py -v

Author: Zeynep Bastas
"""

import json
import os
import subprocess
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NORM_DIR = os.path.join(REPO_ROOT, "normalization")
sys.path.insert(0, NORM_DIR)

from mapper import detect_component, extract_book_id, parse_kv_line, parse_metadata_line  # noqa: E402

MAPPER_PATH = os.path.join(NORM_DIR, "mapper.py")
REDUCER_PATH = os.path.join(NORM_DIR, "reducer.py")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_script(script_path: str, stdin_text: str, env_overrides: dict = None) -> str:
    env = os.environ.copy()
    env.pop("NORM_COMPONENT", None)
    env.pop("NORM_BOOK_ID", None)
    env.pop("mapreduce_map_input_file", None)
    if env_overrides:
        env.update(env_overrides)
    result = subprocess.run(
        [sys.executable, script_path],
        input=stdin_text,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    return result.stdout


def run_pipeline(mapper_input: str, env_overrides: dict = None) -> str:
    mapped = run_script(MAPPER_PATH, mapper_input, env_overrides)
    if not mapped.strip():
        return ""
    sorted_lines = "\n".join(sorted(mapped.strip().splitlines())) + "\n"
    return run_script(REDUCER_PATH, sorted_lines)


# ===========================================================================
# detect_component
# ===========================================================================

class TestDetectComponent:
    def test_wordfreq_directory(self):
        assert detect_component("/output/wordfreq/part-00000") == "wordfreq"

    def test_word_freq_underscore(self):
        assert detect_component("/output/word_freq/1342/part-00000") == "wordfreq"

    def test_word_freq_dash(self):
        assert detect_component("/output/word-freq/part-00000") == "wordfreq"

    def test_component1_keyword(self):
        assert detect_component("/output/component1/part-00000") == "wordfreq"

    def test_comp1_abbreviation(self):
        assert detect_component("/output/comp1/part-00000") == "wordfreq"

    def test_sentiment_directory(self):
        assert detect_component("/output/sentiment/part-00000") == "sentiment"

    def test_component2_keyword(self):
        assert detect_component("/output/component2/part-00000") == "sentiment"

    def test_syntactic_directory(self):
        assert detect_component("/output/syntactic/part-00000") == "syntactic"

    def test_syntax_abbreviation(self):
        assert detect_component("/output/syntax/part-00000") == "syntactic"

    def test_component3_keyword(self):
        assert detect_component("/output/component3/part-00000") == "syntactic"

    def test_metadata_directory(self):
        assert detect_component("/data/metadata/books.csv") == "metadata"

    def test_unknown_path_returns_unknown(self):
        assert detect_component("/output/misc/part-00000") == "unknown"

    def test_empty_path_returns_unknown(self):
        assert detect_component("") == "unknown"

    def test_case_insensitive(self):
        assert detect_component("/OUTPUT/WORDFREQ/PART-00000") == "wordfreq"


# ===========================================================================
# extract_book_id
# ===========================================================================

class TestExtractBookId:
    def test_id_in_filename(self):
        assert extract_book_id("/output/wordfreq/1342_output.txt") == "1342"

    def test_id_as_directory_segment(self):
        assert extract_book_id("/output/wordfreq/1342/part-00000") == "1342"

    def test_no_digits_returns_unknown(self):
        assert extract_book_id("/output/wordfreq/part-output") == "unknown"

    def test_multiple_numbers_picks_filename_first(self):
        assert extract_book_id("/output/wordfreq/84_frankenstein.txt") == "84"

    def test_empty_path_returns_unknown(self):
        assert extract_book_id("") == "unknown"


# ===========================================================================
# parse_kv_line
# ===========================================================================

class TestParseKvLine:
    def test_valid_word_count(self):
        assert parse_kv_line("the\t4507") == ("the", "4507")

    def test_value_contains_tab_only_splits_once(self):
        assert parse_kv_line("key\tval\textra") == ("key", "val\textra")

    def test_no_tab_returns_none(self):
        assert parse_kv_line("novalue") is None

    def test_empty_string_returns_none(self):
        assert parse_kv_line("") is None

    def test_float_value(self):
        assert parse_kv_line("score\t0.23") == ("score", "0.23")


# ===========================================================================
# parse_metadata_line
# ===========================================================================

class TestParseMetadataLine:
    def test_valid_bestseller(self):
        result = parse_metadata_line("1342,Pride and Prejudice,Jane Austen,1,1342_file.txt")
        assert result == {
            "book_id": "1342",
            "title": "Pride and Prejudice",
            "author": "Jane Austen",
            "is_bestseller": "1",
        }

    def test_valid_non_bestseller(self):
        result = parse_metadata_line("84,Frankenstein,Mary Shelley,0,84_file.txt")
        assert result["is_bestseller"] == "0"

    def test_too_few_fields_returns_none(self):
        assert parse_metadata_line("1342,title,author") is None

    def test_title_with_comma_preserved(self):
        result = parse_metadata_line("100,War and Peace,Leo Tolstoy,0,100_file.txt")
        assert result["title"] == "War and Peace"


# ===========================================================================
# Mapper subprocess
# ===========================================================================

class TestMapper:
    def test_wordfreq_emits_tagged_lines(self):
        out = run_script(MAPPER_PATH, "the\t4507\nlove\t123\n",
                         {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"})
        lines = out.strip().splitlines()
        assert len(lines) == 2
        assert lines[0] == "1342\twordfreq\tthe\t4507"
        assert lines[1] == "1342\twordfreq\tlove\t123"

    def test_wordfreq_skips_non_integer_count(self):
        out = run_script(MAPPER_PATH, "the\tnot_a_number\n",
                         {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"})
        assert out.strip() == ""

    def test_wordfreq_skips_missing_tab(self):
        out = run_script(MAPPER_PATH, "badline\n",
                         {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"})
        assert out.strip() == ""

    def test_sentiment_emits_tagged_line(self):
        out = run_script(MAPPER_PATH, "avg_score\t0.23\n",
                         {"NORM_COMPONENT": "sentiment", "NORM_BOOK_ID": "1342"})
        assert out.strip() == "1342\tsentiment\tavg_score\t0.23"

    def test_sentiment_skips_non_float(self):
        out = run_script(MAPPER_PATH, "score\tnot_a_float\n",
                         {"NORM_COMPONENT": "sentiment", "NORM_BOOK_ID": "1342"})
        assert out.strip() == ""

    def test_syntactic_emits_multiple_metrics(self):
        out = run_script(MAPPER_PATH,
                         "avg_sentence_length\t18.4\nvocab_richness\t0.42\nreadability_score\t65.2\n",
                         {"NORM_COMPONENT": "syntactic", "NORM_BOOK_ID": "1342"})
        lines = out.strip().splitlines()
        assert len(lines) == 3
        assert "1342\tsyntactic\tavg_sentence_length\t18.4" in lines
        assert "1342\tsyntactic\tvocab_richness\t0.42" in lines
        assert "1342\tsyntactic\treadability_score\t65.2" in lines

    def test_syntactic_skips_non_float(self):
        out = run_script(MAPPER_PATH, "avg_sentence_length\tbad\n",
                         {"NORM_COMPONENT": "syntactic", "NORM_BOOK_ID": "1342"})
        assert out.strip() == ""

    def test_metadata_emits_three_fields(self):
        out = run_script(MAPPER_PATH,
                         "book_id,title,author,is_bestseller,filename\n"
                         "1342,Pride and Prejudice,Jane Austen,1,file.txt\n",
                         {"NORM_COMPONENT": "metadata"})
        lines = out.strip().splitlines()
        assert any("title\tPride and Prejudice" in l for l in lines)
        assert any("author\tJane Austen" in l for l in lines)
        assert any("is_bestseller\t1" in l for l in lines)

    def test_metadata_skips_header_row(self):
        out = run_script(MAPPER_PATH,
                         "book_id,title,author,is_bestseller,filename\n",
                         {"NORM_COMPONENT": "metadata"})
        assert out.strip() == ""

    def test_metadata_uses_embedded_book_id(self):
        out = run_script(MAPPER_PATH,
                         "84,Frankenstein,Mary Shelley,0,84_file.txt\n",
                         {"NORM_COMPONENT": "metadata"})
        lines = out.strip().splitlines()
        assert all(l.startswith("84\t") for l in lines)

    def test_unknown_component_emits_nothing(self):
        out = run_script(MAPPER_PATH, "some\tdata\n",
                         {"NORM_COMPONENT": "unknown", "NORM_BOOK_ID": "1342"})
        assert out.strip() == ""

    def test_empty_lines_are_skipped(self):
        out = run_script(MAPPER_PATH, "\n\nthe\t4507\n\n",
                         {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"})
        assert len(out.strip().splitlines()) == 1

    def test_book_id_appears_in_output(self):
        out = run_script(MAPPER_PATH, "love\t55\n",
                         {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "84"})
        assert out.strip().startswith("84\t")

    def test_component_detection_via_env_var_path(self):
        out = run_script(MAPPER_PATH, "the\t10\n",
                         {"mapreduce_map_input_file": "/output/wordfreq/1342/part-00000"})
        assert out.strip() == "1342\twordfreq\tthe\t10"


# ===========================================================================
# Reducer subprocess
# ===========================================================================

class TestReducer:
    def _parse(self, output: str) -> dict:
        book_id, json_str = output.strip().split("\t", 1)
        return json.loads(json_str)

    def test_single_book_wordfreq_total(self):
        inp = "1342\twordfreq\tthe\t100\n1342\twordfreq\tlove\t50\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["total_word_count"] == 150

    def test_single_book_unique_word_count(self):
        inp = "1342\twordfreq\tthe\t100\n1342\twordfreq\tlove\t50\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["unique_word_count"] == 2

    def test_top_words_capped_at_20(self):
        words = [(f"word{i}", 100 - i) for i in range(25)]
        inp = "".join(f"1342\twordfreq\t{w}\t{c}\n" for w, c in words)
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert len(record["top_words"]) == 20

    def test_top_words_sorted_descending(self):
        words = [(f"word{i}", 100 - i) for i in range(25)]
        inp = "".join(f"1342\twordfreq\t{w}\t{c}\n" for w, c in words)
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["top_words"][0][0] == "word0"
        assert record["top_words"][0][1] == 100

    def test_word_frequencies_dict_removed_from_output(self):
        inp = "1342\twordfreq\tthe\t100\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert "word_frequencies" not in record

    def test_sentiment_fields_preserved(self):
        inp = "1342\tsentiment\tavg_score\t0.23\n1342\tsentiment\tvariance\t0.05\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["sentiment"]["avg_score"] == pytest.approx(0.23)
        assert record["sentiment"]["variance"] == pytest.approx(0.05)

    def test_syntactic_fields_preserved(self):
        inp = (
            "1342\tsyntactic\tavg_sentence_length\t18.4\n"
            "1342\tsyntactic\tvocab_richness\t0.42\n"
            "1342\tsyntactic\treadability_score\t65.2\n"
        )
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["syntactic"]["avg_sentence_length"] == pytest.approx(18.4)
        assert record["syntactic"]["vocab_richness"] == pytest.approx(0.42)
        assert record["syntactic"]["readability_score"] == pytest.approx(65.2)

    def test_metadata_fields_preserved(self):
        inp = (
            "1342\tmetadata\ttitle\tPride and Prejudice\n"
            "1342\tmetadata\tauthor\tJane Austen\n"
            "1342\tmetadata\tis_bestseller\t1\n"
        )
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["metadata"]["title"] == "Pride and Prejudice"
        assert record["metadata"]["is_bestseller"] == "1"

    def test_multiple_books_emitted_as_separate_lines(self):
        inp = "1342\twordfreq\tthe\t100\n84\twordfreq\tlife\t200\n"
        inp_sorted = "\n".join(sorted(inp.strip().splitlines())) + "\n"
        out = run_script(REDUCER_PATH, inp_sorted)
        lines = out.strip().splitlines()
        assert len(lines) == 2
        ids = {l.split("\t")[0] for l in lines}
        assert {"1342", "84"} == ids

    def test_malformed_line_skipped_gracefully(self):
        inp = "1342\twordfreq\tthe\t100\nbad line\n1342\twordfreq\tlove\t50\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["total_word_count"] == 150

    def test_empty_input_produces_no_output(self):
        out = run_script(REDUCER_PATH, "")
        assert out.strip() == ""

    def test_partial_record_no_wordfreq(self):
        inp = "1342\tsentiment\tavg_score\t0.5\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["sentiment"]["avg_score"] == pytest.approx(0.5)
        assert "total_word_count" not in record

    def test_invalid_wordfreq_value_skipped(self):
        inp = "1342\twordfreq\tthe\tnot_int\n1342\twordfreq\tlove\t50\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["total_word_count"] == 50

    def test_book_id_in_output_record(self):
        inp = "1342\twordfreq\tthe\t10\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["book_id"] == "1342"

    def test_empty_lines_in_input_skipped(self):
        inp = "1342\twordfreq\tthe\t10\n\n\n1342\twordfreq\tlove\t5\n"
        record = self._parse(run_script(REDUCER_PATH, inp))
        assert record["total_word_count"] == 15


# ===========================================================================
# Full pipeline
# ===========================================================================

class TestPipeline:
    def test_wordfreq_end_to_end(self):
        result = run_pipeline(
            "the\t4507\nlove\t123\n",
            {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"},
        )
        record = json.loads(result.strip().split("\t", 1)[1])
        assert record["book_id"] == "1342"
        assert record["total_word_count"] == 4630
        assert record["top_words"][0] == ["the", 4507]

    def test_all_components_combined(self):
        wf_mapped = run_script(MAPPER_PATH, "the\t100\nlove\t50\n",
                               {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"})
        sent_mapped = run_script(MAPPER_PATH, "avg_score\t0.3\n",
                                 {"NORM_COMPONENT": "sentiment", "NORM_BOOK_ID": "1342"})
        synt_mapped = run_script(MAPPER_PATH,
                                 "avg_sentence_length\t18.0\nvocab_richness\t0.5\n",
                                 {"NORM_COMPONENT": "syntactic", "NORM_BOOK_ID": "1342"})

        combined = wf_mapped + sent_mapped + synt_mapped
        sorted_lines = "\n".join(sorted(combined.strip().splitlines())) + "\n"
        result = run_script(REDUCER_PATH, sorted_lines)

        record = json.loads(result.strip().split("\t", 1)[1])
        assert record["total_word_count"] == 150
        assert record["sentiment"]["avg_score"] == pytest.approx(0.3)
        assert record["syntactic"]["avg_sentence_length"] == pytest.approx(18.0)
        assert record["syntactic"]["vocab_richness"] == pytest.approx(0.5)

    def test_two_books_pipeline(self):
        wf1 = run_script(MAPPER_PATH, "the\t100\n",
                         {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"})
        wf2 = run_script(MAPPER_PATH, "life\t200\n",
                         {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "84"})

        combined = wf1 + wf2
        sorted_lines = "\n".join(sorted(combined.strip().splitlines())) + "\n"
        result = run_script(REDUCER_PATH, sorted_lines)
        lines = result.strip().splitlines()
        assert len(lines) == 2

    def test_real_wordfreq_output_sample(self):
        sample = (
            "abhorrence\t6\nabilities\t6\nable\t54\nabode\t8\n"
            "abominable\t6\nabominably\t4\nlove\t90\ntruth\t33\n"
        )
        result = run_pipeline(sample, {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"})
        record = json.loads(result.strip().split("\t", 1)[1])
        assert record["total_word_count"] == sum([6, 6, 54, 8, 6, 4, 90, 33])
        assert record["top_words"][0][0] == "love"

    def test_metadata_with_wordfreq_pipeline(self):
        wf_mapped = run_script(MAPPER_PATH, "love\t50\n",
                               {"NORM_COMPONENT": "wordfreq", "NORM_BOOK_ID": "1342"})
        meta_mapped = run_script(MAPPER_PATH,
                                 "1342,Pride and Prejudice,Jane Austen,1,file.txt\n",
                                 {"NORM_COMPONENT": "metadata"})

        combined = wf_mapped + meta_mapped
        sorted_lines = "\n".join(sorted(combined.strip().splitlines())) + "\n"
        result = run_script(REDUCER_PATH, sorted_lines)
        record = json.loads(result.strip().split("\t", 1)[1])
        assert record["metadata"]["title"] == "Pride and Prejudice"
        assert record["metadata"]["is_bestseller"] == "1"
        assert record["total_word_count"] == 50
