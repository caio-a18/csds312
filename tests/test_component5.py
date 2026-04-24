"""
test_component5.py — pytest suite for Component 5: Results Aggregation.

Run:
    pytest tests/test_component5.py -v
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMP5_DIR = os.path.join(REPO_ROOT, "component5")
MAPPER_PATH = os.path.join(COMP5_DIR, "aggregator_mapper.py")
REDUCER_PATH = os.path.join(COMP5_DIR, "aggregator_reducer.py")


def run_script(script_path: str, stdin_text: str) -> str:
    result = subprocess.run(
        [sys.executable, script_path],
        input=stdin_text,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def run_pipeline(normalized_lines: str) -> dict:
    mapped = run_script(MAPPER_PATH, normalized_lines)
    sorted_lines = "\n".join(sorted(mapped.strip().splitlines())) + "\n"
    reduced = run_script(REDUCER_PATH, sorted_lines)
    return json.loads(reduced)


def sample_record(
    book_id: str,
    bestseller: str,
    polarity: float,
    twc: int = 1000,
    uwc: int = 400,
    syntactic: dict | None = None,
) -> str:
    rec = {
        "book_id": book_id,
        "metadata": {"title": "T", "author": "A", "is_bestseller": bestseller},
        "sentiment": {
            "polarity_score": polarity,
            "positive_count": 10.0,
            "negative_count": 5.0,
            "total_scored_words": 100.0,
        },
        "total_word_count": twc,
        "unique_word_count": uwc,
        "top_words": [],
    }
    if syntactic:
        rec["syntactic"] = syntactic
    return f"{book_id}\t{json.dumps(rec)}"


class TestMapper:
    def test_malformed_json_emits_drop(self):
        out = run_script(MAPPER_PATH, "1\tnot json\n")
        payload = json.loads(out.strip().split("\t", 1)[1])
        assert payload["__drop__"] is True
        assert payload["reason"] == "malformed_json"

    def test_missing_cohort_emits_drop(self):
        rec = {"book_id": "1", "metadata": {"title": "x"}, "sentiment": {}}
        out = run_script(MAPPER_PATH, f"1\t{json.dumps(rec)}\n")
        payload = json.loads(out.strip().split("\t", 1)[1])
        assert payload["__drop__"] is True
        assert payload["reason"] == "missing_or_invalid_cohort"

    def test_emits_reduce_key_and_json(self):
        line = sample_record("1342", "1", 0.25)
        out = run_script(MAPPER_PATH, line + "\n")
        parts = out.strip().split("\t", 1)
        assert len(parts) == 2
        assert parts[0] == "0"
        payload = json.loads(parts[1])
        assert payload["cohort"] == "bestseller"
        assert payload["book_id"] == "1342"
        assert payload["polarity_score"] == 0.25
        assert "type_token_ratio" in payload


class TestReducer:
    def test_empty_input(self):
        out = run_script(REDUCER_PATH, "")
        summary = json.loads(out)
        assert summary["books_aggregated"] == 0
        assert summary["dropped_lines"] == 0
        assert summary["by_cohort"]["bestseller"]["n_books"] == 0


class TestPipeline:
    def test_two_cohort_means_and_polarity_delta(self):
        lines = "\n".join(
            [
                sample_record("1", "1", 0.10),
                sample_record("2", "1", 0.30),
                sample_record("3", "0", 0.10),
            ]
        )
        summary = run_pipeline(lines + "\n")
        assert summary["books_aggregated"] == 3
        assert summary["dropped_lines"] == 0
        assert summary["by_cohort"]["bestseller"]["n_books"] == 2
        assert summary["by_cohort"]["standard"]["n_books"] == 1
        b_pol = summary["by_cohort"]["bestseller"]["means"]["polarity_score"]
        s_pol = summary["by_cohort"]["standard"]["means"]["polarity_score"]
        assert b_pol == pytest.approx(0.20)
        assert s_pol == pytest.approx(0.10)
        d = summary["comparison"]["mean_deltas_bestseller_minus_standard"]["polarity_score"]
        assert d == pytest.approx(0.10)

    def test_syntactic_optional_and_delta(self):
        lines = "\n".join(
            [
                sample_record("1", "1", 0.0, syntactic={"avg_sentence_length": 20.0}),
                sample_record("2", "0", 0.0, syntactic={"avg_sentence_length": 10.0}),
            ]
        )
        summary = run_pipeline(lines + "\n")
        syn_d = summary["comparison"]["syntactic_mean_deltas"]["avg_sentence_length"]
        assert syn_d["delta"] == pytest.approx(10.0)
        assert syn_d["direction"] == "bestseller_higher"

    def test_dropped_lines_counted_in_pipeline(self):
        lines = "\n".join(
            [
                "bad\tnot json",
                sample_record("1", "1", 0.0),
                "nosep",
                "",
            ]
        )
        summary = run_pipeline(lines + "\n")
        assert summary["books_aggregated"] == 1
        assert summary["dropped_lines"] >= 3
        assert summary["dropped_by_reason"]["malformed_json"] >= 1
        assert summary["dropped_by_reason"]["no_tab_separator"] >= 1
        assert summary["dropped_by_reason"]["empty_line"] >= 1


class TestDirection:
    def test_tie_near_zero(self):
        # Build minimal pipeline where delta is ~0
        lines = "\n".join(
            [
                sample_record("1", "1", 0.5),
                sample_record("2", "0", 0.5),
            ]
        )
        summary = run_pipeline(lines + "\n")
        d = summary["comparison"]["mean_deltas_bestseller_minus_standard"]["polarity_score"]
        assert d == pytest.approx(0.0)
        assert summary["comparison"]["direction"]["polarity_score"] == "tie"
