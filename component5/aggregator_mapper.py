#!/usr/bin/env python3
"""
aggregator_mapper.py — Component 5: Results Aggregation (Hadoop Streaming mapper).

Reads normalized per-book lines from Component 4:
    book_id\t<json>

Emits a single shuffle key so one reducer can compute global cohort summaries:
    0\t<json>

Each emitted JSON includes:
    - cohort: "bestseller" | "standard" (from metadata.is_bestseller)
    - book_id
    - lexical and sentiment scalars for aggregation
    - syntactic: optional dict of floats (passed through for reducer-side mean)

Invalid input lines emit a marker record so the reducer can count drops by reason
(`dropped_lines` in the summary).

Author: Mitchell Fein / CSDS 312 Group 9
"""

from __future__ import annotations

import json
import sys

# Single reduce key so one reducer builds the full comparison.
REDUCE_KEY = "0"


def emit_drop(reason: str) -> None:
    print(f"{REDUCE_KEY}\t{json.dumps({'__drop__': True, 'reason': reason}, ensure_ascii=False)}")

KNOWN_SENTIMENT_KEYS = (
    "polarity_score",
    "positive_count",
    "negative_count",
    "total_scored_words",
)


def cohort_from_metadata(meta: dict) -> str | None:
    flag = (meta or {}).get("is_bestseller")
    if flag is None:
        return None
    s = str(flag).strip()
    if s == "1":
        return "bestseller"
    if s == "0":
        return "standard"
    return None


def build_payload(book_id: str, rec: dict, cohort: str) -> dict:
    meta = rec.get("metadata") or {}
    sentiment = rec.get("sentiment") or {}
    syntactic = rec.get("syntactic") or {}

    out: dict = {
        "cohort": cohort,
        "book_id": book_id,
        "title": meta.get("title", ""),
        "author": meta.get("author", ""),
    }

    twc = rec.get("total_word_count")
    uwc = rec.get("unique_word_count")
    if isinstance(twc, int):
        out["total_word_count"] = twc
    elif isinstance(twc, float) and twc.is_integer():
        out["total_word_count"] = int(twc)
    if isinstance(uwc, int):
        out["unique_word_count"] = uwc
    elif isinstance(uwc, float) and uwc.is_integer():
        out["unique_word_count"] = int(uwc)

    if "total_word_count" in out and "unique_word_count" in out and out["total_word_count"] > 0:
        out["type_token_ratio"] = out["unique_word_count"] / out["total_word_count"]

    for k in KNOWN_SENTIMENT_KEYS:
        v = sentiment.get(k)
        if isinstance(v, (int, float)):
            out[k] = float(v)

    if isinstance(syntactic, dict) and syntactic:
        syn_out: dict[str, float] = {}
        for sk, sv in syntactic.items():
            if isinstance(sv, (int, float)):
                syn_out[str(sk)] = float(sv)
        if syn_out:
            out["syntactic"] = syn_out

    return out


def main() -> None:
    for line in sys.stdin:
        raw = line.rstrip("\n")
        if not raw.strip():
            emit_drop("empty_line")
            continue
        if "\t" not in raw:
            emit_drop("no_tab_separator")
            continue
        book_id, rest = raw.split("\t", 1)
        book_id = book_id.strip()
        try:
            rec = json.loads(rest)
        except json.JSONDecodeError:
            emit_drop("malformed_json")
            continue
        if not isinstance(rec, dict):
            emit_drop("not_a_json_object")
            continue
        cohort = cohort_from_metadata(rec.get("metadata") or {})
        if cohort is None:
            emit_drop("missing_or_invalid_cohort")
            continue
        raw_bid = rec.get("book_id")
        if raw_bid is None or raw_bid == "":
            bid = book_id
        elif isinstance(raw_bid, float) and raw_bid.is_integer():
            bid = str(int(raw_bid))
        else:
            bid = str(raw_bid).strip()
        payload = build_payload(bid, rec, cohort)
        print(f"{REDUCE_KEY}\t{json.dumps(payload, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
