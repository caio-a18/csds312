#!/usr/bin/env python3
"""
aggregator_reducer.py — Component 5: Results Aggregation (Hadoop Streaming reducer).

Expects mapper output lines:
    0\t<json_payload_per_book>
    0\t{"__drop__": true, "reason": "<why>"}   — counted in dropped_lines

Aggregates by cohort (bestseller vs standard), computes means, optional syntactic
means per metric, and cohort deltas. Emits one JSON summary (pretty-printed)
to stdout.

Author: Mitchell Fein / CSDS 312 Group 9
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict

SCALAR_METRICS = [
    "polarity_score",
    "positive_count",
    "negative_count",
    "total_scored_words",
    "total_word_count",
    "unique_word_count",
    "type_token_ratio",
]


def mean(sum_v: float, n: int) -> float | None:
    if n <= 0:
        return None
    return sum_v / n


def finalize_cohort(name: str, n: int, sums: dict[str, float], syn_sum: dict[str, float], syn_n: dict[str, int]) -> dict:
    means: dict[str, float | None] = {}
    for m in SCALAR_METRICS:
        if m in sums:
            means[m] = mean(sums[m], n)
    syntactic_means: dict[str, float | None] = {}
    for k in sorted(syn_sum.keys()):
        syntactic_means[k] = mean(syn_sum[k], syn_n.get(k, 0))
    return {
        "cohort": name,
        "n_books": n,
        "means": means,
        "syntactic_means": syntactic_means,
        "sums": {k: sums[k] for k in sorted(sums.keys())},
    }


def delta_means(b_means: dict, s_means: dict) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    keys = set(b_means.keys()) | set(s_means.keys())
    for k in sorted(keys):
        bv = b_means.get(k)
        sv = s_means.get(k)
        if bv is None or sv is None:
            out[k] = None
        else:
            out[k] = bv - sv
    return out


def compare_direction(delta: float | None, eps: float = 1e-9) -> str | None:
    if delta is None:
        return None
    if abs(delta) < eps:
        return "tie"
    return "bestseller_higher" if delta > 0 else "standard_higher"


def main() -> None:
    n_input = 0
    n_used = 0
    drop_counts: dict[str, int] = defaultdict(int)

    # Per cohort: count, scalar sums, syntactic sum and count per key
    cohort_n: dict[str, int] = defaultdict(int)
    cohort_sums: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    cohort_syn_sum: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    cohort_syn_n: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for line in sys.stdin:
        n_input += 1
        line = line.rstrip("\n")
        if "\t" not in line:
            continue
        _, rest = line.split("\t", 1)
        rest = rest.strip()
        if not rest:
            continue
        try:
            payload = json.loads(rest)
        except json.JSONDecodeError:
            continue
        if payload.get("__drop__") is True:
            reason = str(payload.get("reason") or "unknown")
            drop_counts[reason] += 1
            continue
        cohort = payload.get("cohort")
        if cohort not in ("bestseller", "standard"):
            continue
        n_used += 1
        cohort_n[cohort] += 1

        for m in SCALAR_METRICS:
            if m in payload and isinstance(payload[m], (int, float)):
                cohort_sums[cohort][m] += float(payload[m])

        syn = payload.get("syntactic")
        if isinstance(syn, dict):
            for sk, sv in syn.items():
                if isinstance(sv, (int, float)):
                    cohort_syn_sum[cohort][sk] += float(sv)
                    cohort_syn_n[cohort][sk] += 1

    nb = cohort_n.get("bestseller", 0)
    ns = cohort_n.get("standard", 0)

    bestseller = finalize_cohort("bestseller", nb, cohort_sums["bestseller"], cohort_syn_sum["bestseller"], cohort_syn_n["bestseller"])
    standard = finalize_cohort("standard", ns, cohort_sums["standard"], cohort_syn_sum["standard"], cohort_syn_n["standard"])

    b_means = bestseller["means"]
    s_means = standard["means"]
    d_scalars = delta_means(b_means, s_means)

    b_syn = bestseller["syntactic_means"]
    s_syn = standard["syntactic_means"]
    syn_keys = set(b_syn.keys()) | set(s_syn.keys())
    d_syn: dict[str, dict[str, float | str | None]] = {}
    for k in sorted(syn_keys):
        bv = b_syn.get(k)
        sv = s_syn.get(k)
        if bv is None or sv is None:
            d_syn[k] = {"delta": None, "direction": None}
        else:
            dv = bv - sv
            d_syn[k] = {"delta": dv, "direction": compare_direction(dv)}

    n_dropped = sum(drop_counts.values())
    summary = {
        "component": "results_aggregation",
        "version": 1,
        "input_lines": n_input,
        "books_aggregated": n_used,
        "dropped_lines": n_dropped,
        "dropped_by_reason": dict(sorted(drop_counts.items())),
        "by_cohort": {
            "bestseller": bestseller,
            "standard": standard,
        },
        "comparison": {
            "mean_deltas_bestseller_minus_standard": d_scalars,
            "direction": {k: compare_direction(d_scalars[k]) for k in sorted(d_scalars.keys())},
            "syntactic_mean_deltas": d_syn,
        },
    }

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
