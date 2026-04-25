#!/usr/bin/env python3
"""
Create a tiny presentation table from Component 5 aggregation JSON.

Usage:
  python3 component5/make_presentation_table.py <aggregation_summary.json>
  python3 component5/make_presentation_table.py <aggregation_summary.json> --format terminal
  python3 component5/make_presentation_table.py <aggregation_summary.json> --format csv
  python3 component5/make_presentation_table.py <aggregation_summary.json> --top 6
"""

import argparse
import json
import sys


DEFAULT_METRICS = [
    "polarity_score",
    "positive_count",
    "negative_count",
    "type_token_ratio",
    "unique_word_count",
    "total_word_count",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Build tiny table from aggregation summary JSON.")
    parser.add_argument("summary_json", help="Path to aggregation_summary.json")
    parser.add_argument(
        "--format",
        choices=["terminal", "markdown", "csv"],
        default="terminal",
        help="Output format (default: terminal)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=6,
        help="Maximum number of scalar metrics to print (default: 6)",
    )
    return parser.parse_args()


def fmt_value(v):
    if v is None:
        return "NA"
    if isinstance(v, float):
        return "{0:.4f}".format(v)
    return str(v)


def direction_from_delta(delta):
    if delta is None:
        return "NA"
    if abs(delta) < 1e-9:
        return "tie"
    if delta > 0:
        return "bestseller_higher"
    return "standard_higher"


def build_rows(summary, top_n):
    by_cohort = summary.get("by_cohort", {})
    b_means = by_cohort.get("bestseller", {}).get("means", {})
    s_means = by_cohort.get("standard", {}).get("means", {})
    deltas = summary.get("comparison", {}).get("mean_deltas_bestseller_minus_standard", {})

    ordered = []
    for metric in DEFAULT_METRICS:
        if metric in b_means or metric in s_means or metric in deltas:
            ordered.append(metric)
    for metric in sorted(deltas.keys()):
        if metric not in ordered:
            ordered.append(metric)

    rows = []
    for metric in ordered[:top_n]:
        b = b_means.get(metric)
        s = s_means.get(metric)
        d = deltas.get(metric)
        rows.append(
            {
                "Metric": metric,
                "BestsellerMean": fmt_value(b),
                "StandardMean": fmt_value(s),
                "Delta(B-S)": fmt_value(d),
                "Direction": direction_from_delta(d),
            }
        )
    return rows


def print_markdown(rows):
    print("| Metric | Bestseller Mean | Standard Mean | Delta (B-S) | Direction |")
    print("|---|---:|---:|---:|---|")
    for r in rows:
        print(
            "| {0} | {1} | {2} | {3} | {4} |".format(
                r["Metric"], r["BestsellerMean"], r["StandardMean"], r["Delta(B-S)"], r["Direction"]
            )
        )


def print_terminal(rows, summary):
    headers = ["Metric", "Bestseller Mean", "Standard Mean", "Delta (B-S)", "Direction"]
    body = []
    for r in rows:
        body.append(
            [
                r["Metric"],
                r["BestsellerMean"],
                r["StandardMean"],
                r["Delta(B-S)"],
                r["Direction"],
            ]
        )

    widths = []
    for idx, h in enumerate(headers):
        max_body = max([len(str(row[idx])) for row in body] + [0])
        widths.append(max(len(h), max_body))

    def hline():
        return "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def fmt_row(cells):
        rendered = []
        for i, c in enumerate(cells):
            text = str(c)
            if i in (1, 2, 3):
                rendered.append(" " + text.rjust(widths[i]) + " ")
            else:
                rendered.append(" " + text.ljust(widths[i]) + " ")
        return "|" + "|".join(rendered) + "|"

    by_cohort = summary.get("by_cohort", {})
    b_n = by_cohort.get("bestseller", {}).get("n_books", 0)
    s_n = by_cohort.get("standard", {}).get("n_books", 0)
    total = summary.get("books_aggregated", 0)
    dropped = summary.get("dropped_lines", 0)
    print("Books aggregated: {0} (bestseller={1}, standard={2}, dropped={3})".format(total, b_n, s_n, dropped))
    print(hline())
    print(fmt_row(headers))
    print(hline())
    for row in body:
        print(fmt_row(row))
    print(hline())


def print_csv(rows):
    print("Metric,BestsellerMean,StandardMean,Delta(B-S),Direction")
    for r in rows:
        print(
            "{0},{1},{2},{3},{4}".format(
                r["Metric"], r["BestsellerMean"], r["StandardMean"], r["Delta(B-S)"], r["Direction"]
            )
        )


def main():
    args = parse_args()
    try:
        with open(args.summary_json, "r", encoding="utf-8") as f:
            summary = json.load(f)
    except Exception as exc:
        print("ERROR: Could not read summary JSON: {0}".format(exc), file=sys.stderr)
        sys.exit(1)

    rows = build_rows(summary, max(1, args.top))
    if not rows:
        print("No scalar metrics found in summary.", file=sys.stderr)
        sys.exit(2)

    if args.format == "csv":
        print_csv(rows)
    elif args.format == "markdown":
        print_markdown(rows)
    else:
        print_terminal(rows, summary)


if __name__ == "__main__":
    main()
