#!/usr/bin/env python3
"""
download_books.py — Download Project Gutenberg books for the CSDS 312 literary analysis pipeline.

Downloads a curated set of bestsellers and standard works, then writes books_metadata.csv
in the same format expected by the normalization pipeline (Component 4).

Output:
  books/              — downloaded .txt files (one per book)
  books_metadata.csv  — metadata for all successfully downloaded books

Usage:
    python3 download_books.py
    python3 download_books.py --output-dir /custom/path --delay 1.5
"""

import argparse
import csv
import os
import sys
import time
import urllib.error
import urllib.request

# Curated book list: (gutenberg_id, title, author, is_bestseller)
# is_bestseller=1 for critically acclaimed / culturally dominant works,
# is_bestseller=0 for lesser-known or niche works used as the comparison baseline.
BOOKS = [
    (1342, "Pride and Prejudice",                          "Jane Austen",                   1),
    (84,   "Frankenstein",                                 "Mary Shelley",                  1),
    (1661, "The Adventures of Sherlock Holmes",            "Arthur Conan Doyle",            1),
    (11,   "Alice's Adventures in Wonderland",             "Lewis Carroll",                 1),
    (98,   "A Tale of Two Cities",                         "Charles Dickens",               1),
    (2701, "Moby Dick",                                    "Herman Melville",               1),
    (174,  "The Picture of Dorian Gray",                   "Oscar Wilde",                   1),
    (345,  "Dracula",                                      "Bram Stoker",                   1),
    (1400, "Great Expectations",                           "Charles Dickens",               1),
    (76,   "Adventures of Huckleberry Finn",               "Mark Twain",                    1),
    (768,  "Wuthering Heights",                            "Emily Bronte",                  1),
    (1260, "Jane Eyre",                                    "Charlotte Bronte",              1),
    (158,  "Emma",                                         "Jane Austen",                   1),
    (514,  "Little Women",                                 "Louisa May Alcott",             1),
    (219,  "Heart of Darkness",                            "Joseph Conrad",                 0),
    (5200, "The Metamorphosis",                            "Franz Kafka",                   0),
    (2814, "Dubliners",                                    "James Joyce",                   0),
    (1952, "The Yellow Wallpaper",                         "Charlotte Perkins Gilman",      0),
    (43,   "The Strange Case of Dr Jekyll and Mr Hyde",   "Robert Louis Stevenson",        0),
    (25344,"The Scarlet Letter",                           "Nathaniel Hawthorne",           0),
]

# Project Gutenberg serves plain-text files at several URL patterns.
# The cache URL is the most reliable; the files/ URLs are fallbacks.
URL_TEMPLATES = [
    "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt",
    "https://www.gutenberg.org/files/{id}/{id}-0.txt",
    "https://www.gutenberg.org/files/{id}/{id}.txt",
]

USER_AGENT = "CSDS312-LiteraryAnalysis/1.0 (academic research, non-commercial)"
METADATA_FIELDS = ["book_id", "title", "author", "is_bestseller", "filename"]


def fetch_book(book_id: int, output_dir: str, delay: float) -> str | None:
    filename = f"{book_id}.txt"
    filepath = os.path.join(output_dir, filename)

    if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
        size_kb = os.path.getsize(filepath) // 1024
        print(f"    [skip] {filename} already on disk ({size_kb} KB)")
        return filepath

    for template in URL_TEMPLATES:
        url = template.format(id=book_id)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as resp:
                content = resp.read()
            with open(filepath, "wb") as f:
                f.write(content)
            size_kb = len(content) // 1024
            print(f"    [ok]   {filename} — {size_kb} KB")
            time.sleep(delay)
            return filepath
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            print(f"    [warn] HTTP {e.code} from {url}", file=sys.stderr)
            continue
        except urllib.error.URLError as e:
            print(f"    [warn] Network error for {url}: {e.reason}", file=sys.stderr)
            continue

    print(f"    [fail] No URL succeeded for book {book_id}", file=sys.stderr)
    return None


def main():
    parser = argparse.ArgumentParser(description="Download Project Gutenberg books for CSDS 312")
    parser.add_argument("--output-dir", default=None,
                        help="Directory to save .txt files (default: <script_dir>/books)")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Seconds between requests (default: 1.0, be respectful to Gutenberg)")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    books_dir = args.output_dir or os.path.join(script_dir, "books")
    metadata_path = os.path.join(script_dir, "books_metadata.csv")

    os.makedirs(books_dir, exist_ok=True)

    print(f"Downloading {len(BOOKS)} books to: {books_dir}")
    print(f"Metadata output:               {metadata_path}")
    print(f"Request delay:                 {args.delay}s\n")

    downloaded = []
    failed = []

    for book_id, title, author, is_bestseller in BOOKS:
        label = "bestseller" if is_bestseller else "standard"
        print(f"  {title} ({book_id}) [{label}]")
        filepath = fetch_book(book_id, books_dir, args.delay)
        if filepath:
            downloaded.append({
                "book_id":       book_id,
                "title":         title,
                "author":        author,
                "is_bestseller": is_bestseller,
                "filename":      os.path.basename(filepath),
            })
        else:
            failed.append((book_id, title))

    with open(metadata_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=METADATA_FIELDS)
        writer.writeheader()
        writer.writerows(downloaded)

    print(f"\nResults: {len(downloaded)} downloaded, {len(failed)} failed")
    if failed:
        for book_id, title in failed:
            print(f"  FAILED: {title} ({book_id})")
    print(f"Metadata written to: {metadata_path}")
    print("\nNext step:")
    print("  python3 ../load_testing/run_load_test.py")


if __name__ == "__main__":
    main()
