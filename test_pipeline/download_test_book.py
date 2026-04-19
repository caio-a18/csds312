#!/usr/bin/env python3
"""
Download Pride and Prejudice from Project Gutenberg for Hadoop Streaming test.
Uses only built-in Python libraries (no external dependencies).
"""

import urllib.request
import time
import os
import csv
import sys

# Book configuration
BOOK_ID = 1342
BOOK_URL = "https://www.gutenberg.org/files/1342/1342-0.txt"
OUTPUT_FILENAME = "1342_pride_and_prejudice.txt"
METADATA_CSV = "test_metadata.csv"
RATE_LIMIT_DELAY = 0.5  # seconds, to be polite to Gutenberg servers


def download_book(url, output_path, delay=0.5):
    """Download a book from Project Gutenberg with rate limiting."""
    print(f"Downloading: {url}")
    print(f"Saving to:   {output_path}")

    # Rate limiting: wait before making request
    time.sleep(delay)

    try:
        # Set a User-Agent header to identify ourselves politely
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "CSDS312-Student-Project/1.0 (educational use)"}
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            content = response.read()

        with open(output_path, "wb") as f:
            f.write(content)

        size_kb = os.path.getsize(output_path) / 1024
        print(f"Download complete: {size_kb:.1f} KB")
        return True

    except urllib.error.URLError as e:
        print(f"ERROR: Network failure - {e}", file=sys.stderr)
        return False
    except urllib.error.HTTPError as e:
        print(f"ERROR: HTTP {e.code} - {e.reason}", file=sys.stderr)
        return False
    except OSError as e:
        print(f"ERROR: Could not write file - {e}", file=sys.stderr)
        return False


def write_metadata(csv_path, book_id, title, author, is_bestseller, filename):
    """Write book metadata to CSV file."""
    print(f"Writing metadata to: {csv_path}")
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["book_id", "title", "author", "is_bestseller", "filename"])
            writer.writerow([book_id, title, author, is_bestseller, filename])
        print("Metadata written successfully.")
        return True
    except OSError as e:
        print(f"ERROR: Could not write metadata - {e}", file=sys.stderr)
        return False


def main():
    # Determine output directory (same directory as this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, OUTPUT_FILENAME)
    csv_path = os.path.join(script_dir, METADATA_CSV)

    # Download the book
    success = download_book(BOOK_URL, output_path, delay=RATE_LIMIT_DELAY)
    if not success:
        print("Download failed. Check your internet connection and try again.", file=sys.stderr)
        sys.exit(1)

    # Write metadata CSV
    write_metadata(
        csv_path,
        book_id=BOOK_ID,
        title="Pride and Prejudice",
        author="Jane Austen",
        is_bestseller=1,
        filename=OUTPUT_FILENAME,
    )

    print("\nAll done! Files created:")
    print(f"  {output_path}")
    print(f"  {csv_path}")
    print("\nNext steps:")
    print("  1. Run locally:  bash test_wordfreq_local.sh")
    print("  2. Setup HDFS:   bash setup_hdfs.sh")
    print("  3. Run cluster:  bash submit_wordfreq_cluster.sh")


if __name__ == "__main__":
    main()
