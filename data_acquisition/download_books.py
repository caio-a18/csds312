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
    (84, "Frankenstein; or, the modern prometheus", "Shelley, Mary Wollstonecraft", 1),
    (45304, "The City of God, Volume I", "Augustine, of Hippo, Saint", 1),
    (2701, "Moby Dick; Or, The Whale", "Melville, Herman", 1),
    (1342, "Pride and Prejudice", "Austen, Jane", 1),
    (768, "Wuthering Heights", "Bront, Emily", 1),
    (1513, "Romeo and Juliet", "Shakespeare, William", 1),
    (52106, "The origin and development of the moral ideas", "Westermarck, Edward", 1),
    (11, "Alice's Adventures in Wonderland", "Carroll, Lewis", 1),
    (64317, "The Great Gatsby", "Fitzgerald, F. Scott (Francis Scott)", 1),
    (100, "The Complete Works of William Shakespeare", "Shakespeare, William", 1),
    (1260, "Jane Eyre: An Autobiography", "Bront, Charlotte", 1),
    (2641, "A Room with a View", "Forster, E. M. (Edward Morgan)", 1),
    (43, "The strange case of Dr. Jekyll and Mr. Hyde", "Stevenson, Robert Louis", 1),
    (8492, "The King in Yellow", "Chambers, Robert W. (Robert William)", 1),
    (145, "Middlemarch", "Eliot, George", 1),
    (174, "The Picture of Dorian Gray", "Wilde, Oscar", 1),
    (844, "The Importance of Being Earnest: A Trivial Comedy for Serious People", "Wilde, Oscar", 1),
    (2554, "Crime and Punishment", "Dostoyevsky, Fyodor", 1),
    (37106, "Little Women; Or, Meg, Jo, Beth, and Amy", "Alcott, Louisa May", 1),
    (67979, "The Blue Castle: a novel", "Montgomery, L. M. (Lucy Maud)", 1),
    (1184, "The Count of Monte Cristo", "Dumas, Alexandre, Maquet, Auguste", 1),
    (3207, "Leviathan", "Hobbes, Thomas", 1),
    (5197, "My Life  Volume 1", "Wagner, Richard", 1),
    (345, "Dracula", "Stoker, Bram", 1),
    (2542, "A Doll's House : a play", "Ibsen, Henrik", 1),
    (16389, "The Enchanted April", "Von Arnim, Elizabeth", 1),
    (1259, "Twenty years after", "Dumas, Alexandre, Maquet, Auguste", 1),
    (28054, "The Brothers Karamazov", "Dostoyevsky, Fyodor", 1),
    (76, "Adventures of Huckleberry Finn", "Twain, Mark", 1),
    (1661, "The Adventures of Sherlock Holmes", "Doyle, Arthur Conan", 1),
    (6761, "The Adventures of Ferdinand Count Fathom  Complete", "Smollett, T. (Tobias)", 1),
    (98, "A Tale of Two Cities", "Dickens, Charles", 1),
    (394, "Cranford", "Gaskell, Elizabeth Cleghorn", 1),
    (2160, "The Expedition of Humphry Clinker", "Smollett, T. (Tobias)", 1),
    (6593, "History of Tom Jones, a Foundling", "Fielding, Henry", 1),
    (4085, "The Adventures of Roderick Random", "Smollett, T. (Tobias)", 1),
    (1080, "A Modest Proposal: For preventing the children of poor people in Ireland, from being a burden on their parents or country, and for making them beneficial to the publick", "Swift, Jonathan", 1),
    (1400, "Great Expectations", "Dickens, Charles", 1),
    (16328, "Beowulf: An Anglo-Saxon Epic Poem", "Unknown", 1),
    (1232, "The Prince", "Machiavelli, Niccol", 1),
    (1998, "Thus Spake Zarathustra: A Book for All and None", "Nietzsche, Friedrich Wilhelm", 1),
    (57336, "Ancient Britain and the Invasions of Julius Caesar", "Holmes, T. Rice (Thomas Rice)", 1),
    (1952, "The Yellow Wallpaper", "Gilman, Charlotte Perkins", 1),
    (2680, "Meditations", "Marcus Aurelius, Emperor of Rome", 1),
    (25344, "The Scarlet Letter", "Hawthorne, Nathaniel", 1),
    (5200, "Metamorphosis", "Kafka, Franz", 1),
    (47715, "The Works of William Shakespeare [Cambridge Edition] [Vol. 7 of 9]", "Shakespeare, William", 1),
    (2591, "Grimms' Fairy Tales", "Grimm, Jacob, Grimm, Wilhelm", 1),
    (4300, "Ulysses", "Joyce, James", 1),
    (2600, "War and Peace", "Tolstoy, Leo, graf", 1),
    (205, "Walden, and On The Duty Of Civil Disobedience", "Thoreau, Henry David", 1),
    (50559, "The Works of William Shakespeare [Cambridge Edition] [Vol. 3 of 9]", "Shakespeare, William", 1),
    (45, "Anne of Green Gables", "Montgomery, L. M. (Lucy Maud)", 1),
    (74, "The Adventures of Tom Sawyer, Complete", "Twain, Mark", 1),
    (3296, "The Confessions of St. Augustine", "Augustine, of Hippo, Saint", 1),
    (829, "Gulliver's Travels into Several Remote Nations of the World", "Swift, Jonathan", 0),
    (120, "Treasure Island", "Stevenson, Robert Louis", 0),
    (36034, "White nights, and other stories", "Dostoyevsky, Fyodor", 0),
    (49008, "The Works of William Shakespeare [Cambridge Edition] [Vol. 8 of 9]", "Shakespeare, William", 0),
    (51960, "The Ancient Stone Implements, Weapons and Ornaments, of Great Britain: Second Edition, Revised", "Evans, John", 0),
    (8800, "The divine comedy", "Dante Alighieri", 0),
    (1727, "The Odyssey: Rendered into English prose for the use of those who cannot read the original", "Homer", 0),
    (1399, "Anna Karenina", "Tolstoy, Leo, graf", 0),
    (28189, "My Friends the Savages: Notes and Observations of a Perak settler (Malay Peninsula)", "Cerruti, Giovanni Battista", 0),
    (2852, "The Hound of the Baskervilles", "Doyle, Arthur Conan", 0),
    (408, "The Souls of Black Folk", "Du Bois, W. E. B. (William Edward Burghardt)", 0),
    (244, "A Study in Scarlet", "Doyle, Arthur Conan", 0),
    (34901, "On Liberty", "Mill, John Stuart", 0),
    (27673, "Oedipus King of Thebes: Translated into English Rhyming Verse with Explanatory Notes", "Sophocles", 0),
    (55, "The Wonderful Wizard of Oz", "Baum, L. Frank (Lyman Frank)", 0),
    (4363, "Beyond Good and Evil", "Nietzsche, Friedrich Wilhelm", 0),
    (23, "Narrative of the Life of Frederick Douglass, an American Slave", "Douglass, Frederick", 0),
    (57493, "The Natural History of Pliny, Volume 1 (of 6)", "Pliny, the Elder", 0),
    (7370, "Second Treatise of Government", "Locke, John", 0),
    (60230, "The Natural History of Pliny, Volume 2 (of 6)", "Pliny, the Elder", 0),
    (78248, "Prolegomena to the study of Greek religion", "Harrison, Jane Ellen", 0),
    (45917, "Origin of Cultivated Plants: The International Scientific Series Volume XLVIII", "Candolle, Alphonse de", 0),
    (219, "Heart of Darkness", "Conrad, Joseph", 0),
    (50095, "The Works of William Shakespeare [Cambridge Edition] [Vol. 4 of 9]", "Shakespeare, William", 0),
    (161, "Sense and Sensibility", "Austen, Jane", 0),
    (16, "Peter Pan : $b [Peter and Wendy]", "Barrie, J. M. (James Matthew)", 0),
    (51143, "The Waterloo Roll Call: With Biographical Notes and Anecdotes", "Dalton, Charles", 0),
    (135, "Les Misrables", "Hugo, Victor", 0),
    (24869, "The Rmyan of Vlmki, translated into English verse", "Valmiki", 0),
    (59131, "The Natural History of Pliny, Volume 3 (of 6)", "Pliny, the Elder", 0),
    (730, "Oliver Twist", "Dickens, Charles", 0),
    (2148, "The Works of Edgar Allan Poe  Volume 2", "Poe, Edgar Allan", 0),
    (1497, "The Republic", "Plato", 0),
    (215, "The call of the wild", "London, Jack", 0),
    (52160, "A Short History of Freethought Ancient and Modern, Volume 2 of 2: Third edition, Revised and Expanded, in two volumes", "Robertson, J. M. (John Mackinnon)", 0),
    (164, "Twenty Thousand Leagues under the Sea", "Verne, Jules", 0),
    (521, "The Life and Adventures of Robinson Crusoe", "Defoe, Daniel", 0),
    (20203, "Autobiography of Benjamin Franklin", "Franklin, Benjamin", 0),
    (51252, "The Book of the Thousand Nights and a Night  Volume 01 (of 10)", "Unknown", 0),
    (21765, "The Metamorphoses of Ovid, Books I-VII", "Ovid", 0),
    (1023, "Bleak House", "Dickens, Charles", 0),
    (36, "The war of the worlds", "Wells, H. G. (Herbert George)", 0),
    (132, "The Art of War", "Sunzi, active 6th century B.C.", 0),
    (996, "Don Quixote", "Cervantes Saavedra, Miguel de", 0),
    (46, "A Christmas Carol in Prose; Being a Ghost Story of Christmas", "Dickens, Charles", 0),
    (46976, "The Anabasis of Alexander : $b or, The history of the wars and conquests of Alexander the Great", "Arrian", 0),
    (49007, "The Works of William Shakespeare [Cambridge Edition] [Vol. 6 of 9]", "Shakespeare, William", 0),
    (50685, "The Story of Genesis and Exodus: An Early English Song, about 1250 A.D.", "Unknown", 0),
    (48155, "History of Ancient Pottery: Greek, Etruscan, and Roman.  Volume 2 (of 2)", "Walters, H. B. (Henry Beauchamp), Birch, Samuel", 0),
    (19488, "The Life of Joan of Arc, Vol. 1 and 2", "France, Anatole", 0),
    (209, "The Turn of the Screw", "James, Henry", 0),
    (50883, "Narrative and Critical History of America, Vol. 2 (of 8): Spanish Explorations and Settlements in America from the Fifteenth to the Seventeenth Century", "Unknown", 0),
    (35, "The Time Machine", "Wells, H. G. (Herbert George)", 0),
    (15399, "The Interesting Narrative of the Life of Olaudah Equiano, Or Gustavus Vassa, The African: Written By Himself", "Equiano, Olaudah", 0),
    (58750, "Superstition and Force: Essays on the Wager of Law, the Wager of Battle, the Ordeal, Torture", "Lea, Henry Charles", 0),
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
