"""
preprocess.py — Shared text-cleaning utilities for the Book Script Project.

Used by:
  - mapper.py  (word frequency)
  - Other group members' MapReduce components (sentiment, syntactic complexity)

Author: Caio Albuquerque
"""

import re
import os

# ---------------------------------------------------------------------------
# Stop-word list
# ---------------------------------------------------------------------------

_STOPWORDS_PATH = os.path.join(os.path.dirname(__file__), "stopwords.txt")

def _load_stopwords(path: str = _STOPWORDS_PATH) -> frozenset:
    """Load stop words from file; fall back to a minimal built-in set."""
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return frozenset(w.strip().lower() for w in fh if w.strip())
    # Minimal fallback so the module works without the file
    return frozenset(
        "a an the and or but in on at to for of with is are was were be been "
        "have has had do does did will would could should may might shall "
        "i me my we our you your he him his she her it its they them their "
        "this that these those what which who whom how when where why not "
        "no nor so yet both either neither once just than then there here "
        "s t ve ll d re".split()
    )

STOP_WORDS: frozenset = _load_stopwords()

# ---------------------------------------------------------------------------
# Gutenberg boilerplate detection
# ---------------------------------------------------------------------------

_GUTENBERG_START_RE = re.compile(
    r"^\*\*\*\s*START OF (THE|THIS) PROJECT GUTENBERG", re.IGNORECASE
)
_GUTENBERG_END_RE = re.compile(
    r"^\*\*\*\s*END OF (THE|THIS) PROJECT GUTENBERG", re.IGNORECASE
)


def strip_gutenberg(text: str) -> str:
    """
    Remove Project Gutenberg header and footer boilerplate from a full book
    string.  Returns only the body text between the sentinel markers.
    If no markers are found, returns the original text unchanged.
    """
    lines = text.splitlines()
    start_idx = 0
    end_idx = len(lines)

    for i, line in enumerate(lines):
        if _GUTENBERG_START_RE.match(line):
            start_idx = i + 1
            break

    for i in range(len(lines) - 1, start_idx, -1):
        if _GUTENBERG_END_RE.match(lines[i]):
            end_idx = i
            break

    return "\n".join(lines[start_idx:end_idx])


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(r"[a-z0-9']+")


def tokenize(text: str) -> list:
    """
    Lowercase text and return a list of alphabetic/numeric tokens.
    Apostrophes are kept so contractions remain intact before stop-word
    filtering removes them.
    """
    return _TOKEN_RE.findall(text.lower())


# ---------------------------------------------------------------------------
# Stop-word filtering
# ---------------------------------------------------------------------------

def remove_stop_words(tokens: list, stop_words: frozenset = None) -> list:
    """
    Remove stop words and single-character tokens from a token list.
    Uses module-level STOP_WORDS by default; callers can supply their own.
    """
    sw = stop_words if stop_words is not None else STOP_WORDS
    return [t for t in tokens if t not in sw and len(t) > 1]


# ---------------------------------------------------------------------------
# Convenience pipeline
# ---------------------------------------------------------------------------

def preprocess_line(line: str, stop_words: frozenset = None) -> list:
    """
    Full preprocessing for a single line of text:
      1. tokenize  (lowercase + extract tokens)
      2. remove_stop_words

    Returns a list of cleaned tokens ready for MapReduce emission.
    """
    return remove_stop_words(tokenize(line), stop_words)
