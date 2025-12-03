# backend/utils/similarity.py

from __future__ import annotations

from typing import Iterable, List, Set
from difflib import SequenceMatcher


# -------------------------------------------------------
# Tokenization / normalization
# -------------------------------------------------------
def normalize(name: str) -> str:
    if not name:
        return ""
    return name.strip().lower().replace("_", " ").replace("-", " ")


def tokenize(name: str) -> List[str]:
    return [t for t in normalize(name).split() if t]


# -------------------------------------------------------
# Jaccard similarity over tokens
# -------------------------------------------------------
def jaccard_tokens(a: str, b: str) -> float:
    ta, tb = set(tokenize(a)), set(tokenize(b))
    if not ta and not tb:
        return 1.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0


# -------------------------------------------------------
# Sequence-based similarity (Levenshtein-like)
# -------------------------------------------------------
def sequence_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


# -------------------------------------------------------
# Hybrid name similarity (recommended default)
# -------------------------------------------------------
def name_similarity(a: str, b: str) -> float:
    """
    50% Jaccard token-level, 50% SequenceMatcher.
    Good balance for schema matching:
      - work_city vs location    ≈ 0.65+
      - employee_id vs emp_id    ≈ 0.9+
    """
    return 0.5 * jaccard_tokens(a, b) + 0.5 * sequence_similarity(a, b)


# -------------------------------------------------------
# Array similarity (list/string-array)
# -------------------------------------------------------
def array_similarity(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0
