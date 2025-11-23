from typing import Iterable, List, Set

def tokenize(name: str) -> List[str]:
    return [t for t in name.lower().replace("_", " ").replace("-", " ").split() if t]

def jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    return len(sa & sb) / max(1, len(sa | sb))

def name_similarity(a: str, b: str) -> float:
    return jaccard(tokenize(a), tokenize(b))

def array_similarity(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    return len(sa & sb) / max(1, len(sa | sb))
