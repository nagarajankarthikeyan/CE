import re
from difflib import SequenceMatcher
from functools import lru_cache
from google.cloud import bigquery
from app.config import (
    BIGQUERY_PROJECT,
    BIGQUERY_DATASET,
    BIGQUERY_LOCATION,
    GOOGLE_APPLICATION_CREDENTIALS,
)

PLATFORM_SYNONYMS_TABLE = (
    f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.platform_synonyms"
)

client = bigquery.Client.from_service_account_json(
    GOOGLE_APPLICATION_CREDENTIALS,
    location=BIGQUERY_LOCATION
)



def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", text.lower())).strip()


@lru_cache(maxsize=1)
def _platform_synonyms() -> dict:
    query = f"""
    SELECT synonym, synonym_normalized, canonical_platform
    FROM `{PLATFORM_SYNONYMS_TABLE}`
    """
    rows = client.query(query).result()

    synonyms = {}
    for row in rows:
        canonical = (row.get("canonical_platform") or "").strip()
        raw_synonym = (row.get("synonym") or "").strip()
        normalized_synonym = (row.get("synonym_normalized") or "").strip()

        if not canonical:
            continue

        key = normalized_synonym or _normalize_text(raw_synonym)
        if key:
            synonyms[key] = canonical

    return synonyms


def find_platform_match(message: str, threshold: float = 0.84):
    """
    Returns (canonical_platform, matched_phrase) if found, else (None, None).
    Supports exact and typo-tolerant phrase matching.
    """
    msg_norm = _normalize_text(message)
    if not msg_norm:
        return None, None
    synonyms_map = _platform_synonyms()
    if not synonyms_map:
        return None, None

    # 1) Exact phrase match first (prefer longer synonyms).
    for synonym, canonical in sorted(synonyms_map.items(), key=lambda kv: len(kv[0]), reverse=True):
        syn = _normalize_text(synonym)
        if not syn:
            continue
        # Require token/phrase boundaries to avoid false positives like:
        # "li" matching "line" or "x" matching words containing x.
        syn_pattern = r"\b" + r"\s+".join(re.escape(tok) for tok in syn.split()) + r"\b"
        if re.search(syn_pattern, msg_norm, flags=re.IGNORECASE):
            return canonical, syn

    # 2) Fuzzy phrase match against n-grams.
    tokens = msg_norm.split()
    best = (0.0, None, None)  # score, canonical, matched_phrase

    for synonym, canonical in synonyms_map.items():
        syn = _normalize_text(synonym)
        syn_tokens = syn.split()
        if not syn_tokens:
            continue

        n = len(syn_tokens)
        # try exact token-length windows first; also allow +/-1 for minor tokenization issues
        window_sizes = [n]
        if n > 1:
            window_sizes.extend([n - 1, n + 1])

        for w in window_sizes:
            if w <= 0 or w > len(tokens):
                continue
            for i in range(0, len(tokens) - w + 1):
                chunk = " ".join(tokens[i : i + w])
                score = SequenceMatcher(None, syn, chunk).ratio()
                if score > best[0]:
                    best = (score, canonical, chunk)

    if best[0] >= threshold:
        return best[1], best[2]

    return None, None
