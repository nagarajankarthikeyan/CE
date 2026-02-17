import re
from difflib import SequenceMatcher

PLATFORM_SYNONYMS = {

    # --- SA360 / Paid Search ---
    "sa360": "SA360",
    "search ads 360": "SA360",
    "google sa360": "SA360",
    "doubleclick search": "SA360",
    "ds3": "SA360",
    "paid search": "SA360",
    "search": "SA360",
    "sem": "SA360",
    "google search": "SA360",
    "google paid search": "SA360",
    "bing search": "SA360",
    "microsoft search": "SA360",

    # --- META ---
    "meta": "META",
    "meta ads": "META",
    "facebook": "META",
    "facebook ads": "META",
    "fb": "META",
    "instagram": "META",
    "instagram ads": "META",
    "ig": "META",
    "paid social meta": "META",

    # --- DV360 / Programmatic ---
    "dv360": "DV360",
    "display & video 360": "DV360",
    "display and video 360": "DV360",
    "display video 360": "DV360",
    "display/video 360": "DV360",
    "google dv360": "DV360",
    "programmatic": "DV360",
    "programmatic display": "DV360",
    "display": "DV360",
    "banner": "DV360",
    "gdn": "DV360",
    "google display network": "DV360",
    "prospecting display": "DV360",
    "retargeting display": "DV360",

    # --- YouTube ---
    "youtube": "YOUTUBE",
    "youtube ads": "YOUTUBE",
    "yt": "YOUTUBE",
    "google video": "YOUTUBE",
    "trueview": "YOUTUBE",
    "youtube pre-roll": "YOUTUBE",

    # --- LinkedIn ---
    "linkedin": "LINKEDIN",
    "linkedin ads": "LINKEDIN",
    "li": "LINKEDIN",

    # --- TikTok ---
    "tiktok": "TIKTOK",
    "tiktok ads": "TIKTOK",
    "tt": "TIKTOK",

    # --- X (Twitter) ---
    "x": "X",
    "x ads": "X",
    "twitter": "X",
    "twitter ads": "X",
    "paid twitter": "X",

    # --- Pinterest ---
    "pinterest": "PINTEREST",
    "pinterest ads": "PINTEREST",

    # --- Snapchat ---
    "snapchat": "SNAPCHAT",
    "snap ads": "SNAPCHAT",

    # --- Amazon ---
    "amazon": "AMAZON",
    "amazon ads": "AMAZON",
    "amazon sponsored products": "AMAZON",
    "sponsored products": "AMAZON",
    "sponsored brands": "AMAZON",
    "amazon dsp": "AMAZON",
    "retail media amazon": "AMAZON",

    # --- Walmart ---
    "walmart": "WALMART",
    "walmart connect": "WALMART",
    "walmart media": "WALMART",

    # --- Target ---
    "target": "TARGET",
    "roundel": "TARGET",

    # --- Apple Search ---
    "apple search": "APPLE_SEARCH",
    "apple search ads": "APPLE_SEARCH",
    "asa": "APPLE_SEARCH",

    # --- CRM / Owned Media (if needed) ---
    "email": "EMAIL",
    "crm": "EMAIL",
    "lifecycle": "EMAIL",

    # --- Affiliate ---
    "affiliate": "AFFILIATE",
    "affiliate marketing": "AFFILIATE",
}



def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", text.lower())).strip()


def find_platform_match(message: str, threshold: float = 0.84):
    """
    Returns (canonical_platform, matched_phrase) if found, else (None, None).
    Supports exact and typo-tolerant phrase matching.
    """
    msg_norm = _normalize_text(message)
    if not msg_norm:
        return None, None

    # 1) Exact phrase match first (prefer longer synonyms).
    for synonym, canonical in sorted(PLATFORM_SYNONYMS.items(), key=lambda kv: len(kv[0]), reverse=True):
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

    for synonym, canonical in PLATFORM_SYNONYMS.items():
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
