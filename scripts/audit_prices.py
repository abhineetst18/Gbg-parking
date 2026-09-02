"""Audit price parsing across all data sources.

Finds systematic parsing problems by extracting every unique pricing sentence
from each source, running it through parse_sek_per_hour(), and flagging:
  - FAILED: text mentions 'kr' but parser returns None
  - SUSPICIOUS_LOW: parser returns <=2 kr/h but text contains a higher number
  - MISMATCH_HIGH: parsed value seems too high (>200 kr/h, likely a daily/monthly rate leaked)
  - OK: parsed a plausible value

Run from repo root:  python3 scripts/audit_prices.py
"""

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from merge_data import (  # noqa: E402
    find_price_text_mismatches,
    parse_free_minutes,
    parse_sek_per_hour,
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
ROOT_DIR = Path(__file__).resolve().parent.parent

# Any number followed by kr somewhere in the text (used to detect "has a price")
KR_NUMBER = re.compile(r"(\d+(?:[.,]\d+)?)\s*kr", re.IGNORECASE)


def all_numbers_kr(text: str) -> list[float]:
    """Return all numeric amounts that appear immediately before 'kr'."""
    out = []
    for m in KR_NUMBER.finditer(text or ""):
        try:
            out.append(float(m.group(1).replace(",", ".")))
        except ValueError:
            pass
    return out


def classify(text: str) -> tuple[str, float | None]:
    """Classify a pricing text. Returns (category, parsed_value)."""
    parsed = parse_sek_per_hour(text)
    tl = (text or "").lower()
    is_free = bool(re.search(r"\bgratis\b|\bavgiftsfri", tl))
    nums = all_numbers_kr(text)

    if parsed is None:
        if is_free:
            return ("OK_FREE", 0.0)
        if "kr" in tl and nums:
            return ("FAILED", None)
        return ("NO_PRICE_TEXT", None)

    if parsed == 0.0:
        return ("OK_FREE", 0.0)

    # Suspiciously low: parser picked <=2 but text clearly has higher hourly numbers
    if parsed <= 2 and nums and max(nums) > parsed * 3:
        return ("SUSPICIOUS_LOW", parsed)

    # Suspiciously high: hourly > 200 is almost certainly a daily/monthly leak
    if parsed > 200:
        return ("MISMATCH_HIGH", parsed)

    return ("OK", parsed)


def extract_easypark() -> list[tuple[str, str]]:
    """Return (label, pricing_text) tuples from EasyPark complete data.

    Uses the same text combination as merge_data.py for consistency.
    """
    path = DATA_DIR / "easypark_gothenburg_complete.json"
    if not path.exists():
        return []
    raw = json.loads(path.read_text())
    out = []
    for ano, rec in raw.items():
        d = rec.get("areaDetail", {})
        popup = d.get("popUpMessage") or ""
        free_text = d.get("freeTextTariffInfo") or ""
        price_info = d.get("priceInfo") or ""
        # Combine all fields consistently (matches merge_data.py)
        text = "\n".join(filter(None, [popup, free_text, price_info]))
        name = d.get("areaName", ano)
        if text.strip():
            out.append((f"EP {name} ({d.get('areaNo', ano)})", text))
    return out


def extract_pgbg() -> list[tuple[str, str, float | None]]:
    """Return (label, joined_raw_text, stored_price) tuples from P.GBG.

    price_info_raw is a LIST of strings; price_per_hour_sek is the pre-computed
    authoritative value we can cross-check the parser against.
    """
    path = DATA_DIR / "gothenburg_parking_complete.json"
    if not path.exists():
        return []
    rows = json.loads(path.read_text())
    out = []
    for x in rows if isinstance(rows, list) else []:
        name = x.get("name") or x.get("parking_code") or "?"
        raw = x.get("price_info_raw")
        if isinstance(raw, list):
            text = " ".join(s for s in raw if isinstance(s, str))
        elif isinstance(raw, str):
            text = raw
        else:
            text = ""
        stored = x.get("price_per_hour_sek")
        if text.strip():
            out.append((f"PG {name}", text, stored))
    return out


def extract_epark() -> list[tuple[str, str]]:
    path = DATA_DIR / "epark_gothenburg_zones.json"
    if not path.exists():
        return []
    zones = json.loads(path.read_text())
    out = []
    for z in zones if isinstance(zones, list) else []:
        name = z.get("title") or z.get("public_area_code", "?")
        # ePARK free text lives in description (a list of strings), matching the
        # loader's logic: " ".join(description).
        desc = z.get("description") or []
        if isinstance(desc, list):
            text = " ".join(str(d) for d in desc if d).strip()
        else:
            text = str(desc).strip()
        if text:
            out.append((f"EK {name}", text))
    return out


def extract_generated_easypark() -> list[dict]:
    """Return public output records that include EasyPark as a source."""
    path = ROOT_DIR / "parking_data.json"
    if not path.exists():
        return []
    dataset = json.loads(path.read_text())
    return [
        spot
        for spot in dataset.get("spots", [])
        if "easypark" in spot.get("sources", [])
    ]


def normalize_pattern(text: str) -> str:
    """Collapse digits/whitespace so distinct phrasings group together."""
    t = re.sub(r"\d+(?:[.,]\d+)?", "#", text.lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t


def main() -> None:
    # Free-text sources audited via parse_sek_per_hour.
    text_sources = {
        "EasyPark": [(lbl, txt) for lbl, txt in extract_easypark()],
        "ePARK": [(lbl, txt) for lbl, txt in extract_epark()],
    }
    # P.GBG carries its own pre-computed price → cross-check parser vs stored.
    pgbg = extract_pgbg()

    grand = Counter()
    failed_samples: dict[str, list] = defaultdict(list)
    unique_texts: set[str] = set()
    pattern_counts: Counter = Counter()
    pattern_example: dict[str, tuple] = {}

    def record(src, label, text):
        unique_texts.add(text.strip())
        pat = normalize_pattern(text)
        pattern_counts[pat] += 1
        cat, val = classify(text)
        pattern_example.setdefault(pat, (src, label, val, text, cat))
        grand[cat] += 1
        if cat in ("FAILED", "SUSPICIOUS_LOW", "MISMATCH_HIGH"):
            if len(failed_samples[cat]) < 60:
                failed_samples[cat].append((src, label, val, text))
        return cat

    for src, items in text_sources.items():
        cats = Counter()
        for label, text in items:
            cats[record(src, label, text)] += 1
        print(f"\n=== {src} ({len(items)} priced texts) ===")
        for cat, n in cats.most_common():
            print(f"  {cat:16s} {n}")

    # P.GBG cross-check: re-parse raw and compare to stored price_per_hour_sek.
    pg_cats = Counter()
    pg_disagree = []
    for label, text, stored in pgbg:
        cat = record("P.GBG", label, text)
        pg_cats[cat] += 1
        parsed = parse_sek_per_hour(text)
        if stored is not None and parsed is not None and abs(float(stored) - parsed) > 0.5:
            if len(pg_disagree) < 40:
                pg_disagree.append((label, stored, parsed, text))
    print(f"\n=== P.GBG ({len(pgbg)} priced texts) ===")
    for cat, n in pg_cats.most_common():
        print(f"  {cat:16s} {n}")
    print(f"  Parser-vs-stored disagreements: {len(pg_disagree)}")

    generated_easypark = extract_generated_easypark()
    generated_mismatches = find_price_text_mismatches(generated_easypark)
    generated_free_mismatches = []
    for spot in generated_easypark:
        parsed_free = parse_free_minutes(spot.get("price_text", ""))
        if parsed_free is not None and spot.get("free_minutes") != parsed_free:
            generated_free_mismatches.append(
                (str(spot.get("id", "?")), spot.get("free_minutes"), parsed_free)
            )
    generated_by_id = {str(spot.get("id", "?")): spot for spot in generated_easypark}
    print(f"\n=== Generated EasyPark output ({len(generated_easypark)} spots) ===")
    print(f"  Stored-vs-displayed disagreements: {len(generated_mismatches)}")
    for spot_id, stored, parsed in generated_mismatches[:20]:
        text = generated_by_id[spot_id].get("price_text", "").replace("\n", " / ")[:140]
        print(f"  {spot_id}: stored={stored:g} displayed={parsed:g} | {text!r}")
    print(f"  Introductory-free disagreements: {len(generated_free_mismatches)}")
    for spot_id, stored, parsed in generated_free_mismatches[:20]:
        print(f"  {spot_id}: stored={stored!r} displayed={parsed} min")

    print("\n" + "=" * 60)
    print("GRAND TOTAL")
    for cat, n in grand.most_common():
        print(f"  {cat:16s} {n}")
    print(f"  Unique price sentences: {len(unique_texts)}")
    print(f"  Unique normalized patterns: {len(pattern_counts)}")

    for cat in ("FAILED", "SUSPICIOUS_LOW", "MISMATCH_HIGH"):
        samples = failed_samples.get(cat, [])
        if not samples:
            continue
        print(f"\n{'=' * 60}\n{cat} samples\n{'=' * 60}")
        seen = set()
        for src, label, val, text in samples:
            key = normalize_pattern(text)[:80]
            if key in seen:
                continue
            seen.add(key)
            snippet = text.replace("\n", " / ")[:140]
            print(f"  [{src}] parsed={val} | {label}")
            print(f"      → {snippet!r}")

    if pg_disagree:
        print(f"\n{'=' * 60}\nP.GBG parser-vs-stored disagreements\n{'=' * 60}")
        for label, stored, parsed, text in pg_disagree:
            snippet = text.replace("\n", " / ")[:120]
            print(f"  {label}: stored={stored} parser={parsed}")
            print(f"      → {snippet!r}")

    # Full vocabulary of distinct phrasings (most common first) for logic review.
    print(f"\n{'=' * 60}\nALL UNIQUE NORMALIZED PATTERNS ({len(pattern_counts)})\n{'=' * 60}")
    for pat, n in pattern_counts.most_common():
        src, label, val, text, cat = pattern_example[pat]
        flag = "" if cat in ("OK", "OK_FREE", "NO_PRICE_TEXT") else f"  <<< {cat}"
        print(f"  [{n:4d}] parsed={val} [{cat}]{flag}")
        print(f"        {pat[:110]!r}")


if __name__ == "__main__":
    main()
