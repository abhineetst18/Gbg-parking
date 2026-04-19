"""Merge all parking data sources into a unified JSON for the web app."""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
ROOT_DIR = Path(__file__).resolve().parent.parent

# Manual corrections for P.GBG spots with wrong/missing pricing.
# Key: spot name (lowercase) substring match at coords.
# Verified via Google Street View showing P.GBG sign with code.
PGBG_CORRECTIONS = [
    {
        # Gärdesvägen near Hovås: street sign shows code 4890 (Askims Simhall zone)
        # P.GBG data wrongly says "Gratis" for these street segments
        "name_match": "gärdesvägen",
        "lat_range": (57.615, 57.625),
        "lon_range": (11.935, 11.945),
        "override": {
            "price_sek_hr": 7.0,
            "price_text": "7 kr/tim alla dagar 08-22, övrig tid 2 kr/tim",
            "area_code": "4890",
        },
    },
]


def parse_sek_per_hour(text: str) -> float | None:
    """Extract SEK/hour from Swedish pricing text. Picks the primary (first/daytime) rate.
    
    Handles patterns (case-insensitive):
      X kr/15 min, X kr per påbörjade 15 min  → X*4
      X kr/30 min, X kr per påbörjade 30 min  → X*2
      X kr per påbörjade 45 min               → X*(60/45)
      X kr per påbörjade 60 min               → X
      X kr/påbörjad timme, X kr / per påbörjade timme → X
      X kr/tim, X kr/timme, Xkr/Tim           → X
      X kr/h, Xkr/h                           → X
      0 kr/h (free)                            → 0
      Gratis / avgiftsfri                      → 0
    """
    if not text:
        return None
    t = text  # keep original case for position matching
    tl = text.lower()

    # Explicit free parking
    if re.search(r"\bgratis\b|\bavgiftsfri", tl):
        return 0.0

    candidates = []
    
    # "X kr per påbörjade N min" — generic minute-based
    for m in re.finditer(r"(\d+)\s*kr\s*/?\s*(?:per\s+)?(?:påbörjad\w*\s+)?(\d+)\s*min", tl):
        amount = float(m.group(1))
        minutes = int(m.group(2))
        if minutes > 0:
            candidates.append((m.start(), amount * 60 / minutes))
    
    # "X kr per påbörjade 45" (no "min", common truncation at line break)
    for m in re.finditer(r"(\d+)\s*kr\s*/?\s*(?:per\s+)?(?:påbörjad\w*\s+)(\d+)\s*$", tl, re.MULTILINE):
        amount = float(m.group(1))
        minutes = int(m.group(2))
        if minutes > 0 and minutes in (15, 30, 45, 60):
            candidates.append((m.start(), amount * 60 / minutes))
    
    # "X kr / per påbörjad timme" or "X kr/påbörjad tim" (case-insensitive)
    for m in re.finditer(r"(\d+)\s*kr\s*/?\s*(?:per\s+)?påbörjad\w*\s*tim", tl):
        candidates.append((m.start(), float(m.group(1))))
    
    # "X kr/tim", "X kr/timme", "Xkr/Tim", "X kr / per timme" (case-insensitive)
    for m in re.finditer(r"(\d+)\s*kr\s*/?\s*(?:per\s+)?tim", tl):
        candidates.append((m.start(), float(m.group(1))))
    
    # "X kr/h" or "Xkr/h" (compact format)
    for m in re.finditer(r"(\d+)\s*kr\s*/\s*h\b", tl):
        candidates.append((m.start(), float(m.group(1))))
    
    if candidates:
        # Return the first occurrence (primary/daytime rate), rounded to 1 decimal
        candidates.sort(key=lambda c: c[0])
        return round(candidates[0][1], 1)
    return None


def parse_max_daily(text: str) -> float | None:
    """Extract max daily rate from text like 'maxtaxa 30 kr/dag', '600 kr/dygn', '130kr/dygn'."""
    if not text:
        return None
    tl = text.lower()
    m = re.search(r"maxtaxa\s*(\d+)\s*kr", tl)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+)\s*kr\s*/?\s*(?:per\s+)?(?:dygn|dag)\b", tl)
    if m:
        return float(m.group(1))
    # "X kr 24-tim" pattern (daily flat rate)
    m = re.search(r"(\d+)\s*kr\s+24\s*-?\s*tim", tl)
    if m:
        return float(m.group(1))
    return None


def parse_season(text: str) -> tuple[str, str] | None:
    """Extract seasonal date range from text like 'Avgift 1/5-30/9' or 'Avgift 1/6 till 30/9'.
    
    Returns (start, end) as 'MM-DD' strings, or None if not seasonal.
    """
    if not text:
        return None
    tl = text.lower()
    # Pattern: "avgift D/M-D/M" or "avgift D/M till D/M"
    m = re.search(r"avgift\s+(\d{1,2})/(\d{1,2})\s*[-–]\s*(\d{1,2})/(\d{1,2})", tl)
    if not m:
        m = re.search(r"avgift\s+(\d{1,2})/(\d{1,2})\s+till\s+(\d{1,2})/(\d{1,2})", tl)
    if m:
        sd, sm, ed, em = m.group(1), m.group(2), m.group(3), m.group(4)
        return (f"{int(sm):02d}-{int(sd):02d}", f"{int(em):02d}-{int(ed):02d}")
    return None


def parse_time_limit(text: str) -> str | None:
    """Extract parking time limit from Swedish text. Returns e.g. '7d', '24h', '2h', '10min'."""
    if not text:
        return None
    tl = text.lower()
    # "Maxtid X dagar" or "Maxtid X dag"
    m = re.search(r"maxtid\s+(\d+)\s*dag", tl)
    if m:
        return f"{m.group(1)}d"
    # "Max p-tid X dagar/dygn/tim/min"
    m = re.search(r"max\s*p-tid\s*(\d+)\s*(dag|dygn|tim|min)", tl)
    if m:
        val, unit = m.group(1), m.group(2)
        if 'dag' in unit or 'dygn' in unit:
            return f"{val}d"
        return f"{val}h" if 'tim' in unit else f"{val}min"
    # "Maxtid X tim" (hours, not days)
    m = re.search(r"maxtid\s+(\d+)\s*tim", tl)
    if m:
        return f"{m.group(1)}h"
    # "24 tim" or "2 timmar" (time limit, not price)
    m = re.search(r"(\d+)\s*tim(?:mar|me)?\b(?!\s*/?\s*kr)", text)
    if m:
        return f"{m.group(1)}h"
    # "Inom zonen finns tidsbegränsade parkeringar 2tim"
    m = re.search(r"tidsbegräns\w*\s*(?:parkering\w*)?\s*(\d+)\s*tim", text)
    if m:
        return f"{m.group(1)}h"
    # "10min" or "10 min"
    m = re.search(r"tidsbegräns\w*\s*(?:parkering\w*)?\s*(\d+)\s*min", text)
    if m:
        return f"{m.group(1)}min"
    return None


def parse_free_minutes(text: str) -> int | None:
    """Extract initial free parking period from text like 'Fri parkering 2 tim' or '1 timma fri'.
    
    Returns the free period in minutes, or None if no free period.
    """
    if not text:
        return None
    tl = text.lower()
    # "Fri parkering X tim" or "Fritt X tim"
    m = re.search(r"fri\w*\s+(?:parkering\s+)?(\d+(?:[.,]\d+)?)\s*(?:tim|h)\b", tl)
    if m:
        return int(float(m.group(1).replace(",", ".")) * 60)
    # "X tim fri parkering" or "X timma fri"
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:tim\w*|h)\s+fri", tl)
    if m:
        return int(float(m.group(1).replace(",", ".")) * 60)
    # "Fri parkering X min"
    m = re.search(r"fri\w*\s+(?:parkering\s+)?(\d+)\s*min", tl)
    if m:
        return int(m.group(1))
    # "X min fri"
    m = re.search(r"(\d+)\s*min\s+fri", tl)
    if m:
        return int(m.group(1))
    return None



def classify_type(area_type: str) -> str:
    """Normalize area type to: street, garage, lot, ev, other."""
    t = area_type.lower()
    if t in ("onstreet", "on_street", "street", "timelimited"):
        return "street"
    if "garage" in t or "underground" in t or "p-hus" in t:
        return "garage"
    if "lot" in t or "surface" in t or "carpark" in t or "car_park" in t:
        return "lot"
    if "evc" in t or "charg" in t:
        return "ev"
    if "camera" in t:
        return "lot"
    return "other"


def load_easypark(gbg_codes: set[str] | None = None) -> list[dict]:
    """Load EasyPark areas from complete + summary data.
    
    Args:
        gbg_codes: Set of known Parkering Göteborg parking_code strings,
                   used to cross-reference EasyPark spots that are actually
                   GBG-operated (first 4 digits of areaNo = GBG parking_code).
    """
    path = DATA_DIR / "easypark_gothenburg_complete.json"
    if not path.exists():
        print("  WARNING: easypark_gothenburg_complete.json not found")
        return []

    if gbg_codes is None:
        gbg_codes = set()

    raw = json.loads(path.read_text())

    # Load API-fetched tariff prices as supplementary source
    tariff_prices = {}
    tariff_path = DATA_DIR / "easypark_prices.json"
    if tariff_path.exists():
        tariff_prices = json.loads(tariff_path.read_text())
    # Load lot-specific tariff prices (fetched via internal IDs)
    lot_tariff_path = DATA_DIR / "easypark_lot_prices.json"
    if lot_tariff_path.exists():
        lot_prices = json.loads(lot_tariff_path.read_text())
        # Merge: lot prices fill in gaps not covered by tile-based tariffs
        for k, v in lot_prices.items():
            if k not in tariff_prices:
                tariff_prices[k] = v

    results = []
    for ano, record in raw.items():
        detail = record.get("areaDetail", {})
        lat = detail.get("displayPoint", {}).get("lat")
        lon = detail.get("displayPoint", {}).get("lon")
        if not lat or not lon:
            continue

        popup = detail.get("popUpMessage", "") or ""
        free_text = detail.get("freeTextTariffInfo", "") or ""
        price_info = detail.get("priceInfo") or ""
        # Combine all text sources for parsing
        all_text = popup
        if not parse_sek_per_hour(popup):
            # Fallback to freeTextTariffInfo then priceInfo
            all_text = f"{popup} {free_text} {price_info}".strip()
        
        price = parse_sek_per_hour(all_text)
        time_limit = parse_time_limit(all_text)
        max_daily = parse_max_daily(all_text)
        season = parse_season(all_text)
        free_mins = parse_free_minutes(all_text)
        all_lower = all_text.lower()
        permit_req = bool(re.search(r"p-tillstånd|tillstånd\s+(?:erfordras|gäller|krävs)", all_lower))
        has_svc_fee = "serviceavgift" in all_lower

        # Supplement with API-fetched tariff price if text parsing found nothing
        if price is None and ano in tariff_prices:
            price = float(tariff_prices[ano])
        
        # Better price_text: first line of popup, or first meaningful line
        price_display = popup.split("\n")[0].strip() if popup else ""
        if not price_display and free_text:
            price_display = free_text.split("\n")[0].strip()

        area_type = detail.get("areaType", "")
        custom_type = record.get("tileData", {}).get("customAreaType", "")

        # Area code: use the full areaNo (what users type in the EasyPark app)
        area_code = str(detail.get("areaNo", ano))

        # Cross-reference: if first 4 digits match a GBG parking_code,
        # store it so the UI can show both codes
        gbg_code = None
        if len(area_code) >= 7 and area_code[:4] in gbg_codes:
            gbg_code = area_code[:4]

        results.append({
            "id": f"ep_{ano}",
            "name": detail.get("areaName", f"EasyPark {ano}"),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "price_sek_hr": price,
            "price_text": price_display,
            "time_limit": time_limit,
            "max_daily_sek": max_daily,
            "season_start": season[0] if season else None,
            "season_end": season[1] if season else None,
            "free_minutes": free_mins,
            "permit_required": permit_req,
            "service_fee": has_svc_fee,
            "area_code": area_code,
            "gbg_code": gbg_code,
            "type": classify_type(custom_type or area_type),
            "source": "easypark",
            "operator": detail.get("parkingOperatorName", ""),
            "area_type_raw": custom_type or area_type,
            "status": detail.get("status", ""),
        })
    return results


def load_parkster() -> list[dict]:
    """Load Parkster zones from full zone data."""
    path = DATA_DIR / "parkster_gothenburg_zones.json"
    if not path.exists():
        print("  WARNING: parkster_gothenburg_zones.json not found")
        return []

    zones = json.loads(path.read_text())
    # Also load summary for coordinates
    summary_path = DATA_DIR / "parkster_gothenburg_summary.json"
    coord_map = {}
    if summary_path.exists():
        for s in json.loads(summary_path.read_text()):
            if s.get("lat") and s.get("lon"):
                coord_map[s["id"]] = (s["lat"], s["lon"])

    results = []
    for z in zones:
        zid = z.get("id")
        # Try directions coords first, then summary coords
        lat = z.get("directionsLat")
        lon = z.get("directionsLong")
        if not lat or not lon:
            if zid in coord_map:
                lat, lon = coord_map[zid]
        if not lat or not lon:
            continue

        # Extract pricing from feeZone
        fz = z.get("feeZone", {}) or {}
        fees = fz.get("parkingFees", []) or []
        price = None
        price_text = ""

        # Find the NORMAL fee (primary hourly rate)
        normal_fees = [f for f in fees if f.get("typeOfRule") == "NORMAL" and f.get("amountPerHour", 0) > 0]
        if normal_fees:
            price = normal_fees[0]["amountPerHour"]
            start = normal_fees[0].get("startTime")
            end = normal_fees[0].get("endTime")
            day_type = normal_fees[0].get("typeOfDay", "")
            time_range = ""
            if start is not None and end is not None:
                sh, sm = divmod(start, 60)
                eh, em = divmod(end, 60)
                time_range = f" {sh:02d}:{sm:02d}-{eh:02d}:{em:02d}"
            price_text = f"{price:.0f} kr/tim{time_range}"

        # Fallback: amountForOtherTimes
        other_price = fz.get("amountForOtherTimes")
        if price is None and other_price and other_price > 0 and other_price < 99999999:
            price = other_price

        owner = z.get("owner", {}) or {}

        results.append({
            "id": f"pk_{zid}",
            "name": z.get("name", f"Parkster {zid}"),
            "lat": round(float(lat), 6),
            "lon": round(float(lon), 6),
            "price_sek_hr": price if price and price < 99999999 else None,
            "price_text": price_text,
            "time_limit": None,
            "max_daily_sek": None,
            "season_start": None,
            "season_end": None,
            "free_minutes": None,
            "permit_required": False,
            "service_fee": False,
            "area_code": str(z.get("zoneCode", "")),
            "type": "street",  # Parkster is primarily street parking
            "source": "parkster",
            "operator": owner.get("name", ""),
            "area_type_raw": z.get("type", ""),
            "status": "ACTIVE",
        })
    return results


def load_parkering_gbg() -> list[dict]:
    """Load Parkering Göteborg data."""
    path = DATA_DIR / "gothenburg_parking_complete.json"
    if not path.exists():
        print("  WARNING: gothenburg_parking_complete.json not found")
        return []

    areas = json.loads(path.read_text())
    results = []
    for a in areas:
        lat_raw = a.get("lat")
        lon_raw = a.get("lon")

        # Handle GeoJSON-style [lon, lat] arrays
        if isinstance(lat_raw, list):
            # lat field contains [lon, lat] pair; lon field also [lon, lat]
            # Use first pair
            try:
                lon_val = float(lat_raw[0])
                lat_val = float(lat_raw[1])
            except (IndexError, TypeError, ValueError):
                continue
        else:
            lat_val = lat_raw
            lon_val = lon_raw

        if not lat_val or not lon_val:
            continue

        try:
            lat_val = float(lat_val)
            lon_val = float(lon_val)
        except (TypeError, ValueError):
            continue

        # Always parse from raw text (source pre-parsed prices can be wrong)
        raw = a.get("price_info_raw", [])
        raw_text = " ".join(raw) if isinstance(raw, list) else str(raw or "")
        price = parse_sek_per_hour(raw_text) if raw_text else None
        time_limit = parse_time_limit(raw_text) if raw_text else None
        max_daily = parse_max_daily(raw_text) if raw_text else None
        season = parse_season(raw_text) if raw_text else None
        free_mins = parse_free_minutes(raw_text) if raw_text else None
        raw_lower = raw_text.lower()
        permit_req = bool(re.search(r"p-tillstånd|tillstånd\s+(?:erfordras|gäller|krävs)", raw_lower))
        has_svc_fee = "serviceavgift" in raw_lower

        ptype = a.get("parking_type", "")
        # timeLimited + free = time-limited free parking (duration on sign only)
        is_time_limited_free = (ptype == "timeLimited"
                                and (price is None or price == 0)
                                and "gratis" in raw_text.lower())

        results.append({
            "id": f"pg_{a.get('id', '')}",
            "name": a.get("name", ""),
            "lat": round(lat_val, 6),
            "lon": round(lon_val, 6),
            "price_sek_hr": price,
            "price_text": raw_text.strip().rstrip(",").strip() if raw_text else "",
            "time_limit": time_limit,
            "time_limited_free": is_time_limited_free,
            "max_daily_sek": max_daily,
            "season_start": season[0] if season else None,
            "season_end": season[1] if season else None,
            "free_minutes": free_mins,
            "permit_required": permit_req,
            "service_fee": has_svc_fee,
            "area_code": str(a.get("parking_code", "") or ""),
            "type": classify_type(ptype),
            "source": "parkering_gbg",
            "operator": "Göteborgs Stad",
            "area_type_raw": ptype,
            "status": "ACTIVE",
            "total_spaces": a.get("total_spaces"),
            "free_spaces": a.get("free_spaces"),
            "has_charging": a.get("has_charging", False),
        })

    # Apply manual corrections for known data errors
    corrections_applied = 0
    for spot in results:
        for corr in PGBG_CORRECTIONS:
            if (corr["name_match"] in spot["name"].lower()
                    and corr["lat_range"][0] <= spot["lat"] <= corr["lat_range"][1]
                    and corr["lon_range"][0] <= spot["lon"] <= corr["lon_range"][1]):
                spot.update(corr["override"])
                corrections_applied += 1
    if corrections_applied:
        print(f"  Applied {corrections_applied} manual corrections")

    return results


def load_epark(gbg_spots: list[dict] | None = None) -> list[dict]:
    """Load ePARK zones from pre-fetched data.
    
    ePARK zone detail has NO coordinates, so we resolve lat/lon by:
    1. Matching public_area_code → P.GBG spot coordinates
    2. Geocoding remaining via Nominatim (street name + Göteborg)
    
    Args:
        gbg_spots: List of already-loaded P.GBG spots for coordinate lookup.
    """
    import time
    import requests as _req

    path = DATA_DIR / "epark_gothenburg_zones.json"
    if not path.exists():
        print("  WARNING: epark_gothenburg_zones.json not found")
        return []

    zones = json.loads(path.read_text())
    if not zones:
        print("  WARNING: epark_gothenburg_zones.json is empty")
        return []

    # Build P.GBG coordinate lookup: area_code → (lat, lon)
    gbg_coord_map = {}
    if gbg_spots:
        for s in gbg_spots:
            code = s.get("area_code", "")
            if code and s.get("lat") and s.get("lon"):
                gbg_coord_map.setdefault(code, (s["lat"], s["lon"]))

    # Group ePARK zones by public_area_code to avoid duplicates.
    # Many ePARK zones are individual street segments within the same P.GBG zone.
    # We keep one representative per unique (public_area_code, title) pair.
    seen = set()
    unique_zones = []
    for z in zones:
        code = str(z.get("public_area_code") or "")
        title = z.get("title", "")
        key = (code, title)
        if key not in seen:
            seen.add(key)
            unique_zones.append(z)

    # Geocode cache for ePARK-exclusive zones (persistent file cache)
    cache_path = DATA_DIR / "epark_geocode_cache.json"
    if cache_path.exists():
        geocode_cache = json.loads(cache_path.read_text())
        # Convert stored lists back to tuples, keep None as None
        geocode_cache = {k: tuple(v) if v else None for k, v in geocode_cache.items()}
    else:
        geocode_cache = {}
    cache_dirty = False

    def geocode_street(street_name: str) -> tuple[float, float] | None:
        """Geocode a street name in Göteborg via Nominatim."""
        nonlocal cache_dirty
        if street_name in geocode_cache:
            return geocode_cache[street_name]
        for attempt in range(3):
            try:
                r = _req.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": f"{street_name}, Göteborg, Sweden", "format": "json", "limit": "1"},
                    headers={"User-Agent": "ParkingGBG-DataMerge/1.0"},
                    timeout=10,
                )
                if r.status_code == 429:
                    time.sleep(5 * (attempt + 1))
                    continue
                results = r.json()
                if results:
                    lat = float(results[0]["lat"])
                    lon = float(results[0]["lon"])
                    geocode_cache[street_name] = (lat, lon)
                    cache_dirty = True
                    return (lat, lon)
                break  # got valid response with no results
            except Exception:
                time.sleep(2)
        geocode_cache[street_name] = None
        cache_dirty = True
        return None

    results = []
    geocoded_count = 0
    skipped_no_coords = 0
    skip_geocode = os.environ.get("SKIP_GEOCODE", "").strip() == "1"
    if skip_geocode:
        print("  SKIP_GEOCODE=1: skipping Nominatim geocoding for unmatched zones")

    for z in unique_zones:
        zid = z.get("id")
        code = str(z.get("public_area_code") or "")
        title = z.get("title", "")

        # Resolve coordinates
        lat, lon = None, None
        if code in gbg_coord_map:
            lat, lon = gbg_coord_map[code]
        elif title in geocode_cache and geocode_cache[title] is not None:
            # Use persistent cache (no network needed)
            lat, lon = geocode_cache[title]
        elif not skip_geocode:
            # Geocode using street name via Nominatim
            coords = geocode_street(title)
            if coords:
                lat, lon = coords
                geocoded_count += 1
                # Save cache periodically to preserve progress
                if cache_dirty and geocoded_count % 50 == 0:
                    cache_path.write_text(json.dumps(
                        {k: list(v) if v else None for k, v in geocode_cache.items()},
                        indent=1,
                    ))
                    cache_dirty = False
                    print(f"  ... geocoded {geocoded_count} so far (cache saved)")
                time.sleep(1.1)  # Nominatim rate limit: 1 req/sec

        if not lat or not lon:
            skipped_no_coords += 1
            continue

        # Parse pricing from description text
        desc = z.get("description") or []
        raw_text = " ".join(str(d) for d in desc if d)
        price = parse_sek_per_hour(raw_text)
        time_limit = parse_time_limit(raw_text)
        max_daily = parse_max_daily(raw_text)
        season = parse_season(raw_text)
        free_mins = parse_free_minutes(raw_text)
        raw_lower = raw_text.lower()
        permit_req = bool(re.search(r"p-tillstånd|tillstånd\s+(?:erfordras|gäller|krävs)", raw_lower))
        has_svc_fee = "serviceavgift" in raw_lower

        price_display = str(desc[0])[:100] if desc and desc[0] else ""

        operator = (z.get("operator") or {}).get("name", "")
        has_ev = bool(z.get("charge_points"))

        results.append({
            "id": f"ek_{zid}",
            "name": title,
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "price_sek_hr": price,
            "price_text": price_display,
            "time_limit": time_limit,
            "max_daily_sek": max_daily,
            "season_start": season[0] if season else None,
            "season_end": season[1] if season else None,
            "free_minutes": free_mins,
            "permit_required": permit_req,
            "service_fee": has_svc_fee,
            "area_code": code,
            "type": "ev" if has_ev else "street",
            "source": "epark",
            "operator": operator,
            "area_type_raw": "",
            "status": "ACTIVE",
        })

    # Persist geocode cache to disk
    if cache_dirty:
        # Convert tuples to lists for JSON serialization
        cache_path.write_text(json.dumps(
            {k: list(v) if v else None for k, v in geocode_cache.items()},
            indent=1,
        ))
        print(f"  Saved geocode cache ({len(geocode_cache)} entries)")

    if geocoded_count:
        print(f"  Geocoded {geocoded_count} ePARK-exclusive zones via Nominatim")
    if skipped_no_coords:
        print(f"  Skipped {skipped_no_coords} zones (no coordinates)")

    return results


# ── Deduplication ──

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in metres between two points."""
    from math import radians, sin, cos, sqrt, atan2
    R = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _normalize_name(name: str) -> str:
    """Strip leading zone numbers and lowercase for matching."""
    return re.sub(r"^\d+\s+", "", name).strip().lower()


def _merge_group(group: list[dict]) -> dict:
    """Merge a group of spots (same physical location, different sources) into one."""
    # Priority: prefer GBG (has spaces), then EasyPark (has prices), then Parkster, then ePARK
    SOURCE_PRIORITY = {"parkering_gbg": 0, "easypark": 1, "parkster": 2, "epark": 3}
    group.sort(key=lambda s: SOURCE_PRIORITY.get(s["source"], 9))
    primary = group[0]

    # Collect all sources, area codes, and app info
    sources = list(dict.fromkeys(s["source"] for s in group))  # ordered unique
    area_codes = {}
    for s in group:
        if s.get("area_code"):
            area_codes[s["source"]] = s["area_code"]
        if s.get("gbg_code"):
            area_codes.setdefault("parkering_gbg", s["gbg_code"])

    # Best price: prefer lowest non-zero price (0 often means permit-only free
    # zone merged with a nearby paid zone). Only use 0 if ALL sources are free.
    priced = [s for s in group if s["price_sek_hr"] is not None]
    paid = [s for s in priced if s["price_sek_hr"] > 0]
    if paid:
        best_price = min(paid, key=lambda s: s["price_sek_hr"])
    elif priced:
        best_price = priced[0]  # all are 0
    else:
        best_price = primary
    
    # Best price text: prefer longest (most detailed)
    best_text = max(group, key=lambda s: len(s.get("price_text") or ""))

    # Best time limit
    time_limits = [s["time_limit"] for s in group if s.get("time_limit")]
    time_limit = time_limits[0] if time_limits else None

    # Max daily
    max_dailies = [s["max_daily_sek"] for s in group if s.get("max_daily_sek")]
    max_daily = min(max_dailies) if max_dailies else None

    # Time-limited free flag
    tlf = any(s.get("time_limited_free") for s in group)

    # Spaces info (from GBG)
    gbg_spots = [s for s in group if s["source"] == "parkering_gbg"]
    total_spaces = None
    free_spaces = None
    has_charging = False
    for g in gbg_spots:
        if g.get("total_spaces"):
            total_spaces = g["total_spaces"]
            free_spaces = g.get("free_spaces")
        if g.get("has_charging"):
            has_charging = True

    # Use the best name (prefer without zone prefix if available)
    names = [s["name"] for s in group]
    # Prefer GBG name (no zone prefix), else shortest EP name
    best_name = primary["name"]
    for s in group:
        if s["source"] == "parkering_gbg" and s["name"]:
            best_name = s["name"]
            break

    # Type: prefer non-'other', prefer non-'street' if garage/lot exists
    type_priority = {"garage": 0, "lot": 1, "ev": 2, "street": 3, "other": 4}
    best_type = min(group, key=lambda s: type_priority.get(s["type"], 9))["type"]

    merged = {
        "id": primary["id"],
        "name": best_name,
        "lat": primary["lat"],
        "lon": primary["lon"],
        "price_sek_hr": best_price["price_sek_hr"],
        "price_text": best_text.get("price_text", ""),
        "time_limit": time_limit,
        "time_limited_free": tlf,
        "max_daily_sek": max_daily,
        "season_start": next((s.get("season_start") for s in group if s.get("season_start")), None),
        "season_end": next((s.get("season_end") for s in group if s.get("season_end")), None),
        "free_minutes": next((s.get("free_minutes") for s in group if s.get("free_minutes")), None),
        "permit_required": any(s.get("permit_required") for s in group),
        "service_fee": any(s.get("service_fee") for s in group),
        "area_codes": area_codes,
        "sources": sources,
        "type": best_type,
        "operator": primary.get("operator", ""),
        "area_type_raw": primary.get("area_type_raw", ""),
        "status": "ACTIVE",
        "total_spaces": total_spaces,
        "free_spaces": free_spaces,
        "has_charging": has_charging,
    }
    return merged


def deduplicate(spots: list[dict]) -> list[dict]:
    """Merge spots from different sources that represent the same physical location.
    
    Matching strategies (in order):
    1. EasyPark gbg_code → GBG area_code (exact code cross-reference)
    2. Parkster area_code → GBG area_code (zone codes match)
    3. ePARK area_code → GBG area_code (zone codes match)
    4. Proximity (< 80m) + normalized name match (catch remaining)
    """
    # Index spots by id for fast lookup
    by_id = {s["id"]: s for s in spots}
    # Track which spots have been merged (id → group_key)
    merged_into = {}
    # Groups: group_key → [spot, ...]
    groups = {}
    group_counter = 0

    def add_to_group(spot_id: str, partner_id: str):
        nonlocal group_counter
        gk_a = merged_into.get(spot_id)
        gk_b = merged_into.get(partner_id)
        if gk_a and gk_b:
            if gk_a == gk_b:
                return  # already same group
            # Merge the two groups
            for s in groups[gk_b]:
                merged_into[s["id"]] = gk_a
            groups[gk_a].extend(groups.pop(gk_b))
        elif gk_a:
            merged_into[partner_id] = gk_a
            groups[gk_a].append(by_id[partner_id])
        elif gk_b:
            merged_into[spot_id] = gk_b
            groups[gk_b].append(by_id[spot_id])
        else:
            group_counter += 1
            gk = group_counter
            merged_into[spot_id] = gk
            merged_into[partner_id] = gk
            groups[gk] = [by_id[spot_id], by_id[partner_id]]

    # Strategy 1: EP gbg_code → GBG area_code
    gbg_by_code = {}
    for s in spots:
        if s["source"] == "parkering_gbg" and s.get("area_code"):
            gbg_by_code.setdefault(s["area_code"], []).append(s)

    for s in spots:
        if s["source"] == "easypark" and s.get("gbg_code"):
            gbg_matches = gbg_by_code.get(s["gbg_code"], [])
            if gbg_matches:
                # Find closest GBG match
                closest = min(gbg_matches,
                              key=lambda g: _haversine(s["lat"], s["lon"], g["lat"], g["lon"]))
                if _haversine(s["lat"], s["lon"], closest["lat"], closest["lon"]) < 200:
                    add_to_group(s["id"], closest["id"])

    # Strategy 2: Parkster area_code → GBG area_code
    for s in spots:
        if s["source"] == "parkster" and s.get("area_code"):
            gbg_matches = gbg_by_code.get(s["area_code"], [])
            if gbg_matches:
                closest = min(gbg_matches,
                              key=lambda g: _haversine(s["lat"], s["lon"], g["lat"], g["lon"]))
                if _haversine(s["lat"], s["lon"], closest["lat"], closest["lon"]) < 200:
                    add_to_group(s["id"], closest["id"])

    # Strategy 3: ePARK area_code → GBG area_code
    for s in spots:
        if s["source"] == "epark" and s.get("area_code"):
            gbg_matches = gbg_by_code.get(s["area_code"], [])
            if gbg_matches:
                closest = min(gbg_matches,
                              key=lambda g: _haversine(s["lat"], s["lon"], g["lat"], g["lon"]))
                if _haversine(s["lat"], s["lon"], closest["lat"], closest["lon"]) < 200:
                    add_to_group(s["id"], closest["id"])

    # Strategy 4: Proximity + name match for remaining unmatched
    # Build spatial index by name
    from collections import defaultdict
    name_index = defaultdict(list)
    for s in spots:
        name_index[_normalize_name(s["name"])].append(s)

    for name, candidates in name_index.items():
        if len(candidates) < 2:
            continue
        # Only match across different sources
        for i, a in enumerate(candidates):
            for b in candidates[i + 1:]:
                if a["source"] == b["source"]:
                    continue
                if a["id"] in merged_into and b["id"] in merged_into:
                    if merged_into[a["id"]] == merged_into[b["id"]]:
                        continue  # already grouped
                if _haversine(a["lat"], a["lon"], b["lat"], b["lon"]) < 80:
                    add_to_group(a["id"], b["id"])

    # Build result: merged groups + singletons
    result = []
    seen = set()
    for gk, group in groups.items():
        merged = _merge_group(group)
        result.append(merged)
        for s in group:
            seen.add(s["id"])

    # Add unmatched singletons
    for s in spots:
        if s["id"] not in seen:
            # Convert to unified format (single-source spot)
            s_out = {
                "id": s["id"],
                "name": s["name"],
                "lat": s["lat"],
                "lon": s["lon"],
                "price_sek_hr": s["price_sek_hr"],
                "price_text": s.get("price_text", ""),
                "time_limit": s.get("time_limit"),
                "time_limited_free": s.get("time_limited_free", False),
                "max_daily_sek": s.get("max_daily_sek"),
                "season_start": s.get("season_start"),
                "season_end": s.get("season_end"),
                "free_minutes": s.get("free_minutes"),
                "permit_required": s.get("permit_required", False),
                "service_fee": s.get("service_fee", False),
                "area_codes": {s["source"]: s["area_code"]} if s.get("area_code") else {},
                "sources": [s["source"]],
                "type": s["type"],
                "operator": s.get("operator", ""),
                "area_type_raw": s.get("area_type_raw", ""),
                "status": s.get("status", "ACTIVE"),
                "total_spaces": s.get("total_spaces"),
                "free_spaces": s.get("free_spaces"),
                "has_charging": s.get("has_charging", False),
            }
            result.append(s_out)

    return result


def merge_all() -> dict:
    """Merge all sources into unified dataset."""
    print("Loading Parkering Göteborg...")
    pg = load_parkering_gbg()
    print(f"  {len(pg)} areas ({sum(1 for a in pg if a['price_sek_hr'])} with prices)")

    # Build GBG parking_code set for cross-referencing EasyPark
    gbg_codes = {a["area_code"] for a in pg if a.get("area_code")}

    print("Loading EasyPark...")
    ep = load_easypark(gbg_codes=gbg_codes)
    print(f"  {len(ep)} areas ({sum(1 for a in ep if a['price_sek_hr'])} with prices)")
    gbg_xref = sum(1 for a in ep if a.get("gbg_code"))
    print(f"  {gbg_xref} cross-referenced with GBG parking codes")

    print("Loading Parkster...")
    pk = load_parkster()
    print(f"  {len(pk)} zones ({sum(1 for a in pk if a['price_sek_hr'])} with prices)")

    print("Loading ePARK...")
    ek = load_epark(gbg_spots=pg)
    print(f"  {len(ek)} zones ({sum(1 for a in ek if a['price_sek_hr'])} with prices)")

    all_spots = ep + pk + pg + ek
    print(f"\nBefore dedup: {len(all_spots)} parking spots")

    # Stats by source
    for src in ["easypark", "parkster", "parkering_gbg", "epark"]:
        subset = [a for a in all_spots if a["source"] == src]
        with_price = sum(1 for a in subset if a["price_sek_hr"])
        print(f"  {src}: {len(subset)} total, {with_price} priced")

    # Deduplicate cross-source
    print("\nDeduplicating...")
    deduped = deduplicate(all_spots)
    multi = sum(1 for s in deduped if len(s["sources"]) > 1)
    print(f"  Merged: {len(all_spots)} → {len(deduped)} spots ({multi} multi-source)")

    print(f"\nAfter dedup: {len(deduped)} parking spots")
    print(f"  With prices: {sum(1 for a in deduped if a['price_sek_hr'])}")

    # Stats by type
    types = {}
    for a in deduped:
        types[a["type"]] = types.get(a["type"], 0) + 1
    print(f"  Types: {types}")

    return {
        "generated": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "easypark": len(ep),
            "parkster": len(pk),
            "parkering_gbg": len(pg),
            "epark": len(ek),
        },
        "total": len(deduped),
        "spots": deduped,
    }


def main():
    dataset = merge_all()

    # Save to repo root (served by GitHub Pages)
    out_path = ROOT_DIR / "parking_data.json"
    out_path.write_text(json.dumps(dataset, ensure_ascii=False))
    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"\nSaved to {out_path} ({size_mb:.1f} MB)")

    # Also save a copy in data/
    data_copy = DATA_DIR / "parking_merged.json"
    data_copy.write_text(json.dumps(dataset, ensure_ascii=False))
    print(f"Saved copy to {data_copy}")


if __name__ == "__main__":
    main()
