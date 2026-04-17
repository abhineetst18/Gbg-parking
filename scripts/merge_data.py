"""Merge all parking data sources into a unified JSON for the web app."""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
ROOT_DIR = Path(__file__).resolve().parent.parent


def parse_sek_per_hour(text: str) -> float | None:
    """Extract SEK/hour from Swedish pricing text. Picks the primary (first/daytime) rate."""
    if not text:
        return None
    # Collect all price matches with their position in the text
    candidates = []
    # "X kr/15 min" → X*4 per hour
    for m in re.finditer(r"(\d+)\s*kr\s*/?\s*15\s*min", text):
        candidates.append((m.start(), float(m.group(1)) * 4))
    # "X kr/30 min" → X*2 per hour
    for m in re.finditer(r"(\d+)\s*kr\s*/?\s*30\s*min", text):
        candidates.append((m.start(), float(m.group(1)) * 2))
    # "X kr/tim", "X kr/timme", "X kr tim"
    for m in re.finditer(r"(\d+)\s*kr\s*/?\s*tim", text):
        candidates.append((m.start(), float(m.group(1))))
    # "X kr/h" or "Xkr/h" (compact format)
    for m in re.finditer(r"(\d+)\s*kr\s*/\s*h\b", text):
        candidates.append((m.start(), float(m.group(1))))
    # "X kr/påbörjad timme" (per started hour)
    for m in re.finditer(r"(\d+)\s*kr\s*/?\s*påbörjad\s*tim", text):
        candidates.append((m.start(), float(m.group(1))))
    if candidates:
        # Return the first occurrence (primary/daytime rate)
        candidates.sort(key=lambda c: c[0])
        return candidates[0][1]
    return None


def parse_time_limit(text: str) -> str | None:
    """Extract parking time limit from Swedish text. Returns e.g. '24h', '2h', '10min'."""
    if not text:
        return None
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


def parse_max_daily(text: str) -> float | None:
    """Extract max daily rate from text like 'maxtaxa 30 kr/dag'."""
    if not text:
        return None
    m = re.search(r"maxtaxa\s*(\d+)\s*kr", text, re.I)
    if m:
        return float(m.group(1))
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


def load_easypark() -> list[dict]:
    """Load EasyPark areas from complete + summary data."""
    path = DATA_DIR / "easypark_gothenburg_complete.json"
    if not path.exists():
        print("  WARNING: easypark_gothenburg_complete.json not found")
        return []

    raw = json.loads(path.read_text())
    results = []
    for ano, record in raw.items():
        detail = record.get("areaDetail", {})
        lat = detail.get("displayPoint", {}).get("lat")
        lon = detail.get("displayPoint", {}).get("lon")
        if not lat or not lon:
            continue

        popup = detail.get("popUpMessage", "") or ""
        price_info = detail.get("priceInfo") or ""
        price_text = popup or str(price_info)
        price = parse_sek_per_hour(price_text)
        time_limit = parse_time_limit(popup)
        max_daily = parse_max_daily(popup)

        area_type = detail.get("areaType", "")
        custom_type = record.get("tileData", {}).get("customAreaType", "")

        results.append({
            "id": f"ep_{ano}",
            "name": detail.get("areaName", f"EasyPark {ano}"),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "price_sek_hr": price,
            "price_text": popup.split("\n")[0] if popup else "",
            "time_limit": time_limit,
            "max_daily_sek": max_daily,
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

        ptype = a.get("parking_type", "")

        results.append({
            "id": f"pg_{a.get('id', '')}",
            "name": a.get("name", ""),
            "lat": round(lat_val, 6),
            "lon": round(lon_val, 6),
            "price_sek_hr": price,
            "price_text": raw_text.split(",")[0] if raw_text else "",
            "time_limit": time_limit,
            "max_daily_sek": None,
            "type": classify_type(ptype),
            "source": "parkering_gbg",
            "operator": "Göteborgs Stad",
            "area_type_raw": ptype,
            "status": "ACTIVE",
            "total_spaces": a.get("total_spaces"),
            "free_spaces": a.get("free_spaces"),
            "has_charging": a.get("has_charging", False),
        })
    return results


def merge_all() -> dict:
    """Merge all sources into unified dataset."""
    print("Loading EasyPark...")
    ep = load_easypark()
    print(f"  {len(ep)} areas ({sum(1 for a in ep if a['price_sek_hr'])} with prices)")

    print("Loading Parkster...")
    pk = load_parkster()
    print(f"  {len(pk)} zones ({sum(1 for a in pk if a['price_sek_hr'])} with prices)")

    print("Loading Parkering Göteborg...")
    pg = load_parkering_gbg()
    print(f"  {len(pg)} areas ({sum(1 for a in pg if a['price_sek_hr'])} with prices)")

    all_spots = ep + pk + pg
    print(f"\nTotal: {len(all_spots)} parking spots")
    print(f"  With prices: {sum(1 for a in all_spots if a['price_sek_hr'])}")
    print(f"  With coords: {sum(1 for a in all_spots if a['lat'] and a['lon'])}")

    # Stats by source
    for src in ["easypark", "parkster", "parkering_gbg"]:
        subset = [a for a in all_spots if a["source"] == src]
        with_price = sum(1 for a in subset if a["price_sek_hr"])
        print(f"  {src}: {len(subset)} total, {with_price} priced")

    # Stats by type
    types = {}
    for a in all_spots:
        types[a["type"]] = types.get(a["type"], 0) + 1
    print(f"  Types: {types}")

    return {
        "generated": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "easypark": len(ep),
            "parkster": len(pk),
            "parkering_gbg": len(pg),
        },
        "total": len(all_spots),
        "spots": all_spots,
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
