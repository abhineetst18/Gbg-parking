"""Batch-fetch tariffs from EasyPark BFF API and zones from Parkster API."""
import json
import subprocess
import time
import os
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

# EasyPark config
EASYPARK_BASE = "https://app-bff.europe.ezprk.net"
EASYPARK_HEADERS = [
    "-H", "easypark-application-build-number: 100.28843",
    "-H", "easypark-application-channel-name: IOS",
    "-H", "easypark-application-id: net.easypark.app",
    "-H", "easypark-application-market-country: SE",
    "-H", "easypark-application-version-number: 26.10.0",
    "-H", "user-agent: EP-ios/26.10.0, 26.3.1, iPhone",
]

# Parkster config
PARKSTER_BASE = "https://api.parkster.se"
PARKSTER_AUTH = os.environ.get("PARKSTER_AUTH", "")


def fetch_easypark_tariff(area_id: int, token: str) -> dict | None:
    """Fetch tariff for a single EasyPark area."""
    result = subprocess.run(
        ["curl", "-s", f"{EASYPARK_BASE}/ios/api/parkingarea/{area_id}/tariff",
         "-H", f"x-authorization: Bearer {token}",
         *EASYPARK_HEADERS],
        capture_output=True, text=True, timeout=15,
    )
    try:
        data = json.loads(result.stdout)
        if "units" in data:
            return data
    except (json.JSONDecodeError, KeyError):
        pass
    return None


def fetch_parkster_nearby(lat: float, lon: float) -> list[dict]:
    """Fetch Parkster zones near a lat/lon."""
    ts = int(time.time() * 1000)
    url = (f"{PARKSTER_BASE}/api/mobile/v2/parking-zones/location-search"
           f"?clientTime={ts}&locale=en_SE&platform=ios&version=633"
           f"&latitude={lat}&longitude={lon}")
    result = subprocess.run(
        ["curl", "-s", url,
         "-H", f"authorization: {PARKSTER_AUTH}",
         "-H", "accept: application/json"],
        capture_output=True, text=True, timeout=15,
    )
    try:
        data = json.loads(result.stdout)
        zones = data.get("parkingZonesNearbyPosition", [])
        zones += data.get("parkingZonesAtPosition", [])
        return zones
    except (json.JSONDecodeError, KeyError):
        return []


def fetch_parkster_zone_detail(zone_id: int) -> dict | None:
    """Fetch full zone detail including pricing."""
    ts = int(time.time() * 1000)
    url = (f"{PARKSTER_BASE}/api/mobile/v2/parking-zones/{zone_id}"
           f"?clientTime={ts}&platform=ios&version=633")
    result = subprocess.run(
        ["curl", "-s", url,
         "-H", f"authorization: {PARKSTER_AUTH}",
         "-H", "accept: application/json"],
        capture_output=True, text=True, timeout=15,
    )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def batch_easypark_tariffs():
    """Fetch tariffs for all known EasyPark Gothenburg areas."""
    areas_file = DATA_DIR / "easypark_gothenburg_areas.json"
    token_file = Path("/tmp/easypark_token.txt")

    if not token_file.exists():
        print("ERROR: No EasyPark token found at /tmp/easypark_token.txt")
        return

    token = token_file.read_text().strip()
    areas = json.loads(areas_file.read_text())
    area_ids = [a["id"] for a in areas]
    print(f"Fetching tariffs for {len(area_ids)} EasyPark areas...")

    tariffs = {}
    errors = 0
    for i, aid in enumerate(area_ids):
        tariff = fetch_easypark_tariff(aid, token)
        if tariff:
            tariffs[str(aid)] = tariff
        else:
            errors += 1

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(area_ids)} done ({len(tariffs)} tariffs, {errors} errors)")
            time.sleep(0.5)  # rate limit

    print(f"Done: {len(tariffs)} tariffs fetched, {errors} errors")

    # Save tariffs
    outfile = DATA_DIR / "easypark_tariffs.json"
    outfile.write_text(json.dumps(tariffs, indent=2, ensure_ascii=False))
    print(f"Saved to: {outfile}")

    # Merge tariffs into areas
    for area in areas:
        aid = str(area["id"])
        if aid in tariffs:
            area["tariff"] = tariffs[aid]

    merged_file = DATA_DIR / "easypark_gothenburg_complete.json"
    merged_file.write_text(json.dumps(areas, indent=2, ensure_ascii=False))
    print(f"Merged data saved to: {merged_file}")

    return tariffs


def scan_parkster_gothenburg():
    """Grid-scan Gothenburg for all Parkster zones."""
    # Gothenburg bounding box
    lat_min, lat_max = 57.63, 57.78
    lon_min, lon_max = 11.85, 12.10
    step = 0.01  # ~1km grid

    all_zones = {}
    lat = lat_min
    row = 0
    while lat <= lat_max:
        lon = lon_min
        while lon <= lon_max:
            zones = fetch_parkster_nearby(lat, lon)
            for z in zones:
                zid = z.get("id")
                if zid and zid not in all_zones:
                    all_zones[zid] = z
            lon += step
            time.sleep(0.1)  # rate limit
        row += 1
        lat += step
        print(f"  Row {row}: lat={lat:.3f}, total zones found: {len(all_zones)}")

    print(f"\nGrid scan complete: {len(all_zones)} unique Parkster zones")

    # Fetch full details for each zone (includes pricing)
    print(f"Fetching zone details for pricing...")
    detailed = {}
    for i, (zid, zone) in enumerate(all_zones.items()):
        detail = fetch_parkster_zone_detail(zid)
        if detail:
            detailed[zid] = detail
        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(all_zones)} details fetched")
            time.sleep(0.5)

    print(f"Got details for {len(detailed)}/{len(all_zones)} zones")

    # Save
    zones_list = list(detailed.values())
    outfile = DATA_DIR / "parkster_gothenburg_zones.json"
    outfile.write_text(json.dumps(zones_list, indent=2, ensure_ascii=False))
    print(f"Saved to: {outfile}")

    return zones_list


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)

    if len(sys.argv) > 1 and sys.argv[1] == "parkster":
        scan_parkster_gothenburg()
    elif len(sys.argv) > 1 and sys.argv[1] == "easypark":
        batch_easypark_tariffs()
    else:
        # Both
        print("=" * 60)
        print("PHASE 1: EasyPark tariffs")
        print("=" * 60)
        batch_easypark_tariffs()
        print()
        print("=" * 60)
        print("PHASE 2: Parkster zones")
        print("=" * 60)
        scan_parkster_gothenburg()
