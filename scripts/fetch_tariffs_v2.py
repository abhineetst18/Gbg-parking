"""Batch-fetch parking data from EasyPark and Parkster APIs."""
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


def curl_json(url: str, extra_headers: list[str] | None = None) -> tuple[dict | list | None, int]:
    """Run curl, return (parsed_json, http_code)."""
    cmd = ["curl", "-s", "-w", "\n__HTTP_CODE__%{http_code}", url]
    if extra_headers:
        cmd.extend(extra_headers)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    output = result.stdout
    # Extract HTTP code from end
    http_code = 0
    if "__HTTP_CODE__" in output:
        parts = output.rsplit("__HTTP_CODE__", 1)
        output = parts[0]
        try:
            http_code = int(parts[1].strip())
        except ValueError:
            pass
    if not output.strip():
        return None, http_code
    try:
        return json.loads(output), http_code
    except json.JSONDecodeError:
        return None, http_code


def batch_easypark():
    """Fetch full area details for all EasyPark Gothenburg areas using areaNo lookup."""
    areas_file = DATA_DIR / "easypark_gothenburg_areas.json"
    token_file = Path("/tmp/easypark_token.txt")

    if not token_file.exists():
        print("ERROR: No token at /tmp/easypark_token.txt")
        return

    token = token_file.read_text().strip()
    areas = json.loads(areas_file.read_text())

    # Deduplicate by areaNo
    area_nos = {}
    for a in areas:
        ano = a.get("areaNo")
        if ano:
            area_nos[ano] = a

    print(f"Fetching details for {len(area_nos)} unique areaNos...")

    auth_headers = ["-H", f"x-authorization: Bearer {token}", *EASYPARK_HEADERS]
    results = {}
    tariff_count = 0
    errors = 0

    for i, (area_no, tile_area) in enumerate(area_nos.items()):
        # Step 1: Get full area info via areaNo
        url = f"{EASYPARK_BASE}/ios/api/parkingarea?areaNo={area_no}&countryCode=SE"
        data, code = curl_json(url, auth_headers)

        if code == 200 and data:
            area_id = data.get("id", area_no)
            result = {
                "areaNo": area_no,
                "areaDetail": data,
                "tileData": tile_area,
            }

            # Step 2: If not externally rated, fetch structured tariff
            if not data.get("externallyRated", True):
                tariff_url = f"{EASYPARK_BASE}/ios/api/parkingarea/{area_id}/tariff"
                tariff, tcode = curl_json(tariff_url, auth_headers)
                if tcode == 200 and tariff and tariff.get("units"):
                    result["tariff"] = tariff
                    tariff_count += 1

            results[str(area_no)] = result
        else:
            errors += 1

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(area_nos)} done ({len(results)} areas, {tariff_count} tariffs, {errors} errors)")
            time.sleep(0.3)

    print(f"Done: {len(results)} areas fetched, {tariff_count} with tariffs, {errors} errors")

    # Save raw results
    outfile = DATA_DIR / "easypark_gothenburg_complete.json"
    outfile.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"Saved to: {outfile}")

    # Create summary with extracted pricing
    summary = []
    for ano, r in results.items():
        detail = r["areaDetail"]
        tile = r["tileData"]
        entry = {
            "areaNo": ano,
            "id": detail.get("id"),
            "name": detail.get("areaName", ""),
            "city": detail.get("city", ""),
            "operator": detail.get("parkingOperatorName", ""),
            "lat": detail.get("displayPoint", {}).get("lat"),
            "lon": detail.get("displayPoint", {}).get("lon"),
            "areaType": detail.get("areaType", ""),
            "status": detail.get("status", ""),
            "externallyRated": detail.get("externallyRated", False),
            "popUpMessage": detail.get("popUpMessage", ""),
            "priceInfo": detail.get("priceInfo"),
            "parkingTypes": detail.get("parkingTypes", []),
        }
        if "tariff" in r:
            entry["tariff"] = r["tariff"]
        summary.append(entry)

    summary_file = DATA_DIR / "easypark_gothenburg_summary.json"
    summary_file.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"Summary saved to: {summary_file}")


def scan_parkster():
    """Grid-scan Gothenburg for Parkster zones using correct API params."""
    # Gothenburg bounding box (focused on city center + surrounding areas)
    lat_min, lat_max = 57.66, 57.76
    lon_min, lon_max = 11.90, 12.05
    step = 0.005  # ~500m grid, radius=500 in API
    radius = 500

    all_zone_ids = set()
    all_zones_brief = {}  # id -> brief zone data from search
    lat = lat_min
    row = 0
    total_points = 0

    while lat <= lat_max:
        lon = lon_min
        while lon <= lon_max:
            ts = int(time.time() * 1000)
            url = (f"{PARKSTER_BASE}/api/mobile/v2/parking-zones/location-search"
                   f"?clientTime={ts}&locale=en_SE&platform=ios&platformVersion=26.3.1&version=633"
                   f"&radius={radius}"
                   f"&searchLat={lat}&searchLong={lon}"
                   f"&userLat={lat}&userLong={lon}"
                   f"&userId=898018")
            data, code = curl_json(url, [
                "-H", f"authorization: {PARKSTER_AUTH}",
                "-H", "accept: application/json",
            ])
            if code == 200 and data:
                for key in ("parkingZonesAtPosition", "parkingZonesNearbyPosition"):
                    for z in data.get(key, []):
                        zid = z.get("id")
                        if zid and zid not in all_zone_ids:
                            all_zone_ids.add(zid)
                            all_zones_brief[zid] = {
                                "id": zid,
                                "name": z.get("name", ""),
                                "zoneCode": z.get("zoneCode", ""),
                                "parkingZoneType": z.get("parkingZoneType", ""),
                            }
            total_points += 1
            lon += step
            time.sleep(0.05)

        row += 1
        lat += step
        print(f"  Row {row}: lat={lat:.4f}, scanned {total_points} points, found {len(all_zone_ids)} zones")

    print(f"\nGrid scan complete: {len(all_zone_ids)} unique zones from {total_points} points")

    # Fetch full details for each zone
    print(f"Fetching zone details...")
    detailed = []
    errors = 0
    for i, zid in enumerate(sorted(all_zone_ids)):
        ts = int(time.time() * 1000)
        url = (f"{PARKSTER_BASE}/api/mobile/v2/parking-zones/{zid}"
               f"?clientTime={ts}&platform=ios&version=633&locale=en_SE")
        data, code = curl_json(url, [
            "-H", f"authorization: {PARKSTER_AUTH}",
            "-H", "accept: application/json",
        ])
        if code == 200 and data:
            detailed.append(data)
        else:
            errors += 1

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(all_zone_ids)} details fetched ({errors} errors)")
            time.sleep(0.3)

    print(f"Got details for {len(detailed)}/{len(all_zone_ids)} zones ({errors} errors)")

    # Save full details
    outfile = DATA_DIR / "parkster_gothenburg_zones.json"
    outfile.write_text(json.dumps(detailed, indent=2, ensure_ascii=False))
    print(f"Saved to: {outfile}")

    # Create summary with extracted pricing
    summary = []
    for z in detailed:
        fee = z.get("feeZone", {})
        fees = fee.get("parkingFees", [])
        entry = {
            "id": z.get("id"),
            "name": z.get("name", ""),
            "zoneCode": z.get("zoneCode", ""),
            "city": z.get("city", {}).get("name", ""),
            "owner": z.get("parkingZoneOwner", {}).get("name", ""),
            "type": z.get("parkingZoneType", ""),
            "lat": z.get("directionsLat"),
            "lon": z.get("directionsLong"),
            "amountForOtherTimes": fee.get("amountForOtherTimes"),
            "fees": [{
                "amountPerHour": f.get("amountPerHour"),
                "startTime": f.get("startTime"),
                "endTime": f.get("endTime"),
                "typeOfDay": f.get("typeOfDay"),
                "typeOfRule": f.get("typeOfRule"),
            } for f in fees],
        }
        summary.append(entry)

    summary_file = DATA_DIR / "parkster_gothenburg_summary.json"
    summary_file.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"Summary saved to: {summary_file}")


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)

    if len(sys.argv) > 1 and sys.argv[1] == "parkster":
        scan_parkster()
    elif len(sys.argv) > 1 and sys.argv[1] == "easypark":
        batch_easypark()
    else:
        print("=" * 60)
        print("PHASE 1: EasyPark area details")
        print("=" * 60)
        batch_easypark()
        print()
        print("=" * 60)
        print("PHASE 2: Parkster zones")
        print("=" * 60)
        scan_parkster()
