"""Batch-fetch parking data from EasyPark and Parkster APIs.

The EasyPark path is resumable. Every schema-valid response is persisted to a
partial result file plus a checkpoint, so an interrupted, rate-limited or
quota-spanning run continues instead of refetching. The canonical complete and
summary files are only replaced once the refresh reaches the completeness
threshold, so a failed run can never publish a truncated dataset.
"""
import argparse
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
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

EASYPARK_SEED_FILE = "easypark_gothenburg_areas.json"
EASYPARK_PARTIAL_FILE = "easypark_gothenburg_partial.json"
EASYPARK_COMPLETE_FILE = "easypark_gothenburg_complete.json"
EASYPARK_SUMMARY_FILE = "easypark_gothenburg_summary.json"
EASYPARK_COVERAGE_FILE = "easypark_gothenburg_coverage.json"
EASYPARK_STATE_FILE = ".easypark_fetch_state.json"

CHECKPOINT_VERSION = 1
CHECKPOINT_EVERY = 25
COMPLETENESS_THRESHOLD = 0.99
DEFAULT_DELAY = 4.0
DEFAULT_MAX_RETRIES = 3
MAX_BACKOFF = 60.0
JITTER_FRACTION = 0.25
CONNECT_TIMEOUT = 10
REQUEST_TIMEOUT = 30

EXIT_OK = 0
EXIT_INCOMPLETE = 1
EXIT_AUTH = 2
EXIT_RATE_LIMITED = 3
EXIT_CONFIG = 4
EXIT_STATE = 5

# Only these response headers may be echoed to logs, and only after scrubbing.
_RATE_HEADER_NAMES = {"retry-after", "date"}
_RATE_HEADER_PREFIXES = ("x-ratelimit-", "ratelimit-", "x-rate-limit-")

# Redacts bearer tokens, JWT-shaped values and long opaque credentials.
_SECRET_RE = re.compile(
    r"(?:bearer\s+\S+)|(?:eyJ[\w\-]{4,}\.[\w\-.]+)|(?:[A-Za-z0-9_\-]{40,})",
    re.IGNORECASE,
)

# Parkster config
PARKSTER_BASE = "https://api.parkster.se"
PARKSTER_AUTH = os.environ.get("PARKSTER_AUTH", "")


def scrub(text: str) -> str:
    """Redact anything credential-shaped before it reaches a log."""
    return _SECRET_RE.sub("[REDACTED]", str(text))


def _normalize_bearer_token(value: str) -> str | None:
    """Normalize an auth token to a bare JWT, stripping the Bearer scheme if present.

    Returns the bare token after:
    - Stripping outer whitespace
    - Removing exactly one case-insensitive 'Bearer' prefix if present
    - Leaving bare tokens unchanged

    Returns None if the input is empty, whitespace-only, or contains only the
    Bearer scheme. Never logs the token value.
    """
    normalized = value.strip()
    if not normalized:
        return None

    # Check if normalized starts with 'bearer ' (case-insensitive)
    # and strip exactly one occurrence
    parts = normalized.split(None, 1)
    if parts[0].lower() == "bearer":
        if len(parts) < 2:
            # Scheme-only
            return None
        normalized = parts[1]

    return normalized


def _log(message: str) -> None:
    print(scrub(message))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_header_block(raw: str) -> dict[str, str]:
    """Parse the last HTTP response header block into a lowercase-keyed dict."""
    headers: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.upper().startswith("HTTP/"):
            headers = {}
            continue
        name, sep, value = line.partition(":")
        if sep:
            headers[name.strip().lower()] = value.strip()
    return headers


def curl_request(
    url: str,
    extra_headers: list[str] | None = None,
    timeout: int = REQUEST_TIMEOUT,
) -> tuple[dict | list | None, int, dict[str, str]]:
    """Run curl and return (parsed_json, http_status, response_headers).

    Body and headers are written to separate temp files so no separator can be
    forged by a response. Status 0 means the request never completed. The
    command line carries the auth header and is never logged.
    """
    body_fd, body_path = tempfile.mkstemp(prefix="ep_body_")
    header_fd, header_path = tempfile.mkstemp(prefix="ep_hdr_")
    os.close(body_fd)
    os.close(header_fd)
    cmd = [
        "curl", "-sS", "--compressed",
        "--connect-timeout", str(CONNECT_TIMEOUT),
        "--max-time", str(timeout),
        "-o", body_path,
        "-D", header_path,
        "-w", "%{http_code}",
        url,
    ]
    if extra_headers:
        cmd.extend(extra_headers)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 10)
        try:
            status = int(result.stdout.strip() or 0)
        except ValueError:
            status = 0
        body = Path(body_path).read_text(encoding="utf-8", errors="replace")
        headers = _parse_header_block(Path(header_path).read_text(encoding="utf-8", errors="replace"))
    except (subprocess.TimeoutExpired, subprocess.SubprocessError, OSError):
        return None, 0, {}
    finally:
        for path in (body_path, header_path):
            try:
                os.unlink(path)
            except OSError:
                pass
    data = None
    if body.strip():
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            data = None
    return data, status, headers


def curl_json(url: str, extra_headers: list[str] | None = None) -> tuple[dict | list | None, int]:
    """Backwards-compatible simple transport: return (parsed_json, http_code)."""
    data, status, _headers = curl_request(url, extra_headers, timeout=20)
    return data, status


def _stage_json(path: Path, payload) -> Path:
    """Write payload to a sibling .tmp file, fsynced, and return its path."""
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    return tmp


def _atomic_write_json(path: Path, payload) -> None:
    os.replace(_stage_json(path, payload), path)


def _seed_fingerprint(keys: list[str]) -> str:
    digest = hashlib.sha256(json.dumps(keys, ensure_ascii=False).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def is_schema_valid(record: object, area_no: str) -> bool:
    """A record is usable only if the merge pipeline can consume it."""
    if not isinstance(record, dict):
        return False
    if str(record.get("areaNo")) != str(area_no):
        return False
    if not isinstance(record.get("tileData"), dict):
        return False
    detail = record.get("areaDetail")
    if not isinstance(detail, dict):
        return False
    if detail.get("id") in (None, ""):
        return False
    if str(detail.get("areaNo")) != str(area_no):
        return False
    if not detail.get("areaType"):
        return False
    if not isinstance(detail.get("displayPoint"), dict):
        return False
    for field in ("popUpMessage", "freeTextTariffInfo", "priceInfo"):
        if detail.get(field) is not None and not isinstance(detail[field], str):
            return False
    return True


def _safe_rate_headers(headers: dict[str, str] | None) -> dict[str, str]:
    """Allowlisted, scrubbed rate-limit metadata for logging only."""
    if not isinstance(headers, dict):
        return {}
    safe = {}
    for name, value in headers.items():
        key = str(name).lower()
        if key in _RATE_HEADER_NAMES or key.startswith(_RATE_HEADER_PREFIXES):
            safe[key] = scrub(str(value))[:100]
    return safe


def _load_seed(
    seed_areas: list | None,
    seed_path: Path | None,
    log,
) -> dict[str, dict] | None:
    """Return an ordered {str(areaNo): tileData} map, deduplicated first-wins."""
    if seed_areas is None:
        if seed_path is None or not Path(seed_path).exists():
            log(f"ERROR: seed file not found: {seed_path}")
            return None
        try:
            seed_areas = json.loads(Path(seed_path).read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            log(f"ERROR: seed file unreadable: {type(exc).__name__}")
            return None
    if not isinstance(seed_areas, list):
        log("ERROR: seed file must contain a list of tile areas")
        return None
    areas: dict[str, dict] = {}
    for entry in seed_areas:
        if not isinstance(entry, dict):
            continue
        area_no = entry.get("areaNo")
        if area_no is None:
            continue
        key = str(area_no)
        if key not in areas:
            areas[key] = entry
    if not areas:
        log("ERROR: seed file contains no usable areaNo entries")
        return None
    return areas


def _new_state(refresh_id: str, started_at: str, fingerprint: str, seed_count: int,
               delay: float, with_tariff: bool) -> dict:
    return {
        "version": CHECKPOINT_VERSION,
        "refresh_id": refresh_id,
        "started_at": started_at,
        "updated_at": started_at,
        "seed_fingerprint": fingerprint,
        "seed_count": seed_count,
        "delay": delay,
        "with_tariff": with_tariff,
        "completed": {},
        "failures": {},
        "tombstones": {},
        "status_histogram": {},
    }


def _state_invariant_error(
    results: dict,
    completed: dict,
    tombstones: object,
    failures: object,
) -> str | None:
    """Every seed zone is live, tombstoned or unresolved - never two at once."""
    if not isinstance(tombstones, dict):
        return "checkpoint tombstone map is malformed"
    if not isinstance(failures, dict):
        return "checkpoint failure map is malformed"
    live = set(results)
    if live != set(completed):
        return "checkpoint completion map and stored results disagree"
    tombstoned = set(tombstones)
    unresolved = set(failures)
    for label, overlap in (
        ("live and tombstoned", live & tombstoned),
        ("live and unresolved", live & unresolved),
        ("tombstoned and unresolved", tombstoned & unresolved),
    ):
        if overlap:
            return f"areaNo {sorted(overlap)[0]} is marked both {label}"
    return None


def load_refresh_state(
    data_dir: Path,
    fingerprint: str,
    seed_count: int,
) -> tuple[dict | None, dict | None, str | None]:
    """Load a resumable checkpoint.

    Returns (state, results, error). A missing checkpoint and missing partial is
    a clean fresh start (None, None, None). Anything inconsistent is an error;
    the run must refuse rather than silently mix or silently reset.
    """
    state_path = data_dir / EASYPARK_STATE_FILE
    partial_path = data_dir / EASYPARK_PARTIAL_FILE
    if not state_path.exists():
        if partial_path.exists():
            return None, None, (
                f"partial results exist without a checkpoint ({partial_path.name}); "
                "refusing to overwrite them"
            )
        return None, None, None
    if not partial_path.exists():
        return None, None, (
            f"checkpoint {state_path.name} has no companion {partial_path.name}"
        )
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        partial = json.loads(partial_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return None, None, f"checkpoint or partial file unreadable ({type(exc).__name__})"
    if not isinstance(state, dict) or not isinstance(partial, dict):
        return None, None, "checkpoint or partial file has an unexpected shape"
    if state.get("version") != CHECKPOINT_VERSION:
        return None, None, (
            f"checkpoint version {state.get('version')!r} is not supported "
            f"(expected {CHECKPOINT_VERSION})"
        )
    refresh_id = state.get("refresh_id")
    if not refresh_id or partial.get("refresh_id") != refresh_id:
        return None, None, "checkpoint and partial file belong to different refreshes"
    if state.get("seed_fingerprint") != fingerprint:
        return None, None, "seed fingerprint changed since the checkpoint was written"
    if state.get("seed_count") != seed_count:
        return None, None, (
            f"seed count changed since the checkpoint was written "
            f"({state.get('seed_count')} -> {seed_count})"
        )
    completed = state.get("completed")
    raw_results = partial.get("results")
    if not isinstance(completed, dict) or not isinstance(raw_results, dict):
        return None, None, "checkpoint completion map or partial results map is malformed"
    results = {}
    for key in completed:
        record = raw_results.get(key)
        if not is_schema_valid(record, key):
            return None, None, (
                f"checkpoint marks areaNo {key} complete but the stored record is "
                "missing or malformed"
            )
        results[key] = record
    state.setdefault("failures", {})
    state.setdefault("status_histogram", {})
    # Additive within checkpoint v1: pre-tombstone checkpoints resume without a reset.
    state.setdefault("tombstones", {})
    invariant = _state_invariant_error(
        results, completed, state["tombstones"], state["failures"])
    if invariant:
        return None, None, invariant
    return state, results, None


def build_queue(seed_keys: list[str], priority_areas) -> list[str]:
    """Deduplicated first-appearance order with requested areas moved to front."""
    ordered = list(dict.fromkeys(seed_keys))
    front = []
    for area in priority_areas or ():
        key = str(area)
        if key in ordered and key not in front:
            front.append(key)
    return front + [key for key in ordered if key not in front]


def _build_summary(results: dict[str, dict]) -> list[dict]:
    """Extract the pricing summary schema consumed by downstream tooling."""
    summary = []
    for ano, record in results.items():
        detail = record["areaDetail"]
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
        if "tariff" in record:
            entry["tariff"] = record["tariff"]
        summary.append(entry)
    return summary


def _coverage_seed_labels(tile: object) -> dict[str, str]:
    """Public seed labels only; coordinates and geometry are never reported."""
    if not isinstance(tile, dict):
        return {"areaName": "", "operator": ""}
    return {
        "areaName": scrub(str(tile.get("areaName", "")))[:120],
        "operator": scrub(str(tile.get("parkingOperatorName", "")))[:120],
    }


def _build_coverage_report(
    *,
    refresh_id: str,
    generated_at: str,
    seed_count: int,
    results: dict,
    tombstones: dict,
    failures: dict,
    areas: dict[str, dict],
) -> dict:
    """Report-only resolution ledger published alongside the canonical files."""
    live_count = len(results)
    tombstone_count = len(tombstones)
    tombstone_rows = []
    for key in sorted(tombstones):
        entry = tombstones[key] if isinstance(tombstones[key], dict) else {}
        tombstone_rows.append({
            "areaNo": key,
            **_coverage_seed_labels(areas.get(key)),
            "internal_id": entry.get("internal_id"),
            "query_status": entry.get("query_status"),
            "id_status": entry.get("id_status"),
            "confirmed_at": entry.get("confirmed_at"),
        })
    unresolved_rows = []
    for key in sorted(failures):
        entry = failures[key] if isinstance(failures[key], dict) else {}
        tile = areas.get(key) if isinstance(areas.get(key), dict) else {}
        unresolved_rows.append({
            "areaNo": key,
            **_coverage_seed_labels(tile),
            "internal_id": tile.get("id"),
            "reason": entry.get("reason", "query_failed"),
            "last_status": entry.get("last_status"),
        })
    return {
        "refresh_id": refresh_id,
        "generated_at": generated_at,
        "seed_count": seed_count,
        "live_count": live_count,
        "tombstone_count": tombstone_count,
        "unresolved_count": len(failures),
        "coverage": round((live_count + tombstone_count) / seed_count, 4) if seed_count else 0.0,
        "tombstones": tombstone_rows,
        "unresolved": unresolved_rows,
    }


def _promote_outputs(data_dir: Path, results: dict[str, dict],
                     coverage_report: dict | None = None) -> None:
    """Stage every canonical file, then swap them in; leaves no .tmp behind."""
    complete_path = data_dir / EASYPARK_COMPLETE_FILE
    summary_path = data_dir / EASYPARK_SUMMARY_FILE
    staged = [
        (_stage_json(complete_path, results), complete_path),
        (_stage_json(summary_path, _build_summary(results)), summary_path),
    ]
    if coverage_report is not None:
        coverage_path = data_dir / EASYPARK_COVERAGE_FILE
        staged.append((_stage_json(coverage_path, coverage_report), coverage_path))
    for tmp, final in staged:
        os.replace(tmp, final)


def _cleanup_resumable_state(data_dir: Path, log=None) -> int | None:
    """Remove partial and state files after successful promotion.

    Returns None on success (both removed or already absent), or a nonzero exit code
    if an unexpected filesystem error occurs while canonical promotion succeeded.
    Missing files are harmless; unexpected errors are reported.
    """
    log = log or _log
    partial_path = data_dir / EASYPARK_PARTIAL_FILE
    state_path = data_dir / EASYPARK_STATE_FILE

    for path in (partial_path, state_path):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            log(f"WARNING: canonical promotion succeeded but cleanup of "
                f"{path.name} failed: {type(exc).__name__}")
            return EXIT_STATE
    return None


def run_easypark(
    *,
    data_dir: Path,
    token: str,
    seed_areas: list | None = None,
    seed_path: Path | None = None,
    transport=None,
    sleep_fn=None,
    rng=None,
    now_fn=None,
    id_fn=None,
    delay: float = DEFAULT_DELAY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    priority_areas=(),
    with_tariff: bool = False,
    limit: int | None = None,
    reset_state: bool = False,
    log=None,
) -> int:
    """Fetch EasyPark area details resumably. Returns a process exit code."""
    log = log or _log
    transport = transport or curl_request
    sleep_fn = sleep_fn or time.sleep
    rng = rng or random.Random()
    now_fn = now_fn or _utc_now
    id_fn = id_fn or (lambda: str(uuid.uuid4()))
    data_dir = Path(data_dir)

    # Normalize the token, stripping Bearer prefix if present
    normalized_token = _normalize_bearer_token(token)
    if normalized_token is None:
        if token.strip() and token.strip().lower().split()[0] == "bearer":
            log("ERROR: token contains only an authorization scheme; refusing to fetch")
        else:
            log("ERROR: no EasyPark token available; refusing to fetch")
        return EXIT_CONFIG

    areas = _load_seed(seed_areas, seed_path, log)
    if areas is None:
        return EXIT_CONFIG

    data_dir.mkdir(parents=True, exist_ok=True)
    seed_keys = list(areas)
    seed_count = len(seed_keys)
    fingerprint = _seed_fingerprint(seed_keys)

    if reset_state:
        state = _new_state(id_fn(), now_fn(), fingerprint, seed_count, delay, with_tariff)
        results: dict[str, dict] = {}
        log("Starting a new refresh (--reset-state); previous checkpoint ignored.")
    else:
        state, results, error = load_refresh_state(data_dir, fingerprint, seed_count)
        if error:
            log(f"ERROR: {error}")
            log("Refusing to continue. Re-run with --reset-state to start a clean refresh.")
            return EXIT_STATE
        if state is None:
            state = _new_state(id_fn(), now_fn(), fingerprint, seed_count, delay, with_tariff)
            results = {}
        else:
            results = results or {}
            log(f"Resuming refresh {state['refresh_id']}: "
                f"{len(state['completed'])}/{seed_count} already complete")
    state["delay"] = delay
    state["with_tariff"] = with_tariff

    auth_headers = ["-H", f"x-authorization: Bearer {normalized_token}", *EASYPARK_HEADERS]
    completed = state["completed"]
    failures = state["failures"]
    tombstones = state["tombstones"]
    histogram = state["status_histogram"]
    started = time.monotonic()
    since_checkpoint = 0

    def persist() -> None:
        state["updated_at"] = now_fn()
        _atomic_write_json(data_dir / EASYPARK_PARTIAL_FILE, {
            "version": CHECKPOINT_VERSION,
            "refresh_id": state["refresh_id"],
            "seed_fingerprint": fingerprint,
            "results": results,
        })
        _atomic_write_json(data_dir / EASYPARK_STATE_FILE, state)

    def request(url: str, backoff_attempt: int = 0):
        if backoff_attempt:
            wait = min(delay * (2 ** backoff_attempt), MAX_BACKOFF)
        else:
            wait = delay
        sleep_fn(wait + rng.random() * wait * JITTER_FRACTION)
        data, status, headers = transport(url, auth_headers)
        histogram[str(status)] = histogram.get(str(status), 0) + 1
        return data, status, headers

    def fetch_area(area_no: str, tile: dict):
        """Return (outcome, record, status, headers). Outcome drives the loop."""
        attempt = 0
        status = 0
        headers: dict[str, str] = {}
        while True:
            attempt += 1
            data, status, headers = request(
                f"{EASYPARK_BASE}/ios/api/parkingarea?areaNo={area_no}&countryCode=SE",
                backoff_attempt=attempt - 1,
            )
            if status == 200:
                record = {"areaNo": int(area_no) if area_no.isdigit() else area_no,
                          "areaDetail": data,
                          "tileData": tile}
                if is_schema_valid(record, area_no):
                    return "ok", record, status, headers
                if attempt <= max_retries:
                    continue
                return "failed", None, status, headers
            if status in (401, 403):
                return "auth", None, status, headers
            if status == 429:
                return "rate_limited", None, status, headers
            if status == 0 or status >= 500:
                if attempt <= max_retries:
                    continue
                return "failed", None, status, headers
            return "failed", None, status, headers

    def fetch_by_internal_id(area_no: str, tile: dict):
        """Disambiguate a query-form 404 via the app's internal-id endpoint.

        Only a second 404 proves the zone is gone; a 200 whose areaNo does not
        match the seed describes a different zone and stays unresolved.
        """
        internal_id = tile.get("id")
        if internal_id in (None, ""):
            return "no_internal_id", None, None, {}
        attempt = 0
        while True:
            attempt += 1
            data, status, headers = request(
                f"{EASYPARK_BASE}/ios/api/parkingarea/{internal_id}",
                backoff_attempt=attempt - 1,
            )
            if status == 200:
                record = {"areaNo": int(area_no) if area_no.isdigit() else area_no,
                          "areaDetail": data,
                          "tileData": tile}
                if is_schema_valid(record, area_no):
                    return "ok", record, status, headers
                if (isinstance(data, dict) and data.get("areaNo") is not None
                        and str(data.get("areaNo")) != str(area_no)):
                    return "id_areano_mismatch", None, status, headers
                if attempt <= max_retries:
                    continue
                return "schema_invalid", None, status, headers
            if status == 404:
                return "confirmed_gone", None, status, headers
            if status in (401, 403):
                return "auth", None, status, headers
            if status == 429:
                return "rate_limited", None, status, headers
            if status == 0 or status >= 500:
                if attempt <= max_retries:
                    continue
                return "transient_exhausted", None, status, headers
            return "terminal_status", None, status, headers

    def fetch_tariff(record: dict) -> None:
        detail = record["areaDetail"]
        internal_id = detail.get("id")
        if detail.get("externallyRated", True) or not internal_id:
            return
        tariff, status, _headers = request(
            f"{EASYPARK_BASE}/ios/api/parkingarea/{internal_id}/tariff"
        )
        if status == 200 and isinstance(tariff, dict) and tariff.get("units"):
            record["tariff"] = tariff

    queue = build_queue(seed_keys, priority_areas)
    log(f"EasyPark refresh {state['refresh_id']}: {seed_count} unique areaNos, "
        f"delay {delay}s, retries {max_retries}, tariff calls "
        f"{'on' if with_tariff else 'off'}")

    stop_code = None
    attempted = 0

    def failure_entry(key: str, status, reason: str | None = None) -> dict:
        entry = {"attempts": failures.get(key, {}).get("attempts", 0) + 1,
                 "last_status": status}
        if reason:
            entry["reason"] = reason
        return entry

    def bump_progress() -> None:
        nonlocal since_checkpoint
        since_checkpoint += 1
        if since_checkpoint >= CHECKPOINT_EVERY:
            persist()
            since_checkpoint = 0
            log(f"  checkpoint: {len(completed)}/{seed_count} complete, "
                f"{len(failures)} failed")

    def record_live(key: str, record: dict, source: str | None = None) -> None:
        if with_tariff:
            fetch_tariff(record)
        results[key] = record
        entry = {
            "areaNo": record["areaNo"],
            "fetched_at": now_fn(),
            "schema_valid": True,
        }
        if source:
            entry["source"] = source
        completed[key] = entry
        failures.pop(key, None)
        tombstones.pop(key, None)
        bump_progress()

    for key in queue:
        if key in completed or key in tombstones:
            continue
        if limit is not None and attempted >= limit:
            log(f"Reached --limit {limit}; stopping this pass.")
            break
        attempted += 1
        outcome, record, status, headers = fetch_area(key, areas[key])
        if outcome == "ok":
            record_live(key, record)
            continue
        if outcome == "auth":
            failures[key] = failure_entry(key, status)
            log(f"AUTH FAILURE (HTTP {status}) on areaNo {key}. Recapture token if needed.")
            stop_code = EXIT_AUTH
            break
        if outcome == "rate_limited":
            failures[key] = failure_entry(key, status)
            safe = _safe_rate_headers(headers)
            log(f"RATE LIMITED (HTTP 429) on areaNo {key}; stopping immediately.")
            if safe:
                log(f"  rate-limit metadata: {safe}")
            stop_code = EXIT_RATE_LIMITED
            break
        if status == 404:
            fb_outcome, fb_record, fb_status, fb_headers = fetch_by_internal_id(key, areas[key])
            if fb_outcome == "ok":
                record_live(key, fb_record, source="internal_id")
                continue
            if fb_outcome == "confirmed_gone":
                tombstones[key] = {
                    "areaNo": int(key) if key.isdigit() else key,
                    "internal_id": areas[key].get("id"),
                    "query_status": 404,
                    "id_status": 404,
                    "confirmed_at": now_fn(),
                }
                results.pop(key, None)
                completed.pop(key, None)
                failures.pop(key, None)
                bump_progress()
                continue
            if fb_outcome == "auth":
                failures[key] = failure_entry(key, fb_status, "fallback_auth")
                log(f"AUTH FAILURE (HTTP {fb_status}) on areaNo {key}. "
                    "Recapture token if needed.")
                stop_code = EXIT_AUTH
                break
            if fb_outcome == "rate_limited":
                failures[key] = failure_entry(key, fb_status, "fallback_rate_limited")
                safe = _safe_rate_headers(fb_headers)
                log(f"RATE LIMITED (HTTP 429) on areaNo {key}; stopping immediately.")
                if safe:
                    log(f"  rate-limit metadata: {safe}")
                stop_code = EXIT_RATE_LIMITED
                break
            failures[key] = failure_entry(
                key, status if fb_status is None else fb_status, fb_outcome)
            continue
        failures[key] = failure_entry(key, status)

    live_count = len(results)
    tombstone_count = len(tombstones)
    invariant = _state_invariant_error(results, completed, tombstones, failures)
    if invariant:
        log(f"ERROR: {invariant}")
        log("Refusing to write or promote; existing artifacts left untouched.")
        return EXIT_STATE
    persist()
    completeness = live_count / seed_count if seed_count else 0.0
    coverage = (live_count + tombstone_count) / seed_count if seed_count else 0.0
    elapsed = time.monotonic() - started
    log(f"seed={seed_count} done={live_count} failed={len(failures)} "
        f"tombstoned={tombstone_count} coverage={coverage:.4f} "
        f"completeness={completeness:.4f} statuses={dict(sorted(histogram.items()))} "
        f"refresh_id={state['refresh_id']} elapsed={elapsed:.1f}s")

    if stop_code is not None:
        log(f"Checkpoint retained at {(data_dir / EASYPARK_STATE_FILE).name}; "
            "canonical output left untouched. Re-run to resume.")
        return stop_code
    if coverage >= COMPLETENESS_THRESHOLD:
        report = _build_coverage_report(
            refresh_id=state["refresh_id"],
            generated_at=now_fn(),
            seed_count=seed_count,
            results=results,
            tombstones=tombstones,
            failures=failures,
            areas=areas,
        )
        _promote_outputs(data_dir, results, report)
        log(f"Promoted {EASYPARK_COMPLETE_FILE}, {EASYPARK_SUMMARY_FILE} "
            f"and {EASYPARK_COVERAGE_FILE}.")
        cleanup_err = _cleanup_resumable_state(data_dir, log)
        if cleanup_err is not None:
            return cleanup_err
        return EXIT_OK
    log(f"Coverage {coverage:.2%} is below the "
        f"{COMPLETENESS_THRESHOLD:.0%} gate; canonical output left untouched. "
        "Re-run to resume.")
    return EXIT_INCOMPLETE


def batch_easypark(args=None) -> int:
    """Fetch full area details for all EasyPark Gothenburg areas using areaNo lookup."""
    if args is None:
        args = easypark_defaults()
    token_file = Path(os.environ.get("EP_TOKEN_FILE", "/tmp/easypark_token.txt"))
    if not token_file.exists():
        _log(f"ERROR: No token at {token_file}")
        return EXIT_CONFIG
    try:
        token = token_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        _log(f"ERROR: token file unreadable ({type(exc).__name__})")
        return EXIT_CONFIG
    return run_easypark(
        data_dir=DATA_DIR,
        token=token,
        seed_path=DATA_DIR / EASYPARK_SEED_FILE,
        delay=args.delay,
        max_retries=args.max_retries,
        priority_areas=args.priority_area,
        with_tariff=args.with_tariff,
        limit=args.limit,
        reset_state=args.reset_state,
    )


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
    print("Fetching zone details...")
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


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


def _add_easypark_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--delay", type=float, default=_env_float("EP_FETCH_DELAY", DEFAULT_DELAY),
                        help="seconds to wait before each request (env EP_FETCH_DELAY)")
    parser.add_argument("--max-retries", type=int,
                        default=_env_int("EP_FETCH_MAX_RETRIES", DEFAULT_MAX_RETRIES),
                        help="retries for transient failures (env EP_FETCH_MAX_RETRIES)")
    parser.add_argument("--priority-area", action="append", default=[], metavar="AREANO",
                        help="fetch this areaNo first; repeatable")
    parser.add_argument("--with-tariff", action="store_true",
                        help="also request the structured /tariff endpoint (extra quota)")
    parser.add_argument("--reset-state", action="store_true",
                        help="discard any checkpoint and start a new refresh")
    parser.add_argument("--limit", type=int, default=None,
                        help="stop after attempting N areas (bounded probe)")


def _build_parser() -> tuple[argparse.ArgumentParser, argparse.ArgumentParser]:
    parser = argparse.ArgumentParser(
        prog="fetch_tariffs_v2",
        description="Fetch EasyPark and Parkster parking data for Gothenburg.",
    )
    sub = parser.add_subparsers(dest="command")
    easypark = sub.add_parser("easypark", help="resumable EasyPark area detail refresh")
    _add_easypark_args(easypark)
    sub.add_parser("parkster", help="Parkster grid scan")
    return parser, easypark


def easypark_defaults() -> argparse.Namespace:
    _parser, easypark = _build_parser()
    return easypark.parse_args([])


def main(argv: list[str] | None = None) -> int:
    parser, _easypark = _build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    os.makedirs(DATA_DIR, exist_ok=True)

    if args.command == "parkster":
        scan_parkster()
        return EXIT_OK
    if args.command == "easypark":
        return batch_easypark(args)

    print("=" * 60)
    print("PHASE 1: EasyPark area details")
    print("=" * 60)
    code = batch_easypark(easypark_defaults())
    if code != EXIT_OK:
        print(f"EasyPark phase exited {code}; skipping Parkster to avoid extra API load.")
        return code
    print()
    print("=" * 60)
    print("PHASE 2: Parkster zones")
    print("=" * 60)
    scan_parkster()
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
