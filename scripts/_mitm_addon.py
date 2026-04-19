"""
mitmproxy addon that captures and logs HTTP(S) traffic from parking apps.

This is loaded by mitmdump via: mitmdump -s _mitm_addon.py
Env var CAPTURE_FILE controls where the JSON log is written.
"""

import json
import os
from pathlib import Path

from mitmproxy import http


CAPTURE_FILE = Path(
    os.environ.get(
        "CAPTURE_FILE",
        str(Path(__file__).parent.parent / "data" / "captures" / "capture.json"),
    )
)

# Hosts we always skip (analytics, ads, etc.)
SKIP_HOST_FRAGMENTS = frozenset({
    "google-analytics",
    "googletagmanager",
    "doubleclick",
    "facebook.com",
    "fbcdn",
    "crashlytics",
    "app-measurement",
    "mixpanel",
    "amplitude",
    "sentry.io",
    "appsflyer",
    "adjust.com",
    "branch.io",
    "braze.com",
    "mparticle",
    "newrelic",
    "nr-data",
})

captured: list[dict] = []


def _should_skip(host: str) -> bool:
    host_lower = host.lower()
    return any(frag in host_lower for frag in SKIP_HOST_FRAGMENTS)


def _is_interesting(host: str) -> bool:
    host_lower = host.lower()
    interesting_keywords = [
        "easypark", "park", "zone", "area", "pricing",
        "parking", "parkster", "aimo", "qpark", "epark",
        "mobill", "fastpark",
    ]
    return any(kw in host_lower for kw in interesting_keywords)


def response(flow: http.HTTPFlow) -> None:
    """Called when a complete HTTP response has been received."""
    host = flow.request.host
    if _should_skip(host):
        return

    # Build request record
    req_headers = dict(flow.request.headers)
    resp_body = b""
    resp_body_preview = ""
    req_body_preview = ""

    # Capture request body for POST/PUT/PATCH
    if flow.request.content and flow.request.method in ("POST", "PUT", "PATCH"):
        req_ct = flow.request.headers.get("content-type", "")
        if "json" in req_ct or "text" in req_ct or "form" in req_ct:
            try:
                req_body_preview = flow.request.content.decode("utf-8", errors="replace")[:2000]
            except Exception:
                req_body_preview = f"<binary, {len(flow.request.content)} bytes>"

    if flow.response and flow.response.content:
        resp_body = flow.response.content
        content_type = flow.response.headers.get("content-type", "")
        if "json" in content_type or "text" in content_type:
            try:
                resp_body_preview = resp_body.decode("utf-8", errors="replace")[:50000]
            except Exception:
                resp_body_preview = f"<binary, {len(resp_body)} bytes>"
        else:
            resp_body_preview = f"<{content_type}, {len(resp_body)} bytes>"

    record = {
        "timestamp": flow.request.timestamp_start,
        "method": flow.request.method,
        "host": host,
        "path": flow.request.path,
        "query": flow.request.query.fields if flow.request.query else [],
        "status": flow.response.status_code if flow.response else None,
        "request_headers": req_headers,
        "request_body_preview": req_body_preview,
        "response_headers": dict(flow.response.headers) if flow.response else {},
        "response_body_preview": resp_body_preview,
    }

    captured.append(record)

    # Log interesting requests to console with highlight
    is_interesting = _is_interesting(host)
    status = flow.response.status_code if flow.response else "?"

    if is_interesting:
        print(f"\n{'★' * 3} PARKING API: {flow.request.method} {flow.request.url}")
        print(f"    Status: {status}")
        if resp_body_preview and len(resp_body_preview) < 500:
            print(f"    Body: {resp_body_preview[:300]}")
        print()
    else:
        print(f"  {flow.request.method:6s} {status} {flow.request.url[:100]}")

    # Save incrementally
    _save()


def _save() -> None:
    """Save captured data to JSON file."""
    CAPTURE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CAPTURE_FILE.write_text(json.dumps(captured, indent=2, ensure_ascii=False, default=str))


def done() -> None:
    """Called when mitmproxy is shutting down."""
    _save()
    print(f"\n\nSaved {len(captured)} requests to {CAPTURE_FILE}")

    # Quick summary
    hosts: dict[str, int] = {}
    for r in captured:
        h = r["host"]
        hosts[h] = hosts.get(h, 0) + 1

    print("\nHost summary:")
    for h, count in sorted(hosts.items(), key=lambda x: -x[1]):
        marker = " ★" if _is_interesting(h) else ""
        print(f"  {count:4d}  {h}{marker}")
