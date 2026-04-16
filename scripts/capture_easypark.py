"""
EasyPark API Capture Script

Runs mitmproxy to intercept and log all HTTP(S) traffic from the EasyPark app.
Use with iPhone WiFi proxy or Android emulator.

Usage:
    # Start capture (iPhone WiFi proxy mode):
    python capture_easypark.py start

    # Analyze captured data:
    python capture_easypark.py analyze

    # Show your Mac's IP for iPhone proxy config:
    python capture_easypark.py info
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

CAPTURE_DIR = Path(__file__).parent.parent / "data" / "captures"
ADDON_SCRIPT = Path(__file__).parent / "_mitm_addon.py"


def get_local_ip() -> str:
    """Get the Mac's local WiFi IP address."""
    for iface in ["en0", "en1"]:
        try:
            result = subprocess.run(
                ["ipconfig", "getifaddr", iface],
                capture_output=True,
                text=True,
                check=True,
            )
            ip = result.stdout.strip()
            if ip:
                return ip
        except subprocess.CalledProcessError:
            continue
    return "unknown"


def show_info() -> None:
    """Show setup instructions for iPhone proxy configuration."""
    ip = get_local_ip()
    port = 8080

    print("\n" + "=" * 60)
    print("  EasyPark API Capture - iPhone Setup Instructions")
    print("=" * 60)
    print(f"\n  Your Mac IP: {ip}")
    print(f"  Proxy Port:  {port}")
    print()
    print("  iPhone Steps:")
    print("  1. Connect iPhone to same WiFi as this Mac")
    print(f"  2. Settings > WiFi > (i) > Configure Proxy > Manual")
    print(f"     Server: {ip}")
    print(f"     Port:   {port}")
    print(f"  3. Open Safari on iPhone, go to: http://mitm.it")
    print("  4. Download the iOS certificate")
    print("  5. Settings > General > VPN & Device Management > mitmproxy")
    print("     > Install")
    print("  6. Settings > General > About > Certificate Trust Settings")
    print("     > Enable full trust for mitmproxy")
    print()
    print("  Then open EasyPark app and browse the Gothenburg map!")
    print()
    print("  To remove proxy when done:")
    print("  Settings > WiFi > (i) > Configure Proxy > Off")
    print("=" * 60)
    print()


def start_capture() -> None:
    """Start mitmproxy with the EasyPark capture addon."""
    CAPTURE_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    capture_file = CAPTURE_DIR / f"easypark_capture_{timestamp}.json"

    # Show setup info first
    show_info()

    print(f"Capture file: {capture_file}")
    print("Starting mitmproxy... (press Ctrl+C to stop)\n")

    # Run mitmdump with our addon script
    env_vars = {"CAPTURE_FILE": str(capture_file)}
    cmd = [
        "mitmdump",
        "--listen-host", "0.0.0.0",
        "--listen-port", "8080",
        "--set", "ssl_insecure=true",
        "-s", str(ADDON_SCRIPT),
    ]

    try:
        import os
        env = os.environ.copy()
        env.update(env_vars)
        proc = subprocess.run(cmd, env=env)
    except KeyboardInterrupt:
        print(f"\n\nCapture stopped. Data saved to: {capture_file}")
        print(f"Run: python {__file__} analyze --file {capture_file}")


def analyze_captures(file_path: str | None = None) -> None:
    """Analyze captured API calls to identify EasyPark endpoints."""
    if file_path:
        files = [Path(file_path)]
    else:
        if not CAPTURE_DIR.exists():
            print("No captures found. Run 'start' first.")
            return
        files = sorted(CAPTURE_DIR.glob("easypark_capture_*.json"))
        if not files:
            print("No capture files found.")
            return

    all_requests: list[dict] = []
    for f in files:
        try:
            data = json.loads(f.read_text())
            all_requests.extend(data)
        except (json.JSONDecodeError, KeyError):
            print(f"Warning: Could not parse {f}")

    if not all_requests:
        print("No requests captured yet.")
        return

    # Group by host
    hosts: dict[str, list[dict]] = {}
    for req in all_requests:
        host = req.get("host", "unknown")
        hosts.setdefault(host, []).append(req)

    print(f"\n{'=' * 70}")
    print(f"  EasyPark API Capture Analysis")
    print(f"  Total requests: {len(all_requests)}")
    print(f"  Unique hosts: {len(hosts)}")
    print(f"{'=' * 70}\n")

    # Filter out known non-parking hosts
    skip_hosts = {
        "www.google.com", "fonts.googleapis.com", "play.google.com",
        "googleads.g.doubleclick.net", "www.googletagmanager.com",
        "firebaseinstallations.googleapis.com", "app-measurement.com",
        "settings.crashlytics.com", "firebase-settings.crashlytics.com",
        "graph.facebook.com", "connect.facebook.net",
        "cdn.mxpnl.com", "api.mixpanel.com",
        "sentry.io", "cdn.amplitude.com",
        "ssl.google-analytics.com",
    }

    print("INTERESTING HOSTS (likely EasyPark API):\n")
    interesting = []
    for host, reqs in sorted(hosts.items()):
        # Skip known tracking/analytics
        if any(skip in host for skip in skip_hosts):
            continue
        if "easypark" in host.lower() or "park" in host.lower():
            interesting.append((host, reqs))
            print(f"  ★ {host} ({len(reqs)} requests)")
        elif host not in skip_hosts:
            print(f"    {host} ({len(reqs)} requests)")

    if interesting:
        print(f"\n{'─' * 70}")
        print("EASYPARK API ENDPOINTS FOUND:\n")
        for host, reqs in interesting:
            print(f"\n  Host: {host}")
            print(f"  {'─' * 50}")
            # Group by path
            paths: dict[str, list[dict]] = {}
            for r in reqs:
                path = r.get("path", "/")
                paths.setdefault(path, []).append(r)

            for path, path_reqs in sorted(paths.items()):
                methods = set(r.get("method", "?") for r in path_reqs)
                statuses = set(str(r.get("status", "?")) for r in path_reqs)
                print(f"  {','.join(methods):6s} {path}")
                print(f"         Status: {', '.join(statuses)}")
                # Show sample request headers
                sample = path_reqs[0]
                if sample.get("request_headers"):
                    auth_headers = {
                        k: v
                        for k, v in sample["request_headers"].items()
                        if k.lower()
                        in ("authorization", "x-api-key", "x-csrf", "cookie")
                    }
                    if auth_headers:
                        print(f"         Auth: {auth_headers}")
                if sample.get("response_body_preview"):
                    preview = sample["response_body_preview"][:200]
                    print(f"         Body: {preview}")
                print()

        # Save summary
        summary_file = CAPTURE_DIR / "api_summary.json"
        summary = []
        for host, reqs in interesting:
            for r in reqs:
                summary.append({
                    "host": host,
                    "method": r.get("method"),
                    "path": r.get("path"),
                    "status": r.get("status"),
                    "request_headers": r.get("request_headers"),
                    "query": r.get("query"),
                    "response_body_preview": r.get("response_body_preview"),
                })
        summary_file.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        print(f"\nAPI summary saved to: {summary_file}")
    else:
        print("\n⚠ No EasyPark-specific hosts found in capture.")
        print("  This likely means SSL certificate pinning is active.")
        print("  Look for 'Certificate rejected' or TLS error messages above.")
        print("\n  Next step: Try Android emulator + Frida approach.")

    # Show all hosts for reference
    print(f"\n{'─' * 70}")
    print("ALL CAPTURED HOSTS:\n")
    for host, reqs in sorted(hosts.items(), key=lambda x: -len(x[1])):
        print(f"  {len(reqs):4d}  {host}")


def main() -> None:
    parser = argparse.ArgumentParser(description="EasyPark API Capture Tool")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("info", help="Show iPhone proxy setup instructions")
    sub.add_parser("start", help="Start mitmproxy capture")

    analyze_parser = sub.add_parser("analyze", help="Analyze captured data")
    analyze_parser.add_argument("--file", help="Specific capture file to analyze")

    args = parser.parse_args()

    if args.command == "info":
        show_info()
    elif args.command == "start":
        start_capture()
    elif args.command == "analyze":
        analyze_captures(args.file)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
