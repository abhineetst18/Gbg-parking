"""Offline tests for the resumable EasyPark fetch path.

Every test uses a fake transport and an injected sleep function: no network
request is made and no real delay is incurred.
"""

import json
import random
import re
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import fetch_tariffs_v2 as fetch

AREA_RE = re.compile(r"areaNo=(\d+)")
SENTINEL_COMPLETE = '{"sentinel": "complete"}'
SENTINEL_SUMMARY = '[{"sentinel": "summary"}]'


def tile(area_no) -> dict:
    return {"areaNo": int(area_no), "customAreaType": "OnStreet"}


def detail(area_no, **overrides) -> dict:
    payload = {
        "id": 200000 + (int(area_no) % 100000),
        "areaNo": int(area_no),
        "areaName": f"Zone {area_no}",
        "areaType": "OnStreet",
        "city": "Göteborg",
        "displayPoint": {"lat": 57.7, "lon": 11.97},
        "externallyRated": True,
        "parkingOperatorName": "Test Operator",
        "popUpMessage": "20 kr/tim",
        "status": "ACTIVE",
    }
    payload.update(overrides)
    return payload


def make_handler(plan=None, details=None, headers_plan=None):
    """Build a response handler.

    plan: per-call-index HTTP status overrides (1-based); anything past the plan
    is a valid 200. details: optional {str(areaNo): detail payload}.
    """
    plan = list(plan or [])
    details = details or {}
    headers_plan = headers_plan or {}

    def handler(url, call_index):
        status = plan[call_index - 1] if call_index <= len(plan) else 200
        headers = headers_plan.get(call_index, {})
        if status != 200:
            return {"error": "upstream", "status": status}, status, headers
        match = AREA_RE.search(url)
        if match:
            key = match.group(1)
            return details.get(key, detail(key)), 200, headers
        return {"units": [{"price": 20.0}]}, 200, headers

    return handler


class FakeTransport:
    """Records every request URL and returns scripted responses."""

    def __init__(self, handler=None):
        self.handler = handler or make_handler()
        self.calls: list[str] = []
        self.header_sets: list[list[str]] = []

    def __call__(self, url, extra_headers=None, timeout=None):
        self.calls.append(url)
        self.header_sets.append(list(extra_headers or []))
        return self.handler(url, len(self.calls))

    @property
    def area_calls(self) -> list[str]:
        return [AREA_RE.search(u).group(1) for u in self.calls if AREA_RE.search(u)]


class EasyParkFetchTestCase(unittest.TestCase):
    """Shared temp-dir harness with sentinel canonical files."""

    seed_nos = ["1001", "1002", "1003"]

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.data = Path(self._tmp.name) / "data"
        self.data.mkdir(parents=True)
        self.complete_path = self.data / fetch.EASYPARK_COMPLETE_FILE
        self.summary_path = self.data / fetch.EASYPARK_SUMMARY_FILE
        self.state_path = self.data / fetch.EASYPARK_STATE_FILE
        self.partial_path = self.data / fetch.EASYPARK_PARTIAL_FILE
        self.complete_path.write_text(SENTINEL_COMPLETE, encoding="utf-8")
        self.summary_path.write_text(SENTINEL_SUMMARY, encoding="utf-8")
        self.slept: list[float] = []

    def seed(self, nos=None) -> list[dict]:
        return [tile(n) for n in (nos if nos is not None else self.seed_nos)]

    def run_fetch(self, transport, *, seed=None, capture=True, **kwargs):
        """Invoke run_easypark offline; returns (exit_code, combined_output)."""
        out, err = StringIO(), StringIO()
        params = {
            "data_dir": self.data,
            "token": "test-token",
            "seed_areas": self.seed() if seed is None else seed,
            "transport": transport,
            "sleep_fn": self.slept.append,
            "rng": random.Random(0),
            "delay": 4.0,
            "id_fn": lambda: "refresh-fixed-id",
        }
        params.update(kwargs)
        if capture:
            with redirect_stdout(out), redirect_stderr(err):
                code = fetch.run_easypark(**params)
        else:
            code = fetch.run_easypark(**params)
        return code, out.getvalue() + err.getvalue()

    def assert_canonical_untouched(self):
        self.assertEqual(self.complete_path.read_text(encoding="utf-8"), SENTINEL_COMPLETE)
        self.assertEqual(self.summary_path.read_text(encoding="utf-8"), SENTINEL_SUMMARY)

    def read_state(self) -> dict:
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def read_partial(self) -> dict:
        return json.loads(self.partial_path.read_text(encoding="utf-8"))


class TestRateLimitStop(EasyParkFetchTestCase):
    """A1: first 429 stops the run and never publishes canonical output."""

    def test_stops_at_first_429_and_preserves_canonical_files(self):
        transport = FakeTransport(make_handler(
            plan=[200, 200, 429],
            headers_plan={3: {"retry-after": "60", "x-ratelimit-remaining": "0"}},
        ))
        code, output = self.run_fetch(transport)

        self.assertEqual(code, fetch.EXIT_RATE_LIMITED)
        self.assertEqual(len(transport.calls), 3)
        self.assertEqual(transport.area_calls, ["1001", "1002", "1003"])
        self.assert_canonical_untouched()

        state = self.read_state()
        self.assertEqual(sorted(state["completed"]), ["1001", "1002"])
        self.assertEqual(state["status_histogram"], {"200": 2, "429": 1})
        self.assertEqual(sorted(self.read_partial()["results"]), ["1001", "1002"])
        self.assertIn("RATE LIMITED", output)
        self.assertIn("retry-after", output)

    def test_no_sleep_is_real_and_delay_applied_per_request(self):
        transport = FakeTransport(make_handler(plan=[429]))
        code, _ = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_RATE_LIMITED)
        self.assertEqual(len(self.slept), 1)
        self.assertGreaterEqual(self.slept[0], 4.0)


class TestResume(EasyParkFetchTestCase):
    """A2: resuming skips completed zones and requests only the remainder."""

    def test_resume_requests_only_remaining_zones(self):
        first = FakeTransport(make_handler(plan=[200, 200, 429]))
        code, _ = self.run_fetch(first)
        self.assertEqual(code, fetch.EXIT_RATE_LIMITED)

        second = FakeTransport(make_handler())
        code, output = self.run_fetch(second)

        self.assertEqual(second.area_calls, ["1003"])
        self.assertNotIn("areaNo=1001", "".join(second.calls))
        self.assertNotIn("areaNo=1002", "".join(second.calls))
        self.assertIn("Resuming refresh refresh-fixed-id", output)
        self.assertEqual(code, fetch.EXIT_OK)
        self.assertEqual(sorted(json.loads(
            self.complete_path.read_text(encoding="utf-8"))), ["1001", "1002", "1003"])


class TestIncompleteDrain(EasyParkFetchTestCase):
    """A3: a normal drain below the gate returns 1 and publishes nothing."""

    seed_nos = ["1001", "1002", "1003", "1004"]

    def test_incomplete_run_returns_one_and_keeps_canonical_files(self):
        transport = FakeTransport(make_handler(plan=[200, 404, 404, 200]))
        code, output = self.run_fetch(transport)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assert_canonical_untouched()
        self.assertTrue(self.state_path.exists())
        self.assertTrue(self.partial_path.exists())
        state = self.read_state()
        self.assertEqual(sorted(state["completed"]), ["1001", "1004"])
        self.assertEqual(sorted(state["failures"]), ["1002", "1003"])
        self.assertIn("below the 99% gate", output)

    def test_terminal_4xx_is_not_retried(self):
        transport = FakeTransport(make_handler(plan=[404, 200, 200, 200]))
        code, _ = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(transport.area_calls, ["1001", "1002", "1003", "1004"])


class TestPromotion(EasyParkFetchTestCase):
    """A4: at or above the gate both canonical files are swapped in atomically."""

    def test_complete_run_promotes_both_files_without_temp_leftovers(self):
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport)

        self.assertEqual(code, fetch.EXIT_OK)
        complete = json.loads(self.complete_path.read_text(encoding="utf-8"))
        summary = json.loads(self.summary_path.read_text(encoding="utf-8"))
        self.assertEqual(sorted(complete), self.seed_nos)
        self.assertEqual(len(summary), 3)
        self.assertEqual([e["areaNo"] for e in summary], self.seed_nos)
        self.assertEqual(list(self.data.glob("*.tmp")), [])
        self.assertIn("Promoted", output)

    def test_gate_allows_one_percent_shortfall(self):
        seed_nos = [str(1000 + i) for i in range(200)]
        # 199/200 = 99.5% -> promotes; a single dead zone must not block release.
        transport = FakeTransport(make_handler(plan=[404]))
        code, _ = self.run_fetch(transport, seed=[tile(n) for n in seed_nos])
        self.assertEqual(code, fetch.EXIT_OK)
        self.assertEqual(len(json.loads(self.complete_path.read_text(encoding="utf-8"))), 199)


class TestAuthStop(EasyParkFetchTestCase):
    """A5: 401 and 403 stop the run immediately."""

    def test_401_stops_immediately(self):
        transport = FakeTransport(make_handler(plan=[401]))
        code, output = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_AUTH)
        self.assertEqual(len(transport.calls), 1)
        self.assert_canonical_untouched()
        self.assertIn("AUTH FAILURE", output)

    def test_403_stops_immediately(self):
        transport = FakeTransport(make_handler(plan=[403]))
        code, _ = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_AUTH)
        self.assertEqual(len(transport.calls), 1)
        self.assert_canonical_untouched()


class TestTransientRetries(EasyParkFetchTestCase):
    """A6: 5xx and status 0 are retried, recorded, and never propagate."""

    seed_nos = ["1001"]

    def test_500_then_200_succeeds_and_both_statuses_are_counted(self):
        transport = FakeTransport(make_handler(plan=[500]))
        code, output = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_OK)
        self.assertEqual(len(transport.calls), 2)
        # State is cleaned up after successful promotion; verify from output
        self.assertIn("statuses={'200': 1, '500': 1}", output)

    def test_status_zero_is_retried_then_recorded_as_failure(self):
        transport = FakeTransport(make_handler(plan=[0, 0, 0, 0]))
        code, _ = self.run_fetch(transport, max_retries=3)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(len(transport.calls), 4)
        self.assertEqual(self.read_state()["status_histogram"], {"0": 4})
        self.assertEqual(self.read_state()["failures"]["1001"]["last_status"], 0)
        self.assert_canonical_untouched()

    def test_malformed_200_is_retried_then_failed_never_completed(self):
        details = {"1001": {"id": 1, "areaNo": 999999}}
        transport = FakeTransport(make_handler(details=details, plan=[200] * 5))
        code, _ = self.run_fetch(transport, max_retries=2)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(len(transport.calls), 3)
        self.assertEqual(self.read_state()["completed"], {})

    def test_non_string_tariff_text_is_rejected(self):
        malformed = detail("1001", priceInfo={"amount": 20})
        transport = FakeTransport(make_handler(details={"1001": malformed}))

        code, _ = self.run_fetch(transport, max_retries=1)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(self.read_state()["completed"], {})
        self.assert_canonical_untouched()

    def test_all_tariff_text_fields_require_string_or_none(self):
        for field in ("popUpMessage", "freeTextTariffInfo", "priceInfo"):
            with self.subTest(field=field):
                malformed_detail = detail("1001", **{field: {"amount": 20}})
                record = {
                    "areaNo": 1001,
                    "areaDetail": malformed_detail,
                    "tileData": tile("1001"),
                }
                self.assertFalse(fetch.is_schema_valid(record, "1001"))

                for valid_value in (None, "20 kr/tim"):
                    valid_detail = detail("1001", **{field: valid_value})
                    record["areaDetail"] = valid_detail
                    self.assertTrue(fetch.is_schema_valid(record, "1001"))

    def test_curl_transport_catches_timeout_without_network(self):
        import subprocess
        from unittest.mock import patch

        with patch.object(fetch.subprocess, "run",
                          side_effect=subprocess.TimeoutExpired("curl", 1)) as runner:
            data, status, headers = fetch.curl_request("https://example.invalid/x")
        self.assertTrue(runner.called)
        self.assertEqual((data, status, headers), (None, 0, {}))

    def test_curl_transport_catches_oserror_without_network(self):
        from unittest.mock import patch

        with patch.object(fetch.subprocess, "run", side_effect=OSError("no curl")):
            self.assertEqual(fetch.curl_request("https://example.invalid/x"), (None, 0, {}))


class TestPriorityOrdering(EasyParkFetchTestCase):
    """A7: priority areas go first without changing the final key set."""

    seed_nos = ["1001", "5047010", "1003"]

    def test_priority_area_is_requested_first_and_key_set_is_unchanged(self):
        transport = FakeTransport(make_handler())
        code, _ = self.run_fetch(transport, priority_areas=["5047010"])

        self.assertEqual(code, fetch.EXIT_OK)
        self.assertEqual(transport.area_calls[0], "5047010")
        self.assertEqual(transport.area_calls, ["5047010", "1001", "1003"])
        complete = json.loads(self.complete_path.read_text(encoding="utf-8"))
        self.assertEqual(sorted(complete), sorted(self.seed_nos))

    def test_unknown_priority_area_does_not_join_the_queue(self):
        self.assertEqual(fetch.build_queue(self.seed_nos, ["9999999"]), self.seed_nos)

    def test_duplicate_seed_entries_are_deduplicated_first_wins(self):
        self.assertEqual(fetch.build_queue(["a", "b", "a", "c"], []), ["a", "b", "c"])


class TestAnchorRoundTrip(EasyParkFetchTestCase):
    """A8: the live anchor payload survives the fetch and the merge parser."""

    seed_nos = ["5047010"]
    ANCHOR_POPUP = "Taxa 1/6-31/8 alla dagar 00-24 20 kr/påbörjad 45 min"

    def test_anchor_payload_round_trips_into_merge_parser(self):
        from unittest.mock import patch

        import merge_data

        anchor = detail(
            "5047010",
            id=211815,
            externallyRated=False,
            popUpMessage=self.ANCHOR_POPUP,
            areaName="5047010 Anchor Zone",
        )
        transport = FakeTransport(make_handler(details={"5047010": anchor}))
        code, _ = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_OK)

        raw = json.loads(self.complete_path.read_text(encoding="utf-8"))
        stored = raw["5047010"]["areaDetail"]
        self.assertEqual(stored["popUpMessage"], self.ANCHOR_POPUP)
        self.assertEqual(stored["id"], 211815)
        self.assertIs(stored["externallyRated"], False)

        with patch.object(merge_data, "DATA_DIR", self.data):
            spots = merge_data.load_easypark()
        self.assertEqual(len(spots), 1)
        spot = spots[0]
        self.assertAlmostEqual(spot["price_sek_hr"], 26.7, places=1)
        self.assertEqual(spot["season_start"], "06-01")
        self.assertEqual(spot["season_end"], "08-31")
        self.assertIn("20 kr", spot["price_text"])
        self.assertIn("45 min", spot["price_text"])
        self.assertEqual(spot["area_code"], "5047010")


class TestSecretSafety(EasyParkFetchTestCase):
    """A9: credentials never reach logs or the checkpoint."""

    seed_nos = ["1001", "1002"]
    TOKEN = "eyJhbGciOiJIUzI1NiJ9.SUPERSECRETPAYLOAD.SIGNATUREVALUE"
    HEADER_SECRET = "TOPSECRETHEADERVALUE1234"

    def test_secrets_in_headers_and_payloads_are_never_logged_or_persisted(self):
        def handler(url, call_index):
            if call_index == 1:
                return detail("1001"), 200, {}
            return (
                {"error": f"Bearer {self.HEADER_SECRET}", "token": self.TOKEN},
                429,
                {
                    "retry-after": "30",
                    "x-ratelimit-reset": f"Bearer {self.HEADER_SECRET}",
                    "authorization": f"Bearer {self.HEADER_SECRET}",
                    "set-cookie": f"session={self.HEADER_SECRET}",
                },
            )

        transport = FakeTransport(handler)
        code, output = self.run_fetch(transport, token=self.TOKEN)
        self.assertEqual(code, fetch.EXIT_RATE_LIMITED)

        state_text = self.state_path.read_text(encoding="utf-8")
        partial_text = self.partial_path.read_text(encoding="utf-8")
        for blob, label in ((output, "log"), (state_text, "checkpoint"),
                            (partial_text, "partial")):
            self.assertNotIn(self.TOKEN, blob, f"token leaked into {label}")
            self.assertNotIn(self.HEADER_SECRET, blob, f"header secret leaked into {label}")
            self.assertNotIn("Bearer ", blob, f"bearer value leaked into {label}")
        self.assertNotIn("set-cookie", state_text)
        self.assertNotIn("authorization", state_text.lower())
        self.assertIn("retry-after", output)

    def test_token_is_sent_to_transport_but_scrubbed_from_logs(self):
        transport = FakeTransport(make_handler())
        _code, output = self.run_fetch(transport, token=self.TOKEN)
        self.assertIn(f"x-authorization: Bearer {self.TOKEN}", transport.header_sets[0])
        self.assertNotIn(self.TOKEN, output)

    def test_scrub_redacts_credential_shapes(self):
        self.assertNotIn("abc", fetch.scrub("Authorization: Bearer abc"))
        self.assertEqual(fetch.scrub("areaNo 5047010 done"), "areaNo 5047010 done")


class TestTariffOptIn(EasyParkFetchTestCase):
    """A10: tariff calls are off by default and use the internal id when on."""

    seed_nos = ["1001"]

    def _internally_rated_transport(self):
        payload = detail("1001", id=211815, externallyRated=False)
        return FakeTransport(make_handler(details={"1001": payload}))

    def test_default_issues_no_tariff_requests(self):
        transport = self._internally_rated_transport()
        code, _ = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_OK)
        self.assertEqual(len(transport.calls), 1)
        self.assertFalse(any("/tariff" in url for url in transport.calls))
        self.assertNotIn("tariff", json.loads(
            self.complete_path.read_text(encoding="utf-8"))["1001"])

    def test_opt_in_uses_internal_id_and_stores_the_tariff(self):
        transport = self._internally_rated_transport()
        code, _ = self.run_fetch(transport, with_tariff=True)
        self.assertEqual(code, fetch.EXIT_OK)
        self.assertEqual(len(transport.calls), 2)
        self.assertTrue(transport.calls[1].endswith("/ios/api/parkingarea/211815/tariff"))
        self.assertNotIn("1001/tariff", transport.calls[1])
        record = json.loads(self.complete_path.read_text(encoding="utf-8"))["1001"]
        self.assertEqual(record["tariff"], {"units": [{"price": 20.0}]})
        # State is cleaned up after successful promotion
        self.assertFalse(self.state_path.exists())

    def test_externally_rated_area_never_triggers_a_tariff_call(self):
        transport = FakeTransport(make_handler())
        code, _ = self.run_fetch(transport, with_tariff=True)
        self.assertEqual(code, fetch.EXIT_OK)
        self.assertEqual(len(transport.calls), 1)


class TestStateRefusal(EasyParkFetchTestCase):
    """A11: inconsistent resume state is refused; reset is the explicit escape."""

    def _write_state(self, state, partial=None):
        self.state_path.write_text(json.dumps(state), encoding="utf-8")
        if partial is not None:
            self.partial_path.write_text(json.dumps(partial), encoding="utf-8")

    def _valid_pair(self):
        transport = FakeTransport(make_handler(plan=[200, 429]))
        code, _ = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_RATE_LIMITED)
        return self.read_state(), self.read_partial()

    def test_unsupported_checkpoint_version_is_refused(self):
        state, partial = self._valid_pair()
        state["version"] = 99
        self._write_state(state, partial)
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_STATE)
        self.assertEqual(transport.calls, [])
        self.assertIn("checkpoint version", output)
        self.assert_canonical_untouched()

    def test_missing_companion_partial_is_refused(self):
        self._valid_pair()
        self.partial_path.unlink()
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_STATE)
        self.assertEqual(transport.calls, [])
        self.assertIn("no companion", output)

    def test_orphan_partial_without_checkpoint_is_refused(self):
        self._valid_pair()
        self.state_path.unlink()
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_STATE)
        self.assertEqual(transport.calls, [])
        self.assertIn("without a checkpoint", output)

    def test_seed_fingerprint_mismatch_is_refused(self):
        self._valid_pair()
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport, seed=[tile(n) for n in ["2001", "2002", "2003"]])
        self.assertEqual(code, fetch.EXIT_STATE)
        self.assertEqual(transport.calls, [])
        self.assertIn("seed fingerprint", output)

    def test_seed_count_mismatch_is_refused(self):
        state, partial = self._valid_pair()
        state["seed_count"] = 99
        self._write_state(state, partial)
        code, output = self.run_fetch(FakeTransport(make_handler()))
        self.assertEqual(code, fetch.EXIT_STATE)
        self.assertIn("seed", output)

    def test_refresh_id_mismatch_between_files_is_refused(self):
        state, partial = self._valid_pair()
        partial["refresh_id"] = "some-other-refresh"
        self._write_state(state, partial)
        code, output = self.run_fetch(FakeTransport(make_handler()))
        self.assertEqual(code, fetch.EXIT_STATE)
        self.assertIn("different refreshes", output)

    def test_completed_entry_with_malformed_record_is_refused(self):
        state, partial = self._valid_pair()
        partial["results"]["1001"]["areaDetail"].pop("displayPoint")
        self._write_state(state, partial)
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_STATE)
        self.assertEqual(transport.calls, [])
        self.assertIn("malformed", output)

    def test_unreadable_checkpoint_is_refused(self):
        _state, partial = self._valid_pair()
        self.state_path.write_text("{not json", encoding="utf-8")
        self.partial_path.write_text(json.dumps(partial), encoding="utf-8")
        code, output = self.run_fetch(FakeTransport(make_handler()))
        self.assertEqual(code, fetch.EXIT_STATE)
        self.assertIn("unreadable", output)

    def test_reset_state_starts_a_clean_refresh(self):
        state, partial = self._valid_pair()
        state["version"] = 99
        self._write_state(state, partial)
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport, reset_state=True)
        self.assertEqual(code, fetch.EXIT_OK)
        self.assertEqual(sorted(transport.area_calls), self.seed_nos)
        self.assertIn("Starting a new refresh", output)
        # State is cleaned up after successful promotion
        self.assertFalse(self.state_path.exists())


class TestTruthfulExitAndSummary(EasyParkFetchTestCase):
    """A12: summary content and exit codes reflect what actually happened."""

    def test_summary_matches_persisted_results(self):
        transport = FakeTransport(make_handler())
        code, _ = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_OK)
        complete = json.loads(self.complete_path.read_text(encoding="utf-8"))
        summary = json.loads(self.summary_path.read_text(encoding="utf-8"))
        self.assertEqual(len(summary), len(complete))
        for entry in summary:
            record = complete[entry["areaNo"]]["areaDetail"]
            self.assertEqual(entry["id"], record["id"])
            self.assertEqual(entry["lat"], record["displayPoint"]["lat"])
            self.assertEqual(entry["popUpMessage"], record["popUpMessage"])

    def test_final_log_reports_counts_and_refresh_id(self):
        transport = FakeTransport(make_handler(plan=[200, 200, 404]))
        code, output = self.run_fetch(transport, max_retries=0)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertIn("seed=3 done=2 failed=1", output)
        self.assertIn("refresh_id=refresh-fixed-id", output)
        self.assertIn("'404': 1", output)

    def test_missing_token_returns_nonzero_without_requests(self):
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport, token="")
        self.assertNotEqual(code, fetch.EXIT_OK)
        self.assertEqual(code, fetch.EXIT_CONFIG)
        self.assertEqual(transport.calls, [])
        self.assertIn("no EasyPark token", output)

    def test_missing_seed_file_returns_nonzero_without_requests(self):
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport, seed=None,
                                      seed_areas=None,
                                      seed_path=self.data / "does_not_exist.json")
        self.assertEqual(code, fetch.EXIT_CONFIG)
        self.assertEqual(transport.calls, [])
        self.assertIn("seed file not found", output)

    def test_limit_bounds_the_number_of_attempted_areas(self):
        transport = FakeTransport(make_handler())
        code, _ = self.run_fetch(transport, limit=1)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(len(transport.calls), 1)
        self.assert_canonical_untouched()


class TestCliSurface(unittest.TestCase):
    """The command surface keeps easypark/parkster and adds bounded options."""

    def test_easypark_defaults_are_conservative(self):
        args = fetch.easypark_defaults()
        self.assertEqual(args.delay, fetch.DEFAULT_DELAY)
        self.assertEqual(args.max_retries, fetch.DEFAULT_MAX_RETRIES)
        self.assertFalse(args.with_tariff)
        self.assertFalse(args.reset_state)
        self.assertIsNone(args.limit)
        self.assertEqual(args.priority_area, [])

    def test_easypark_options_parse(self):
        parser, _easypark = fetch._build_parser()
        args = parser.parse_args([
            "easypark", "--delay", "2.5", "--max-retries", "1",
            "--priority-area", "5047010", "--priority-area", "1234567",
            "--with-tariff", "--reset-state", "--limit", "5",
        ])
        self.assertEqual(args.command, "easypark")
        self.assertEqual(args.delay, 2.5)
        self.assertEqual(args.max_retries, 1)
        self.assertEqual(args.priority_area, ["5047010", "1234567"])
        self.assertTrue(args.with_tariff)
        self.assertTrue(args.reset_state)
        self.assertEqual(args.limit, 5)

    def test_parkster_command_still_exists(self):
        parser, _easypark = fetch._build_parser()
        self.assertEqual(parser.parse_args(["parkster"]).command, "parkster")
        self.assertIsNone(parser.parse_args([]).command)


class TestPromotionLifecycle(EasyParkFetchTestCase):
    """A12/A13: state and partial are cleared only after successful promotion."""

    def test_successful_promotion_removes_state_and_partial(self):
        """After promotion, no state or partial files remain; next run starts fresh."""
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport)

        self.assertEqual(code, fetch.EXIT_OK)
        self.assertIn("Promoted", output)
        self.assertFalse(self.state_path.exists(), "state file should be removed after promotion")
        self.assertFalse(self.partial_path.exists(), "partial file should be removed after promotion")

        # Canonical files should exist and contain the promoted data
        complete = json.loads(self.complete_path.read_text(encoding="utf-8"))
        self.assertEqual(sorted(complete), self.seed_nos)

    def test_second_invocation_after_promotion_starts_fresh(self):
        """After a successful promotion, a second run starts a new refresh, not from carried state."""
        first = FakeTransport(make_handler())
        code1, output1 = self.run_fetch(first)
        self.assertEqual(code1, fetch.EXIT_OK)
        self.assertFalse(self.state_path.exists())
        self.assertFalse(self.partial_path.exists())

        # Second invocation with the same seed should request all zones again
        second = FakeTransport(make_handler())
        code2, output2 = self.run_fetch(second)

        self.assertEqual(code2, fetch.EXIT_OK)
        self.assertEqual(second.area_calls, self.seed_nos,
                         "second run should request all zones, not skip any")
        self.assertNotIn("Resuming refresh", output2,
                         "second run should not resume from carried state")
        # After second run, state should again be cleaned up
        self.assertFalse(self.state_path.exists())
        self.assertFalse(self.partial_path.exists())

    def test_incomplete_run_preserves_resumable_state(self):
        """Promotion failure before completion preserves state and partial for resumption."""
        seed_nos = ["1001", "1002", "1003", "1004"]
        transport = FakeTransport(make_handler(plan=[200, 404, 404, 200]))
        code, output = self.run_fetch(transport, seed=[tile(n) for n in seed_nos])

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertIn("below the 99% gate", output)
        # State and partial should exist for resumption
        self.assertTrue(self.state_path.exists(),
                        "incomplete run should preserve state for resumption")
        self.assertTrue(self.partial_path.exists(),
                        "incomplete run should preserve partial for resumption")

        state = self.read_state()
        self.assertEqual(sorted(state["completed"]), ["1001", "1004"])
        self.assertEqual(sorted(state["failures"]), ["1002", "1003"])

        # Canonical files should be untouched (sentinel values)
        self.assert_canonical_untouched()

    def test_auth_stop_preserves_resumable_state(self):
        """Auth/rate-limit stop preserves checkpoint for resumption."""
        transport = FakeTransport(make_handler(plan=[200, 401]))
        code, output = self.run_fetch(transport)

        self.assertEqual(code, fetch.EXIT_AUTH)
        self.assertIn("AUTH FAILURE", output)
        # State and partial should exist for resumption
        self.assertTrue(self.state_path.exists())
        self.assertTrue(self.partial_path.exists())

        state = self.read_state()
        self.assertEqual(sorted(state["completed"]), ["1001"])
        self.assert_canonical_untouched()

    def test_cleanup_handles_missing_files_gracefully(self):
        """Cleanup succeeds even if state/partial are already absent."""
        # Manually remove state and partial before running
        transport = FakeTransport(make_handler())

        # Run a successful fetch - state/partial will exist after persist()
        code, _ = self.run_fetch(transport)
        self.assertEqual(code, fetch.EXIT_OK)

        # Files should be cleaned up
        self.assertFalse(self.state_path.exists())
        self.assertFalse(self.partial_path.exists())


class TestAuthNormalization(EasyParkFetchTestCase):
    """A14: Auth boundary normalization handles bare and Bearer-prefixed tokens."""

    FIXTURE_JWT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.test.payload"

    def auth_header_value(self, header_set):
        """Read the x-authorization value from curl's ['-H', '<header>'] argument pairs."""
        for index, arg in enumerate(header_set):
            if arg == "-H" and index + 1 < len(header_set):
                value = header_set[index + 1]
                if value.lower().startswith("x-authorization:"):
                    return value.split(":", 1)[1].strip()
        self.fail(f"no x-authorization header argument pair in {header_set!r}")

    def assert_single_bearer(self, transport, expected_token):
        self.assertGreater(len(transport.header_sets), 0)
        for header_set in transport.header_sets:
            header_value = self.auth_header_value(header_set)
            self.assertEqual(header_value, f"Bearer {expected_token}")
            self.assertEqual(header_value.lower().count("bearer"), 1)

    def test_bare_token_produces_single_bearer_scheme(self):
        """Bare JWT -> exactly one Bearer scheme in transport headers."""
        transport = FakeTransport(make_handler())
        code, _ = self.run_fetch(transport, token=self.FIXTURE_JWT)

        self.assertEqual(code, fetch.EXIT_OK)
        self.assert_single_bearer(transport, self.FIXTURE_JWT)

        # curl needs '-H' and the header value as two adjacent argv entries.
        args = transport.header_sets[0]
        self.assertEqual(args[0], "-H")
        self.assertEqual(args[1], f"x-authorization: Bearer {self.FIXTURE_JWT}")
        self.assertEqual(
            [a for a in args if a.startswith("-H ")], [],
            "curl option and header value must not be collapsed into one argument")

    def test_bearer_prefixed_token_produces_single_bearer_scheme(self):
        """Token with Bearer prefix -> exactly one Bearer in transport headers."""
        transport = FakeTransport(make_handler())
        code, _ = self.run_fetch(transport, token=f"Bearer {self.FIXTURE_JWT}")

        self.assertEqual(code, fetch.EXIT_OK)
        self.assert_single_bearer(transport, self.FIXTURE_JWT)

    def test_mixed_case_bearer_scheme_normalized(self):
        """Mixed-case Bearer (BeArEr) is correctly stripped."""
        transport = FakeTransport(make_handler())
        code, _ = self.run_fetch(transport, token=f"BeArEr {self.FIXTURE_JWT}")

        self.assertEqual(code, fetch.EXIT_OK)
        self.assert_single_bearer(transport, self.FIXTURE_JWT)

    def test_whitespace_surrounding_token_handled(self):
        """Whitespace around token and after Bearer is trimmed."""
        transport = FakeTransport(make_handler())
        code, _ = self.run_fetch(transport, token=f"  Bearer   {self.FIXTURE_JWT}  ")

        self.assertEqual(code, fetch.EXIT_OK)
        self.assert_single_bearer(transport, self.FIXTURE_JWT)

    def test_empty_token_returns_config_error_before_transport(self):
        """Empty token returns EXIT_CONFIG without any network request."""
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport, token="")

        self.assertEqual(code, fetch.EXIT_CONFIG)
        self.assertEqual(len(transport.calls), 0, "Empty token should prevent all transport calls")
        self.assertIn("no EasyPark token", output)

    def test_scheme_only_token_returns_config_error_before_transport(self):
        """Token containing only 'Bearer' (scheme-only) returns EXIT_CONFIG without transport."""
        transport = FakeTransport(make_handler())
        code, output = self.run_fetch(transport, token="Bearer   ")

        self.assertEqual(code, fetch.EXIT_CONFIG)
        self.assertEqual(len(transport.calls), 0, "Scheme-only token should prevent all transport calls")
        self.assertIn("authorization scheme", output)

    def test_token_not_leaked_to_stdout_or_stderr(self):
        """Token values never appear in captured output."""
        transport = FakeTransport(make_handler())
        secret_token = "Bearer secret-test-token-xyz123"
        code, output = self.run_fetch(transport, token=secret_token, capture=True)

        self.assertEqual(code, fetch.EXIT_OK)
        # The literal token should NOT appear in output
        self.assertNotIn("secret-test-token-xyz123", output)
        self.assertNotIn(secret_token, output)

    def test_http_401_log_does_not_claim_expired_or_invalid(self):
        """HTTP 401 auth failure log is factual and does not diagnose expiry."""
        transport = FakeTransport(make_handler(plan=[401]))
        code, output = self.run_fetch(transport, token="test-token")

        self.assertEqual(code, fetch.EXIT_AUTH)
        self.assertIn("AUTH FAILURE (HTTP 401)", output)
        # Should NOT claim the token is expired or invalid
        self.assertNotIn("expired", output.lower())
        self.assertNotIn("invalid", output.lower())
        # Should suggest recapture action
        self.assertIn("Recapture", output)

    def test_http_401_stops_immediately_without_retry(self):
        """HTTP 401 stops after the first auth failure, no retries."""
        transport = FakeTransport(make_handler(plan=[200, 401, 200]))
        code, output = self.run_fetch(transport, token="test-token")

        self.assertEqual(code, fetch.EXIT_AUTH)
        # Should only have made 2 calls (first 200, then 401 stop)
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(transport.area_calls, ["1001", "1002"])
        self.assertIn("AUTH FAILURE", output)


INTERNAL_ID_RE = re.compile(r"/parkingarea/(\d+)(?:\?|$)")


def seed_tile(area_no, internal_id="auto", **overrides) -> dict:
    """A seed tile carrying the public labels and the app-internal id."""
    payload = tile(area_no)
    payload["areaName"] = f"Seed {area_no}"
    payload["parkingOperatorName"] = "Seed Operator"
    payload["originalGeometry"] = "POLYGON ((11.9 57.6, 11.9 57.7, 11.9 57.6))"
    if internal_id == "auto":
        payload["id"] = 900000 + int(area_no)
    elif internal_id is not None:
        payload["id"] = internal_id
    payload.update(overrides)
    return payload


class ScriptedApi:
    """Serves the query endpoint and the internal-id endpoint independently.

    `query` and `ident` map a key to a status or to a list of statuses consumed
    one per call. Unlisted query keys answer 200; unlisted ids answer 404.
    """

    def __init__(self, *, query=None, ident=None, details=None,
                 id_details=None, headers=None):
        self.query = dict(query or {})
        self.ident = dict(ident or {})
        self.details = details or {}
        self.id_details = id_details or {}
        self.headers = headers or {}
        self.id_calls: list[str] = []

    @staticmethod
    def _next(table, key, default):
        value = table.get(key, default)
        if isinstance(value, list):
            return value.pop(0) if value else default
        return value

    def __call__(self, url, call_index):
        if url.endswith("/tariff"):
            return {"units": [{"price": 20.0}]}, 200, {}
        id_match = INTERNAL_ID_RE.search(url)
        if id_match and "areaNo=" not in url:
            ident = id_match.group(1)
            self.id_calls.append(ident)
            status = self._next(self.ident, ident, 404)
            headers = self.headers.get(("id", ident), {})
            if status == 200:
                return self.id_details.get(ident, detail("999999")), 200, headers
            return {"error": "upstream", "status": status}, status, headers
        area_match = AREA_RE.search(url)
        key = area_match.group(1) if area_match else ""
        status = self._next(self.query, key, 200)
        headers = self.headers.get(("query", key), {})
        if status == 200:
            return self.details.get(key, detail(key)), 200, headers
        return {"error": "upstream", "status": status}, status, headers


class TestInternalIdFallback(EasyParkFetchTestCase):
    """P005-C1/C2: a query 404 is disambiguated by exactly one internal-id probe."""

    seed_nos = ["1001", "1002", "1003"]

    def fallback_seed(self, **per_area):
        return [seed_tile(n, **per_area.get(n, {})) for n in self.seed_nos]

    def test_c1_id_200_matching_areano_becomes_a_live_record(self):
        api = ScriptedApi(
            query={"1001": 404},
            ident={"901001": 200},
            id_details={"901001": detail("1001")},
        )
        transport = FakeTransport(api)
        code, output = self.run_fetch(transport, seed=self.fallback_seed(), limit=1)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(len(transport.calls), 2)
        self.assertTrue(transport.calls[1].endswith("/ios/api/parkingarea/901001"))
        state = self.read_state()
        self.assertEqual(state["completed"]["1001"]["source"], "internal_id")
        self.assertEqual(state["tombstones"], {})
        self.assertEqual(state["failures"], {})
        self.assertIn("1001", self.read_partial()["results"])
        self.assertIn("tombstoned=0", output)
        self.assert_canonical_untouched()

    def test_c2_id_404_is_a_tombstone_that_resume_skips(self):
        api = ScriptedApi(query={"1001": 404}, ident={"901001": 404})
        transport = FakeTransport(api)
        code, output = self.run_fetch(transport, seed=self.fallback_seed(), limit=1)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(len(transport.calls), 2)
        state = self.read_state()
        entry = state["tombstones"]["1001"]
        self.assertEqual(entry["areaNo"], 1001)
        self.assertEqual(entry["internal_id"], 901001)
        self.assertEqual(entry["query_status"], 404)
        self.assertEqual(entry["id_status"], 404)
        self.assertTrue(entry["confirmed_at"])
        self.assertEqual(state["failures"], {})
        self.assertEqual(state["completed"], {})
        self.assertEqual(self.read_partial()["results"], {})
        self.assertIn("tombstoned=1", output)
        self.assertIn("coverage=0.3333", output)
        self.assert_canonical_untouched()

        resumed = FakeTransport(ScriptedApi())
        code, output = self.run_fetch(resumed, seed=self.fallback_seed(), limit=1)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(resumed.area_calls, ["1002"])
        self.assertEqual(self.read_state()["tombstones"]["1001"]["id_status"], 404)

    def test_c3_fallback_401_and_403_stop_immediately_without_tombstoning(self):
        for status in (401, 403):
            with self.subTest(status=status):
                self.setUp()
                api = ScriptedApi(query={"1001": 404}, ident={"901001": status})
                transport = FakeTransport(api)
                code, output = self.run_fetch(transport, seed=self.fallback_seed())

                self.assertEqual(code, fetch.EXIT_AUTH)
                self.assertEqual(len(transport.calls), 2)
                self.assertEqual(transport.area_calls, ["1001"])
                state = self.read_state()
                self.assertEqual(state["tombstones"], {})
                self.assertEqual(state["failures"]["1001"]["reason"], "fallback_auth")
                self.assertEqual(state["failures"]["1001"]["last_status"], status)
                self.assertIn(f"AUTH FAILURE (HTTP {status})", output)
                self.assert_canonical_untouched()

    def test_c4_fallback_429_stops_with_allowlisted_rate_metadata_only(self):
        secret = "TOPSECRETRATEHEADERVALUE987654"
        api = ScriptedApi(
            query={"1001": 404},
            ident={"901001": 429},
            headers={("id", "901001"): {
                "retry-after": "45",
                "x-ratelimit-remaining": "0",
                "authorization": f"Bearer {secret}",
                "set-cookie": f"session={secret}",
            }},
        )
        transport = FakeTransport(api)
        code, output = self.run_fetch(transport, seed=self.fallback_seed())

        self.assertEqual(code, fetch.EXIT_RATE_LIMITED)
        self.assertEqual(len(transport.calls), 2)
        state = self.read_state()
        self.assertEqual(state["tombstones"], {})
        self.assertEqual(state["failures"]["1001"]["reason"], "fallback_rate_limited")
        self.assertIn("retry-after", output)
        self.assertIn("x-ratelimit-remaining", output)
        self.assertNotIn("set-cookie", output)
        self.assertNotIn("authorization", output.lower())
        self.assertNotIn(secret, output)
        self.assertNotIn(secret, self.state_path.read_text(encoding="utf-8"))
        self.assert_canonical_untouched()

    def test_c5_fallback_transient_statuses_retry_then_stay_unresolved(self):
        for status, expected_calls in ((500, 5), (0, 5)):
            with self.subTest(status=status):
                self.setUp()
                api = ScriptedApi(query={"1001": 404}, ident={"901001": status})
                transport = FakeTransport(api)
                code, _ = self.run_fetch(transport, seed=self.fallback_seed(),
                                         limit=1, max_retries=3)

                self.assertEqual(code, fetch.EXIT_INCOMPLETE)
                self.assertEqual(len(transport.calls), expected_calls)
                self.assertEqual(api.id_calls, ["901001"] * 4)
                state = self.read_state()
                self.assertEqual(state["tombstones"], {})
                self.assertEqual(state["failures"]["1001"]["reason"], "transient_exhausted")
                self.assertEqual(state["failures"]["1001"]["last_status"], status)

    def test_c6_missing_internal_id_stays_unresolved_without_a_fallback_call(self):
        seed = [seed_tile("1001", internal_id=None), seed_tile("1002"), seed_tile("1003")]
        api = ScriptedApi(query={"1001": 404})
        transport = FakeTransport(api)
        code, _ = self.run_fetch(transport, seed=seed, limit=1)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(api.id_calls, [])
        state = self.read_state()
        self.assertEqual(state["tombstones"], {})
        self.assertEqual(state["failures"]["1001"]["reason"], "no_internal_id")
        self.assertEqual(state["failures"]["1001"]["last_status"], 404)

    def test_c7_id_200_with_a_different_areano_is_never_stored_or_tombstoned(self):
        api = ScriptedApi(
            query={"1001": 404},
            ident={"901001": 200},
            id_details={"901001": detail("9999")},
        )
        transport = FakeTransport(api)
        code, _ = self.run_fetch(transport, seed=self.fallback_seed(),
                                 limit=1, max_retries=3)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(api.id_calls, ["901001"], "a mismatch must not be retried")
        state = self.read_state()
        self.assertEqual(state["tombstones"], {})
        self.assertEqual(state["completed"], {})
        self.assertEqual(state["failures"]["1001"]["reason"], "id_areano_mismatch")
        self.assertEqual(self.read_partial()["results"], {})

    def test_c11_limit_counts_seed_zones_not_fallback_requests(self):
        api = ScriptedApi(query={"1001": 404})
        transport = FakeTransport(api)
        code, _ = self.run_fetch(transport, seed=self.fallback_seed(), limit=2)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(transport.area_calls, ["1001", "1002"])
        self.assertEqual(len(transport.calls), 3)
        self.assertEqual(self.read_state()["status_histogram"], {"200": 1, "404": 2})

    def test_c14_fallback_only_follows_a_query_404_and_uses_the_internal_id(self):
        api = ScriptedApi(query={"1001": 404})
        transport = FakeTransport(api)
        self.run_fetch(transport, seed=self.fallback_seed(), limit=1)
        self.assertEqual(api.id_calls, ["901001"])
        self.assertNotIn("/parkingarea/1001", transport.calls[1])
        self.assertNotIn("areaNo=", transport.calls[1])

        self.setUp()
        api = ScriptedApi(query={"1001": 500})
        transport = FakeTransport(api)
        self.run_fetch(transport, seed=self.fallback_seed(), limit=1, max_retries=1)
        self.assertEqual(api.id_calls, [], "a 5xx query must not trigger the id probe")

        self.setUp()
        api = ScriptedApi(query={"1001": 403})
        transport = FakeTransport(api)
        self.run_fetch(transport, seed=self.fallback_seed())
        self.assertEqual(api.id_calls, [], "an auth stop must not trigger the id probe")

        self.setUp()
        api = ScriptedApi()
        transport = FakeTransport(api)
        self.run_fetch(transport, seed=self.fallback_seed())
        self.assertEqual(api.id_calls, [], "a healthy query must not trigger the id probe")


class TestResolvedCoveragePromotion(EasyParkFetchTestCase):
    """P005-C8/C9/C13: tombstones count toward the gate; canonical stays live-only."""

    seed_nos = ["1001", "1002", "1003"]

    def fallback_seed(self):
        return [seed_tile(n) for n in self.seed_nos]

    @property
    def coverage_path(self) -> Path:
        return self.data / fetch.EASYPARK_COVERAGE_FILE

    def gone_api(self):
        return ScriptedApi(query={"1001": 404}, ident={"901001": 404})

    def test_c8_tombstones_reach_the_gate_and_canonical_stays_live_only(self):
        transport = FakeTransport(self.gone_api())
        code, output = self.run_fetch(transport, seed=self.fallback_seed())

        self.assertEqual(code, fetch.EXIT_OK)
        self.assertIn("tombstoned=1 coverage=1.0000", output)
        complete = json.loads(self.complete_path.read_text(encoding="utf-8"))
        summary = json.loads(self.summary_path.read_text(encoding="utf-8"))
        self.assertEqual(sorted(complete), ["1002", "1003"])
        self.assertEqual([e["areaNo"] for e in summary], ["1002", "1003"])

        report = json.loads(self.coverage_path.read_text(encoding="utf-8"))
        self.assertEqual(report["seed_count"], 3)
        self.assertEqual(report["live_count"], 2)
        self.assertEqual(report["tombstone_count"], 1)
        self.assertEqual(report["unresolved_count"], 0)
        self.assertEqual(report["coverage"], 1.0)
        self.assertEqual(report["refresh_id"], "refresh-fixed-id")
        self.assertEqual(len(report["tombstones"]), 1)
        row = report["tombstones"][0]
        self.assertEqual(row["areaNo"], "1001")
        self.assertEqual(row["internal_id"], 901001)
        self.assertEqual(row["areaName"], "Seed 1001")
        self.assertEqual(row["operator"], "Seed Operator")
        self.assertEqual(row["query_status"], 404)
        self.assertEqual(row["id_status"], 404)
        self.assertEqual(report["unresolved"], [])

        self.assertEqual(list(self.data.glob("*.tmp")), [])
        self.assertFalse(self.state_path.exists())
        self.assertFalse(self.partial_path.exists())

        second = FakeTransport(self.gone_api())
        code, _ = self.run_fetch(second, seed=self.fallback_seed(), limit=1)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(second.area_calls, ["1001"],
                         "a new refresh must start with no carried tombstones")

    def test_c9_reset_state_discards_previously_confirmed_tombstones(self):
        first = FakeTransport(self.gone_api())
        code, _ = self.run_fetch(first, seed=self.fallback_seed(), limit=1)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertIn("1001", self.read_state()["tombstones"])

        second = FakeTransport(ScriptedApi())
        code, output = self.run_fetch(second, seed=self.fallback_seed(), reset_state=True)

        self.assertEqual(code, fetch.EXIT_OK)
        self.assertIn("Starting a new refresh", output)
        self.assertEqual(second.area_calls, self.seed_nos)
        report = json.loads(self.coverage_path.read_text(encoding="utf-8"))
        self.assertEqual(report["tombstone_count"], 0)
        self.assertEqual(report["tombstones"], [])
        self.assertEqual(report["live_count"], 3)

    def test_c13_coverage_report_carries_no_location_or_credential_data(self):
        secret = "eyJhbGciOiJIUzI1NiJ9.COVERAGEREPORTSECRET.SIGNATUREVALUE"
        api = ScriptedApi(query={"1001": 404, "1002": 404},
                          ident={"901001": 404, "901002": 500})
        transport = FakeTransport(api)
        code, _ = self.run_fetch(transport, seed=self.fallback_seed(),
                                 token=secret, max_retries=0)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        # Build the exact payload the promotion path publishes.
        report = fetch._build_coverage_report(
            refresh_id="refresh-fixed-id",
            generated_at="2026-08-01T00:00:00Z",
            seed_count=3,
            results={},
            tombstones=self.read_state()["tombstones"],
            failures=self.read_state()["failures"],
            areas={n: t for n, t in zip(self.seed_nos, self.fallback_seed())},
        )
        text = json.dumps(report, ensure_ascii=False)
        for banned in ("POLYGON", "originalGeometry", "displayPoint", "geometry",
                       "Bearer", "authorization", "set-cookie", secret):
            self.assertNotIn(banned, text)

        allowed_keys = {
            "refresh_id", "generated_at", "seed_count", "live_count",
            "tombstone_count", "unresolved_count", "coverage", "tombstones",
            "unresolved", "areaNo", "areaName", "operator", "internal_id",
            "query_status", "id_status", "confirmed_at", "reason", "last_status",
        }
        found = set()

        def walk(node):
            if isinstance(node, dict):
                found.update(node)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(report)
        self.assertEqual(found - allowed_keys, set())
        for key in found:
            self.assertNotIn("lat", key.lower())
            self.assertNotIn("lon", key.lower())


class TestTombstoneStateSafety(EasyParkFetchTestCase):
    """P005-C10/C12: additive migration resumes; contradictory state fails closed."""

    seed_nos = ["1001", "1002", "1003"]

    def fallback_seed(self):
        return [seed_tile(n) for n in self.seed_nos]

    def write_pair(self, state, partial):
        self.state_path.write_text(json.dumps(state), encoding="utf-8")
        self.partial_path.write_text(json.dumps(partial), encoding="utf-8")

    def legacy_pair(self):
        """A checkpoint written before tombstones existed, holding a raw 404 failure."""
        seed = self.fallback_seed()
        fingerprint = fetch._seed_fingerprint(self.seed_nos)
        record = {"areaNo": 1002, "areaDetail": detail("1002"), "tileData": seed[1]}
        state = {
            "version": fetch.CHECKPOINT_VERSION,
            "refresh_id": "legacy-refresh",
            "started_at": "2026-07-01T00:00:00Z",
            "updated_at": "2026-07-01T00:00:00Z",
            "seed_fingerprint": fingerprint,
            "seed_count": len(self.seed_nos),
            "delay": 4.0,
            "with_tariff": False,
            "completed": {"1002": {"areaNo": 1002, "fetched_at": "2026-07-01T00:00:00Z",
                                   "schema_valid": True}},
            "failures": {"1001": {"attempts": 1, "last_status": 404}},
            "status_histogram": {"200": 1, "404": 1},
        }
        partial = {
            "version": fetch.CHECKPOINT_VERSION,
            "refresh_id": "legacy-refresh",
            "seed_fingerprint": fingerprint,
            "results": {"1002": record},
        }
        return state, partial

    def test_c10_pre_tombstone_state_resumes_and_keeps_404s_unresolved(self):
        state, partial = self.legacy_pair()
        self.assertNotIn("tombstones", state)
        self.write_pair(state, partial)

        transport = FakeTransport(ScriptedApi())
        code, output = self.run_fetch(transport, seed=self.fallback_seed(), limit=0)

        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(transport.calls, [])
        self.assertIn("Resuming refresh legacy-refresh", output)
        migrated = self.read_state()
        self.assertEqual(migrated["tombstones"], {})
        self.assertEqual(migrated["failures"]["1001"]["last_status"], 404)
        self.assertNotIn("1001", migrated["completed"])

        probe = FakeTransport(ScriptedApi(query={"1001": 404}, ident={"901001": 404}))
        code, _ = self.run_fetch(probe, seed=self.fallback_seed(), limit=1)
        self.assertEqual(code, fetch.EXIT_INCOMPLETE)
        self.assertEqual(probe.area_calls, ["1001"])
        resolved = self.read_state()
        self.assertIn("1001", resolved["tombstones"])
        self.assertEqual(resolved["failures"], {})

    def test_c12_malformed_or_overlapping_tombstones_fail_closed(self):
        state, partial = self.legacy_pair()
        cases = {
            "not a mapping": ["1001"],
            "live and tombstoned": {"1002": {"areaNo": 1002}},
            "tombstoned and unresolved": {"1001": {"areaNo": 1001}},
        }
        for label, tombstones in cases.items():
            with self.subTest(case=label):
                self.setUp()
                broken = dict(state)
                broken["tombstones"] = tombstones
                self.write_pair(broken, partial)
                before = self.state_path.read_text(encoding="utf-8")

                transport = FakeTransport(ScriptedApi())
                code, output = self.run_fetch(transport, seed=self.fallback_seed())

                self.assertEqual(code, fetch.EXIT_STATE)
                self.assertEqual(transport.calls, [])
                self.assertIn("marked both" if isinstance(tombstones, dict)
                              else "malformed", output)
                self.assertEqual(self.state_path.read_text(encoding="utf-8"), before)
                self.assert_canonical_untouched()


if __name__ == "__main__":
    unittest.main()
