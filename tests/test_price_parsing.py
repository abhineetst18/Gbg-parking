"""Unit tests for Swedish tariff text parsing.

Locks interval behavior, mixed free+paid handling, and rate-bearing text selection.
"""

import json
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import merge_data
from merge_data import (
    _merge_group,
    extract_rate_bearing_line,
    find_price_text_mismatches,
    parse_free_minutes,
    parse_season,
    parse_sek_per_hour,
    parse_time_limit,
)


class TestParseSekPerHour(unittest.TestCase):
    """Test parse_sek_per_hour() interval and free+paid logic."""

    def test_45_min_interval(self):
        """20 kr per 45 min -> 26.7 kr/h"""
        result = parse_sek_per_hour("20 kr per 45 min")
        self.assertAlmostEqual(result, 26.7, places=1)

    def test_45_min_interval_pabörjade(self):
        """20 kr per påbörjade 45 min -> 26.7 kr/h"""
        result = parse_sek_per_hour("20 kr per påbörjade 45 min")
        self.assertAlmostEqual(result, 26.7, places=1)

    def test_historical_på_började_hour(self):
        """Historical: 20 Kr / per på började timme -> 20.0 kr/h"""
        result = parse_sek_per_hour("20 Kr / per på började timme")
        self.assertAlmostEqual(result, 20.0, places=1)

    def test_decimal_comma_15_min(self):
        """Decimal comma: 7,50 kr/15 min -> 30.0 kr/h"""
        result = parse_sek_per_hour("7,50 kr/15 min")
        self.assertAlmostEqual(result, 30.0, places=1)

    def test_first_rate_wins_over_later(self):
        """Daytime rate (first) should be selected over evening rate (later)."""
        text = "Måndag – Fredag 15:00 – 22:00\n20 kr per påbörjade 45 min\nÖvrig tid 5 kr / per påbörjade timme"
        result = parse_sek_per_hour(text)
        # First explicit rate is "20 kr per påbörjade 45 min" = 26.7
        self.assertAlmostEqual(result, 26.7, places=1)

    def test_mixed_free_then_paid(self):
        """Första 15 min gratis, därefter 20 kr/tim -> 20.0 (paid rate wins)."""
        result = parse_sek_per_hour("Första 15 min gratis, därefter 20 kr/tim")
        self.assertAlmostEqual(result, 20.0, places=1)

    def test_free_only(self):
        """Pure free parking: gratis -> 0.0"""
        self.assertEqual(parse_sek_per_hour("Gratis parkering"), 0.0)
        self.assertEqual(parse_sek_per_hour("Avgiftsfri"), 0.0)

    def test_literal_zero_kr(self):
        """Explicit 0 kr/h is valid free parking."""
        self.assertEqual(parse_sek_per_hour("0 kr/h"), 0.0)

    def test_30_min_interval(self):
        """15 kr per 30 min -> 30.0 kr/h"""
        result = parse_sek_per_hour("15 kr per 30 min")
        self.assertAlmostEqual(result, 30.0, places=1)

    def test_15_min_interval(self):
        """10 kr/15 min -> 40.0 kr/h"""
        result = parse_sek_per_hour("10 kr/15 min")
        self.assertAlmostEqual(result, 40.0, places=1)

    def test_space_tolerant_på_började(self):
        """Internal space variant: på började (with space) -> recognized."""
        result = parse_sek_per_hour("6 kr per på började 30 min")
        self.assertAlmostEqual(result, 12.0, places=1)


class TestExtractRateBearingLine(unittest.TestCase):
    """Test extract_rate_bearing_line() for price_text selection."""

    def test_rate_on_second_line(self):
        """Schedule on line 1, rate on line 2 -> return line 2."""
        text = "Måndag – Fredag 15:00 – 22:00\n20 kr per påbörjade 45 min\nÖvrig tid 5 kr/tim"
        parsed_price = 26.7  # from "20 kr per påbörjade 45 min"
        result = extract_rate_bearing_line(text, parsed_price)
        self.assertIn("20 kr", result)
        self.assertIn("45 min", result)

    def test_rate_on_first_line(self):
        """Rate on line 1 -> return line 1."""
        text = "20 kr/tim alla dagar 08-22\nÖvrig tid 2 kr/tim"
        parsed_price = 20.0
        result = extract_rate_bearing_line(text, parsed_price)
        self.assertEqual(result, "20 kr/tim alla dagar 08-22")

    def test_no_newlines(self):
        """Single-line text -> return whole text (bounded)."""
        text = "5 kr/tim"
        result = extract_rate_bearing_line(text, 5.0)
        self.assertEqual(result, "5 kr/tim")

    def test_fallback_to_first_meaningful_line(self):
        """No kr-bearing line found -> first non-empty line."""
        text = "Parkeringszon Nord\nZon 1234\nTillstånd krävs"
        result = extract_rate_bearing_line(text, None)
        self.assertEqual(result, "Parkeringszon Nord")


class TestIntroductoryFreePeriod(unittest.TestCase):
    """Test introductory-free metadata without inventing a maximum stay."""

    def test_first_minutes_gratis(self):
        text = "Första 15 min gratis, därefter 20 kr/tim"
        self.assertEqual(parse_free_minutes(text), 15)
        self.assertIsNone(parse_time_limit(text))

    def test_hours_gratis(self):
        text = "Alla dagar 08-22 2 tim gratis därefter 5 kr per påbörjad tim."
        self.assertEqual(parse_free_minutes(text), 120)
        self.assertIsNone(parse_time_limit(text))

    def test_avgiftsfritt_first_hours(self):
        text = "Avgiftsfritt första 2 timmar därefter 9kr/h"
        self.assertEqual(parse_free_minutes(text), 120)
        self.assertIsNone(parse_time_limit(text))

    def test_explicit_time_limit_still_wins(self):
        text = "2 tim gratis därefter 5 kr/tim. Max p-tid 4 tim"
        self.assertEqual(parse_free_minutes(text), 120)
        self.assertEqual(parse_time_limit(text), "4h")

    def test_time_limit_sentence_is_still_supported(self):
        text = "24 tim Tidsbegränsningen gäller vardag 00.00-24.00."
        self.assertEqual(parse_time_limit(text), "24h")

    def test_later_time_limit_survives_introductory_free_period(self):
        text = (
            "2 tim gratis därefter 5 kr/tim.\n"
            "24 tim Tidsbegränsningen gäller vardag 00.00-24.00."
        )
        self.assertEqual(parse_free_minutes(text), 120)
        self.assertEqual(parse_time_limit(text), "24h")


class TestMergedTariffProvenance(unittest.TestCase):
    """Test that a merged rate keeps only compatible tariff metadata."""

    @staticmethod
    def _spot(source, price, text, **metadata):
        return {
            "id": f"{source}_1",
            "name": "Shared parking area",
            "lat": 57.7,
            "lon": 11.9,
            "price_sek_hr": price,
            "price_text": text,
            "time_limit": None,
            "max_daily_sek": metadata.get("max_daily_sek"),
            "season_start": metadata.get("season_start"),
            "season_end": metadata.get("season_end"),
            "free_minutes": metadata.get("free_minutes"),
            "permit_required": False,
            "service_fee": False,
            "area_code": "1",
            "gbg_code": None,
            "type": "street",
            "source": source,
            "operator": "",
            "area_type_raw": "",
            "status": "ACTIVE",
        }

    def test_selected_rate_does_not_inherit_other_rate_metadata(self):
        easypark = self._spot(
            "easypark",
            5.0,
            "5 kr/tim, första 2 tim gratis",
            max_daily_sek=40.0,
            season_start="06-01",
            season_end="08-31",
            free_minutes=120,
        )
        parkster = self._spot("parkster", 18.0, "18 kr/tim")

        merged = _merge_group([easypark, parkster])

        self.assertEqual(merged["price_sek_hr"], 18.0)
        self.assertEqual(merged["price_text"], "18 kr/tim")
        self.assertIsNone(merged["max_daily_sek"])
        self.assertIsNone(merged["season_start"])
        self.assertIsNone(merged["season_end"])
        self.assertIsNone(merged["free_minutes"])

    def test_selected_rate_keeps_its_own_metadata(self):
        easypark = self._spot("easypark", 5.0, "5 kr/tim")
        parkster = self._spot(
            "parkster",
            18.0,
            "18 kr/tim, max 90 kr/dag",
            max_daily_sek=90.0,
            season_start="06-01",
            season_end="08-31",
            free_minutes=30,
        )

        merged = _merge_group([easypark, parkster])

        self.assertEqual(merged["max_daily_sek"], 90.0)
        self.assertEqual(merged["season_start"], "06-01")
        self.assertEqual(merged["season_end"], "08-31")
        self.assertEqual(merged["free_minutes"], 30)

    def test_same_rate_text_fallback_adopts_text_source_metadata(self):
        easypark = self._spot(
            "easypark",
            5.0,
            "",
            max_daily_sek=40.0,
            free_minutes=120,
        )
        parkster = self._spot(
            "parkster",
            5.0,
            "5 kr/tim, max 90 kr/dag",
            max_daily_sek=90.0,
            season_start="06-01",
            season_end="08-31",
            free_minutes=30,
        )

        merged = _merge_group([easypark, parkster])

        self.assertEqual(merged["price_text"], "5 kr/tim, max 90 kr/dag")
        self.assertEqual(merged["max_daily_sek"], 90.0)
        self.assertEqual(merged["season_start"], "06-01")
        self.assertEqual(merged["season_end"], "08-31")
        self.assertEqual(merged["free_minutes"], 30)

    def test_different_rate_text_is_not_borrowed(self):
        easypark = self._spot("easypark", 18.0, "")
        parkster = self._spot("parkster", 5.0, "5 kr/tim")

        merged = _merge_group([easypark, parkster])

        self.assertEqual(merged["price_sek_hr"], 18.0)
        self.assertEqual(merged["price_text"], "")


class TestGeneratedPriceConsistency(unittest.TestCase):
    """Test the final numeric-rate versus display-text invariant."""

    def test_detects_parseable_disagreement(self):
        spots = [{"id": "bad", "price_sek_hr": 0.0, "price_text": "5 kr/tim"}]
        self.assertEqual(find_price_text_mismatches(spots), [("bad", 0.0, 5.0)])

    def test_allows_matching_and_unparseable_fallback_text(self):
        spots = [
            {"id": "matching", "price_sek_hr": 5.0, "price_text": "5 kr/tim"},
            {"id": "fallback", "price_sek_hr": 12.0, "price_text": "Taxa enligt app"},
        ]
        self.assertEqual(find_price_text_mismatches(spots), [])

    def test_main_does_not_overwrite_output_when_validation_fails(self):
        mismatched = {
            "id": "bad",
            "price_sek_hr": 0.0,
            "price_text": "5 kr/tim",
            "sources": ["easypark"],
            "type": "street",
        }
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            data_dir = root / "data"
            data_dir.mkdir()
            output = root / "parking_data.json"
            data_copy = data_dir / "parking_merged.json"
            output.write_text("public-sentinel")
            data_copy.write_text("data-sentinel")

            with (
                patch.object(merge_data, "ROOT_DIR", root),
                patch.object(merge_data, "DATA_DIR", data_dir),
                patch.object(merge_data, "load_parkering_gbg", return_value=[]),
                patch.object(merge_data, "load_easypark", return_value=[]),
                patch.object(merge_data, "load_parkster", return_value=[]),
                patch.object(merge_data, "load_epark", return_value=[]),
                patch.object(merge_data, "deduplicate", return_value=[mismatched]),
            ):
                with redirect_stdout(StringIO()):
                    with self.assertRaisesRegex(ValueError, "Refusing to publish"):
                        merge_data.main()

            self.assertEqual(output.read_text(), "public-sentinel")
            self.assertEqual(data_copy.read_text(), "data-sentinel")


class TestPublishedEasyParkData(unittest.TestCase):
    """Test the tariff fields currently served by the PWA."""

    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parent.parent / "parking_data.json"
        cls.spots = json.loads(path.read_text())["spots"]
        cls.easypark_spots = [
            spot for spot in cls.spots if "easypark" in spot.get("sources", [])
        ]

    def test_numeric_rates_match_displayed_tariffs(self):
        self.assertEqual(find_price_text_mismatches(self.easypark_spots), [])

    def test_introductory_free_metadata_matches_displayed_tariffs(self):
        mismatches = []
        for spot in self.easypark_spots:
            parsed_free = parse_free_minutes(spot.get("price_text", ""))
            if parsed_free is not None and spot.get("free_minutes") != parsed_free:
                mismatches.append((spot.get("id"), spot.get("free_minutes"), parsed_free))
        self.assertEqual(mismatches, [])


class TestParseSeason(unittest.TestCase):
    """Test parse_season() for seasonal date range extraction."""

    def test_taxa_prefix_with_dash(self):
        """Live 2026 captured: Taxa 1/6-31/8 -> (06-01, 08-31)."""
        result = parse_season("Taxa 1/6-31/8 alla dagar 00-24 20 kr/påbörjad 45 min")
        self.assertEqual(result, ("06-01", "08-31"))

    def test_avgift_prefix_with_dash(self):
        """Historical: Avgift 1/5-30/9 -> (05-01, 09-30)."""
        result = parse_season("Avgift 1/5-30/9 alla dagar")
        self.assertEqual(result, ("05-01", "09-30"))

    def test_avgift_with_till(self):
        """Historical: Avgift 1/6 till 30/9 -> (06-01, 09-30)."""
        result = parse_season("Avgift 1/6 till 30/9")
        self.assertEqual(result, ("06-01", "09-30"))

    def test_taxa_with_till(self):
        """Taxa variant with till: Taxa 15/5 till 15/9 -> (05-15, 09-15)."""
        result = parse_season("Taxa 15/5 till 15/9")
        self.assertEqual(result, ("05-15", "09-15"))

    def test_no_season_marker(self):
        """No seasonal prefix -> None."""
        result = parse_season("Måndag – Fredag 08:00 – 18:00\n20 kr/tim")
        self.assertIsNone(result)

    def test_live_captured_full_integration(self):
        """Integration: live captured text parses price, season, and display line."""
        text = "Taxa 1/6-31/8 alla dagar 00-24 20 kr/påbörjad 45 min"

        # Price parsing
        price = parse_sek_per_hour(text)
        self.assertAlmostEqual(price, 26.7, places=1)

        # Season parsing
        season = parse_season(text)
        self.assertEqual(season, ("06-01", "08-31"))

        # Rate-bearing line extraction
        display = extract_rate_bearing_line(text, price)
        self.assertIn("20 kr", display)
        self.assertIn("45 min", display)


if __name__ == "__main__":
    unittest.main()
