# ZP1-GbgParking Dependency Map

## Data Pipeline

### Inputs (Raw Data)
- `data/easypark_gothenburg_areas.json` — EasyPark tile seed (source of the areaNo work queue)
- `data/easypark_gothenburg_complete.json` — EasyPark zones with popUpMessage, freeTextTariffInfo, priceInfo, coordinates (canonical; only replaced by a ≥99% *resolved-coverage* refresh; contains live records only, never tombstones)
- `data/easypark_gothenburg_coverage.json` — report-only resolution ledger promoted with the canonical files (live/tombstoned/unresolved counts + per-zone reasons); no coordinates, geometry or credentials; not read by the merge pipeline or the frontend
- `data/easypark_gothenburg_partial.json` — in-progress raw results for the current refresh (staging only, never read by merge)
- `data/.easypark_fetch_state.json` — fetch checkpoint v1 (no auth material, no response headers; carries `completed`, `failures`, `tombstones`, `status_histogram`)
- `data/easypark_prices.json` — EasyPark tariff fallback (ano → price)
- `data/easypark_lot_prices.json` — EasyPark lot-specific tariff fallback
- `data/parkster_gothenburg_zones.json` — Parkster zones with fee schedules
- `data/parkster_gothenburg_summary.json` — Parkster summary (coordinates)
- `data/gbg_parking_opendata.geojson` — P.GBG municipal zones
- **Status:** The whole `data/` directory is `.gitignore`d, including staged partial results and checkpoints; raw data must be regenerated via fetch scripts

### Fetch/Capture Scripts
- `scripts/fetch_tariffs_v2.py` — Manual, resumable EasyPark refresh (requires JWT token at `/tmp/easypark_token.txt`, override via `EP_TOKEN_FILE`)
  - **Commands:** `easypark`, `parkster`, or no command (EasyPark then Parkster; Parkster is skipped when EasyPark exits nonzero)
  - **EasyPark options:** `--delay` (default 4.0s, env `EP_FETCH_DELAY`), `--max-retries` (default 3, env `EP_FETCH_MAX_RETRIES`), repeatable `--priority-area`, `--with-tariff` (default off), `--reset-state`, `--limit`
  - **Resumability:** every schema-valid 200 is written to `easypark_gothenburg_partial.json` + `.easypark_fetch_state.json` atomically (every 25 successes and before every exit). Default runs resume; resume is refused unless checkpoint version, refresh_id linkage, seed SHA-256 fingerprint and seed count all agree and every completed record is schema-valid. The merge-consumed tariff fields (`popUpMessage`, `freeTextTariffInfo`, `priceInfo`) must be strings or null; other types are retried and recorded as malformed rather than promoted
  - **State lifecycle:** checkpoint and partial persist across quota/auth/incomplete stops; cleanup is attempted only after canonical promotion succeeds. When cleanup succeeds they are removed and exit is 0; cleanup failure exits 5 and canonical outputs may already be promoted. A subsequent normal refresh starts fresh only when cleanup completed. `--reset-state` is only for explicitly abandoning or refusing an incomplete state
  - **Rate/auth handling:** stop at the first 429 (exit 3), stop immediately on 401/403 (exit 2), retry 5xx/timeouts with exponential backoff + jitter, treat other 4xx as terminal failures
  - **Terminal-404 disambiguation:** a query-form 404 (`/ios/api/parkingarea?areaNo=…`) triggers exactly one fallback probe of the app's internal-id endpoint `/ios/api/parkingarea/<tileData['id']>`, routed through the same pacing/jitter/backoff/histogram path. Outcomes: id 200 with a schema-valid, areaNo-matching payload → live record marked `source: "internal_id"`; id 404 → **tombstone** (metadata only: areaNo, internal_id, query_status, id_status, confirmed_at); id 200 with a different areaNo → unresolved `id_areano_mismatch` (never stored, never tombstoned — a captured internal id has been observed pointing at another zone); no `tileData['id']` → unresolved `no_internal_id` with zero extra requests; malformed 200 after retries → `schema_invalid`; 0/5xx after retries → `transient_exhausted`; other terminal status → `terminal_status`. Fallback 401/403 exits 2 and 429 exits 3 immediately and never tombstones. `--limit` counts seed zones, not HTTP calls, so a bounded pass can issue up to 2 requests (plus retries) per zone; the status histogram counts every request
  - **Tombstones:** additive optional field inside checkpoint **v1** (`CHECKPOINT_VERSION` unchanged); `load_refresh_state` applies `setdefault("tombstones", {})`, so a pre-tombstone checkpoint resumes without `--reset-state`. Pre-existing raw 404 `failures` are **not** auto-migrated: they stay unresolved and re-enter the queue until a real id probe runs. The work queue skips keys in `completed` **or** `tombstones`. `--reset-state` starts with no tombstones
  - **State invariants:** before every persist and before promotion, `_state_invariant_error` requires `results` and `completed` to hold the same keys and requires results/tombstones/failures to be pairwise disjoint. A violation (including a malformed tombstone map) fails closed with exit 5, writes nothing and leaves checkpoint, partial and canonical files untouched
  - **Promotion gate:** resolved coverage `(live + tombstones) / seed_count ≥ 99%` stages, fsyncs and `os.replace`s `easypark_gothenburg_complete.json`, `easypark_gothenburg_summary.json` and `easypark_gothenburg_coverage.json` (exit 0); below that the run exits 1 and leaves all three byte-identical. `completeness` is still reported as the live-only ratio. The final log keeps `seed=N done=N failed=N` and appends `tombstoned=N coverage=0.xxxx`. Canonical complete/summary carry live records only — `_build_summary(results)` is unchanged, so downstream merge counts are unaffected and there is no merge-side count gate
  - **Exit codes:** 0 promoted, 1 incomplete, 2 auth, 3 rate limited, 4 config (missing token/seed), 5 refused resume/config state, violated state invariants, or canonical promotion succeeded but resumable-state cleanup failed
  - **Secret handling:** logs carry only areaNo, aggregate counts, statuses, refresh_id and allowlisted rate-limit headers, all scrubbed of credential-shaped values; the checkpoint stores no tokens, cookies or response headers. The coverage report is restricted to an explicit field allowlist (refresh_id, generated_at, counts, coverage, and per-zone areaNo/areaName/operator/internal_id/status/reason) and never carries `displayPoint`, `originalGeometry`, lat/lon or header material
  - **Optional tariff:** `/tariff` is requested only with `--with-tariff`, only when `externallyRated` is false, and only via the internal `areaDetail['id']` (never areaNo). `merge_data` does not currently consume `record['tariff']`
- `scripts/capture_easypark.py` — mitmproxy addon to intercept EasyPark mobile app traffic
- `scripts/fetch_tariffs.py` — (legacy) earlier EasyPark fetch script
- **Dependency:** Fresh data capture requires valid EasyPark authentication token (user-supplied)

### Merge/Processing Path
- `scripts/merge_data.py` — Core pipeline
  - **Entry:** `main()` calls `load_easypark()`, `load_parkster()`, loads GBG opendata
  - **Key functions:**
    - `parse_sek_per_hour(text)` — Extracts SEK/hour from Swedish tariff text
    - `parse_max_daily(text)` — Extracts max daily rate
    - `parse_season(text)` — Extracts seasonal date ranges
    - `parse_time_limit(text)` — Extracts parking time limits
    - `parse_free_minutes(text)` — Extracts initial free period
    - `classify_type(area_type)` — Normalizes to street/garage/lot/ev/other
  - **EasyPark processing (lines 209-322):**
    - Combines `popUpMessage`, `freeTextTariffInfo`, `priceInfo` in priority order
    - Runs `parse_sek_per_hour()` on combined text
    - Falls back to `tariff_prices` dictionary when parsing yields None
    - Selects the rate-bearing line whose parsed value agrees with `price_sek_hr`
    - Keeps display text, daily cap, season, and introductory-free metadata on one tariff provenance record; a same-rate source may donate missing text and then becomes that record, while text from a different rate is never borrowed
    - Refuses to publish when a parseable `price_text` disagrees with `price_sek_hr`
    - Outputs unified spot records with lat/lon, price_sek_hr, price_text, metadata
  - **Manual corrections:** `PGBG_CORRECTIONS` list overrides specific zones (e.g., Gärdesvägen 4890)
  - **Output:** `parking_data.json` (root directory)

- `scripts/audit_prices.py` — Diagnostic script
  - Extracts all unique pricing sentences from data sources
  - Runs `parse_sek_per_hour()` and classifies results (OK / FAILED / SUSPICIOUS_LOW / MISMATCH_HIGH / OK_FREE)
  - Cross-checks P.GBG parser-vs-stored values
  - **Dependency:** Imports `parse_sek_per_hour` from merge_data
  - **EasyPark surface:** audits the same combined `popUpMessage` + `freeTextTariffInfo` + `priceInfo` text the merge path parses (P001 fix)
  - **Generated surface:** audits every public record containing EasyPark and reports stored-vs-displayed rate and introductory-free disagreements

### Production Output
- `parking_data.json` — Unified parking spot list
  - **Schema per record:**
    - `id`, `name`, `lat`, `lon`
    - `price_sek_hr` (float or null), `price_text` (display string)
    - `time_limit`, `max_daily_sek`, `season_start`, `season_end`, `free_minutes`
    - `permit_required`, `service_fee`, `area_code`, `gbg_code`
    - `type`, `source`, `operator`, `area_type_raw`, `status`
  - **Consumers:** Frontend (`index.html`, `sw.js`)
  - **Mutation trigger:** Manual run of `python3 scripts/merge_data.py`
  - **Regeneration status:** Production `parking_data.json` remains the 2026-06-16 source snapshot. On 2026-08-31, four assertion-identified EasyPark rows were repaired in place without changing the 3,607-row set or source timestamp: three paid tariffs were no longer marked free, one stale multi-source rate was aligned with both displayed text and current Parkster data, and two introductory-free durations stopped masquerading as maximum stays. A full regeneration still waits for a ≥99% resolved-coverage EasyPark refresh plus a merge run

## Frontend Consumers

### Web App
- `index.html` — PWA frontend
  - **Reads:** `parking_data.json` (fetched via service worker)
  - **Displays:** Map markers with name, price_sek_hr, price_text, area_code
  - **Search/filter:** By price, type, proximity
  - **Responsive panel:** below 768px, controls and results remain a collapsible bottom panel; at 768px and above, they become a fixed 360px right sidebar while the map fills the remaining width

- `sw.js` — Service worker
  - **Cache strategy:** Network-first for `parking_data.json` (cache name: `parking-gbg-v37`)
  - **Cache invalidation trigger:** Bump `CACHE_NAME` version when data changes
  - **Dependencies:** Production deployment requires coordinated sw.js version bump + data refresh

### Static Assets
- `manifest.json` — PWA manifest (app name, icons, theme)
- `.env.example` — Template for local environment (no secrets in repo)

## Known Limitations & Dependencies

### Sensitive/Ignored Artifacts
- `data/` directory — Contains personal EasyPark capture data; fully `.gitignore`d
- `/tmp/easypark_token.txt` — JWT token (user-supplied; expires periodically)
- No API keys or secrets committed to repo

### Current Data Staleness
- Production `parking_data.json` is based on the 2026-06-16 snapshot with the bounded 2026-08-31 four-row correction above; `sw.js` is at `parking-gbg-v37`
- **No full merge/regeneration is allowed yet:** the current canonical EasyPark file contains only 301 live records versus 1,893 EasyPark source records in the historical published snapshot. Wait until a sufficiently complete refresh passes the ≥99% resolved-coverage promotion gate
- The in-flight EasyPark refresh is a checkpoint-v1 state with live results plus raw query-form 404 failures that have not yet been probed against the internal-id endpoint; those stay unresolved until a probe runs
- EasyPark tariffs change periodically; refresh is manual and quota-bound, never automated
- Fresh data requires: (1) valid token, (2) run `scripts/fetch_tariffs_v2.py easypark` (resume as needed until exit 0), (3) run merge, (4) commit + deploy + bump sw.js
- A refresh may legitimately span several quota windows: rerun the same command, which resumes from the checkpoint instead of refetching

### Parsing Constraints
- Parser assumes Swedish text conventions (kr/tim, påbörjad, gratis, maxtid)
- Mixed free+paid text: paid candidates are collected before the `gratis` short-circuit, so "Första 15 min gratis, därefter 20 kr/tim" yields 20.0 (P001 fix)
- Introductory-free periods: recognizes prefix and suffix forms using `fri`, `gratis`, and `avgiftsfritt`, including "Första 15 min gratis", "2 tim gratis", and "Avgiftsfritt första 2 timmar"
- Duration disambiguation: a duration attached to introductory-free wording is not emitted as `time_limit`; scanning continues so a later maximum-stay duration is still retained
- Interval rates (X kr/N min): Generic pattern supports 15/30/45/60-minute intervals
- Historical "på började" variant: Space-tolerant pattern added 2026-06-16
- Rate-bearing text retention: `extract_rate_bearing_line()` returns the line that reproduces the parsed price, so a schedule-only first line no longer becomes `price_text` (P001 fix)
- Seasonal ranges: `parse_season()` recognizes both `Avgift` and `Taxa` prefixes with `-`/`–`/`till` separators (e.g. `Taxa 1/6-31/8` → `06-01`..`08-31`)

### Manual Corrections
- `PGBG_CORRECTIONS` hardcodes Gärdesvägen zone fix (P.GBG upstream data error)
- No automated correction mechanism; verified via Google Street View

## Verification & Testing
- **Automated tests:** standard-library `unittest`, no pytest dependency. Run from the project root with `python3 -m unittest discover -s tests -p 'test_*.py'`
  - `tests/test_price_parsing.py` — locks interval parsing, mixed free+paid handling, rate-bearing line selection, free-period/time-limit disambiguation, tariff-source provenance, public-output consistency, and seasonal ranges in `merge_data`
  - `tests/test_fetch_easypark.py` — offline coverage of the resumable EasyPark fetch (fake transport, injected sleep; makes no network request): stop-on-429, resume, incomplete drain, ≥99% promotion, 401/403 stop, retry/backoff, priority ordering, live-anchor round trip through the merge parser, secret scrubbing, tariff opt-in, resume-state refusal, truthful exit codes, and the internal-id 404 fallback (live promotion via `source: "internal_id"`, tombstoning, areaNo-mismatch rejection, fallback auth/rate-limit/transient handling, additive v1 migration, fail-closed state invariants, resolved-coverage promotion and coverage-report field safety)
- **Manual audit:** `python3 scripts/audit_prices.py` (requires raw data)
- **Production validation:** Manual spot-checks against EasyPark mobile app
