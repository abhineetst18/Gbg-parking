# Gbg-parking — Agent Instructions

## Project Overview
Single-file PWA parking finder for Gothenburg, Sweden. Vanilla HTML/CSS/JS (~1470 lines in `index.html`) + hand-written service worker (`sw.js`).

## Architecture
- **index.html** — Entire app (HTML + CSS + JS in one file)
- **sw.js** — Service worker: cache-first for assets, network-first for data, bounded tile cache (500 max)
- **manifest.json** — PWA manifest with SVG icons
- **parking_data.json** — Static dataset of 3,607 parking spots (2.1 MB)
- **scripts/** — Data generation/scraping utilities
- **data/** — Raw source data

## Tech Stack
- Leaflet 1.9.4 + MarkerCluster (CDN with SRI)
- Photon + Nominatim geocoding APIs
- CSS custom properties for theming (dark/light)
- Swedish/English translation (client-side)

## Design Standards
Follow `webDesignLanguage.instructions.md` from the AstPythonTools repo:
- 4px/8px spacing grid
- Color tokens via CSS custom properties (never hardcoded hex in templates)
- Focus-visible + hover on all interactive elements
- Typography scale: 10, 11, 13, 15, 18, 24px only
- `prefers-reduced-motion` respected

## Security
- Content-Security-Policy via `<meta>` tag
- All data fields escaped with `escHtml()` or `JSON.stringify()` before innerHTML
- `encodeURIComponent()` on deep link URLs
- Class attributes sanitized via allowlist regex
- SRI hashes on all CDN resources
- Service worker only caches `resp.ok` responses

## Known Future Improvements

### Performance: Incremental marker updates (Low Priority)
`refreshMap()` currently destroys and recreates all ~3,607 Leaflet markers on every filter/radius/sort change. With MarkerCluster handling DOM rendering (only ~20-40 visible markers at any zoom), the actual impact is ~50-150ms JS time on filter change — acceptable with the existing debounce.

**If this becomes a problem:**
1. Cache `L.circleMarker` instances in a `Map<spotId, marker>`
2. On filter change, compute the new visible set and diff against current cluster members
3. Only `addLayer`/`removeLayer` the delta
4. Keep popup HTML generation lazy (build on first `popupopen` event, not at marker creation)

**Trigger:** Only worth implementing if users report lag on low-end Android devices or if the dataset grows past ~8,000 spots.

### PWA: Proper raster icons
The manifest uses inline SVG data URIs for all icon sizes. For better install-surface appearance (especially Android adaptive icons), generate proper 192px and 512px PNG icons + a maskable variant.

### PWA: Self-host CDN libraries
Leaflet and MarkerCluster are loaded from unpkg.com. Self-hosting would eliminate the CDN dependency for offline-first resilience and remove the need for SRI hash maintenance on version bumps.
