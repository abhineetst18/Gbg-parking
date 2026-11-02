# Gbg-parking
Parking finder app

## CARTO basemap key

The light and dark maps require a free CARTO basemap API key. For local use,
set `cartoApiKey` in `config.js`. Never commit a real key.

Production deploys use the `CARTO_API_KEY` GitHub Actions repository secret.
Set **Settings → Pages → Build and deployment → Source** to **GitHub Actions**;
pushes to `main` then build and deploy the site automatically.

### Troubleshooting map tiles

| Tile text | Cause | Fix |
|-----------|-------|-----|
| `API KEY REQUIRED` | No key reached the browser | Check the secret exists (`gh secret list`), Pages source is **GitHub Actions**, and the latest deploy run succeeded |
| `API key not found` | Key is invalid, revoked, or not allowed for this domain | Create a new key in CARTO, update the secret (`gh secret set CARTO_API_KEY`), and re-run the deploy workflow |

### Alternative tile providers

If CARTO changes its terms, pricing, or key requirements, these are the fallbacks:

| Provider | Key | Dark/light | Effort | Notes |
|----------|-----|------------|--------|-------|
| **Esri World Dark/Light Gray Canvas** | None | Yes (labels are a separate reference layer) | Small | Best keyless option; closest look to CARTO. `arcgisonline.com` is already allowed by the CSP and cached by `sw.js`. Fine for personal, non-commercial use. |
| **Stadia Maps (Alidade Smooth / Smooth Dark)** | Account + domain registration (no key in code) | Yes | Small | Very good look; free for non-commercial use. |
| **OpenStreetMap standard** | None | Light only | Small | OSM's tile usage policy discourages heavy app use; no dark style. |
| **OpenFreeMap / MapTiler** | None / free key | Yes | Large | Vector tiles; requires switching from Leaflet raster layers to MapLibre GL. |

Esri URLs (append a labels layer from
`.../Canvas/World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}` or `World_Light_Gray_Reference`):

```text
https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}
https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}
```

To switch providers, update:

- `darkLayer` / `lightLayer` tile URLs and the map attribution in `index.html`
- `img-src` in the Content-Security-Policy `<meta>` tag in `index.html`
- The tile cache host match in `sw.js`
- The key injection step in `.github/workflows/deploy-pages.yml` (remove it if the provider is keyless)
