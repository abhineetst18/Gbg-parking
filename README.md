# Gbg-parking
Parking finder app

## CARTO basemap key

The light and dark maps require a free CARTO basemap API key. For local use,
set `cartoApiKey` in `config.js`. Never commit a real key.

Production deploys use the `CARTO_API_KEY` GitHub Actions repository secret.
Set **Settings → Pages → Build and deployment → Source** to **GitHub Actions**;
pushes to `main` then build and deploy the site automatically.
