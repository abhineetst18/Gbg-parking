const CACHE_NAME = 'parking-gbg-v38';
const TILE_CACHE = 'parking-gbg-tiles-v2';
const TILE_METADATA_CACHE = 'parking-gbg-tile-metadata-v1';
const MAX_TILES = 500;
const TILE_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;
const ASSETS = [
  './',
  './index.html',
  './config.js',
  './parking_data.json',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js',
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => ![CACHE_NAME, TILE_CACHE, TILE_METADATA_CACHE].includes(k))
          .map(k => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

async function trimTileCache() {
  const [tileCache, metadataCache] = await Promise.all([
    caches.open(TILE_CACHE),
    caches.open(TILE_METADATA_CACHE),
  ]);
  const keys = await tileCache.keys();
  const excessKeys = keys.slice(0, Math.max(0, keys.length - MAX_TILES));
  await Promise.all(excessKeys.flatMap(request => [
    tileCache.delete(request),
    metadataCache.delete(request),
  ]));
}

async function getFreshTile(request) {
  const [tileCache, metadataCache] = await Promise.all([
    caches.open(TILE_CACHE),
    caches.open(TILE_METADATA_CACHE),
  ]);
  const cached = await tileCache.match(request);
  if (!cached) {
    await metadataCache.delete(request);
    return null;
  }

  const metadata = await metadataCache.match(request);
  const cachedAt = metadata ? Number(await metadata.text()) : NaN;
  if (!Number.isFinite(cachedAt) || Date.now() - cachedAt >= TILE_MAX_AGE_MS) {
    await Promise.all([
      tileCache.delete(request),
      metadataCache.delete(request),
    ]);
    return null;
  }
  return cached;
}

async function cacheTile(request, response) {
  const [tileCache, metadataCache] = await Promise.all([
    caches.open(TILE_CACHE),
    caches.open(TILE_METADATA_CACHE),
  ]);
  await Promise.all([
    tileCache.put(request, response),
    metadataCache.put(request, new Response(String(Date.now()))),
  ]);
  await trimTileCache();
}

self.addEventListener('fetch', event => {
  const url = event.request.url;

  // Network-first for parking data
  if (url.includes('parking_data.json')) {
    event.respondWith(
      fetch(event.request)
        .then(resp => {
          if (resp.ok) {
            const clone = resp.clone();
            caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
          }
          return resp;
        })
        .catch(() => caches.match(event.request))
    );
    return;
  }

  // Cache map tiles on the end user's device for at most 30 days.
  if (url.includes('basemaps.cartocdn.com') || url.includes('arcgisonline.com')) {
    event.respondWith(
      getFreshTile(event.request).catch(() => null).then(async cached => {
        if (cached) return cached;

        const response = await fetch(event.request);
        if (response.ok || response.type === 'opaque') {
          try {
            await cacheTile(event.request, response.clone());
          } catch {
            // Cache storage is optional; return the downloaded tile on quota failure.
          }
        }
        return response;
      })
    );
    return;
  }

  // Let external API calls (Nominatim search, etc.) bypass SW entirely
  if (!url.startsWith(self.location.origin) && !url.includes('unpkg.com')) {
    return;
  }

  // Same-origin assets + CDN libs: cache-first
  event.respondWith(
    caches.match(event.request).then(cached => cached || fetch(event.request))
  );
});
