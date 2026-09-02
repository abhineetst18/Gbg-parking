const CACHE_NAME = 'parking-gbg-v37';
const TILE_CACHE = 'parking-gbg-tiles-v1';
const MAX_TILES = 500;
const ASSETS = [
  './',
  './index.html',
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
      Promise.all(keys.filter(k => k !== CACHE_NAME && k !== TILE_CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

function trimCache(cacheName, maxItems) {
  caches.open(cacheName).then(cache =>
    cache.keys().then(keys => {
      if (keys.length > maxItems) {
        cache.delete(keys[0]).then(() => trimCache(cacheName, maxItems));
      }
    })
  );
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

  // Cache map tiles (CartoDB + Esri satellite) — separate bounded cache
  if (url.includes('basemaps.cartocdn.com') || url.includes('arcgisonline.com')) {
    event.respondWith(
      caches.open(TILE_CACHE).then(cache =>
        cache.match(event.request).then(cached =>
          cached || fetch(event.request).then(resp => {
            if (resp.ok) {
              cache.put(event.request, resp.clone());
              trimCache(TILE_CACHE, MAX_TILES);
            }
            return resp;
          })
        )
      )
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
