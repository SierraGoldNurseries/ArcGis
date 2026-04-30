// SGN Structures Map - safe service worker
// Fixes: "Failed to execute 'addAll' on 'Cache': Request failed"
// Reason: cache.addAll() fails the whole install if any file is missing/404.
// This version caches files one-by-one and skips missing files.

const CACHE_NAME = 'sgn-map-v11-safe-cache';

const CORE_ASSETS = [
  './',
  './index.html',
  './import.html',
  './manifest.json',
  './data/structures_polygons.geojson',
  './data/structures_points.geojson',
  './data/layer_inventory.csv',
  './data/project_manifest.json'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(async (cache) => {
        for (const asset of CORE_ASSETS) {
          try {
            const response = await fetch(asset, { cache: 'no-store' });
            if (response && response.ok) {
              await cache.put(asset, response);
            }
          } catch (err) {
            // Skip missing/offline assets instead of failing install.
            console.warn('[SGN SW] skipped cache asset:', asset, err);
          }
        }
      })
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) =>
        Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const request = event.request;

  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // Do not cache GitHub/Vercel API calls or backend function calls.
  if (
    url.pathname.startsWith('/api/') ||
    url.pathname.startsWith('/.netlify/functions/')
  ) {
    event.respondWith(fetch(request));
    return;
  }

  // For GeoJSON/CSV/data files, prefer network so map updates after builds.
  if (
    url.pathname.includes('/data/') ||
    url.pathname.endsWith('.geojson') ||
    url.pathname.endsWith('.csv') ||
    url.pathname.endsWith('.json')
  ) {
    event.respondWith(
      fetch(request, { cache: 'no-store' })
        .then((response) => {
          if (response && response.ok) {
            const copy = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(request, copy)).catch(() => {});
          }
          return response;
        })
        .catch(() => caches.match(request))
    );
    return;
  }

  // For app shell, use cache fallback.
  event.respondWith(
    caches.match(request).then((cached) => {
      return cached || fetch(request).then((response) => {
        if (response && response.ok) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(request, copy)).catch(() => {});
        }
        return response;
      });
    })
  );
});
