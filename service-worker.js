const CACHE = 'sgn-map-app-v10-fix-1';
const ASSETS = [
  './',
  './index.html',
  './import.html',
  './manifest.json',
  './data/structures_polygons.geojson',
  './data/structures_points.geojson',
  './data/layer_inventory.csv'
];
self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE).then((cache) => cache.addAll(ASSETS)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', (event) => {
  event.waitUntil(caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  const isData = url.pathname.includes('/data/') || /\.(geojson|json|csv)$/i.test(url.pathname);
  event.respondWith(
    fetch(event.request).then((response) => {
      if (response.ok && !isData) {
        const copy = response.clone();
        caches.open(CACHE).then((cache) => cache.put(event.request, copy)).catch(() => {});
      }
      return response;
    }).catch(() => {
      if (isData) return new Response(JSON.stringify({ type: 'FeatureCollection', features: [] }), { headers: { 'Content-Type': 'application/json' } });
      return caches.match(event.request).then((cached) => cached || caches.match('./index.html'));
    })
  );
});
