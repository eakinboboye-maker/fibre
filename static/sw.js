const CACHE_NAME = "banana-fibre-ops-v2";
const ASSETS = [
  "/",
  "/static/index.html",
  "/static/styles.css",
  "/static/app.js",
  "/static/manifest.webmanifest",
  "/static/sw.js"
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS)));
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.map(k => (k !== CACHE_NAME ? caches.delete(k) : null)))
    )
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // Only handle same-origin
  if (url.origin !== location.origin) return;

  // API: network first; for GET you can fallback to cache
  if (url.pathname.startsWith("/api/")) {
    if (req.method !== "GET") {
      // Non-GET API requests should go to network (offline queue is handled by app.js)
      event.respondWith(
        fetch(req).catch(() => new Response(JSON.stringify({ detail: "Offline" }), {
          status: 503, headers: { "Content-Type": "application/json" }
        }))
      );
      return;
    }

    // GET /api: try network, cache success, fallback to cache if offline
    event.respondWith((async () => {
      try {
        const net = await fetch(req);
        const cache = await caches.open(CACHE_NAME);
        cache.put(req, net.clone());
        return net;
      } catch {
        const cached = await caches.match(req);
        if (cached) return cached;
        return new Response(JSON.stringify({ detail: "Offline" }), {
          status: 503, headers: { "Content-Type": "application/json" }
        });
      }
    })());
    return;
  }

  // App shell/static: cache first
  event.respondWith(
    caches.match(req).then(cached => cached || fetch(req))
  );
});

