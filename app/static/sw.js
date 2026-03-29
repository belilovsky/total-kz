// Service Worker for Total.kz — caching, offline, push notifications
var CACHE_NAME = 'total-v2';
var OFFLINE_URL = '/offline';
var PRECACHE = [
  '/offline',
  '/static/css/public.min.css?v=16.2',
  '/static/img/favicon.svg',
  '/static/img/placeholder.svg'
];

// Install: precache essential resources
self.addEventListener('install', function(event) {
  event.waitUntil(
    caches.open(CACHE_NAME).then(function(cache) {
      return cache.addAll(PRECACHE);
    })
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', function(event) {
  event.waitUntil(
    caches.keys().then(function(keys) {
      return Promise.all(
        keys.filter(function(k) { return k !== CACHE_NAME; })
            .map(function(k) { return caches.delete(k); })
      );
    })
  );
  self.clients.claim();
});

// Fetch: cache strategy per resource type
self.addEventListener('fetch', function(event) {
  var url = new URL(event.request.url);

  // Skip non-GET requests
  if (event.request.method !== 'GET') return;

  // Skip API calls, admin, external
  if (url.pathname.startsWith('/api/') ||
      url.pathname.startsWith('/admin') ||
      url.origin !== self.location.origin) return;

  // Static assets: cache-first
  if (url.pathname.startsWith('/static/') ||
      url.pathname.startsWith('/media/') ||
      url.pathname.startsWith('/img/') ||
      url.pathname.startsWith('/imgproxy/')) {
    event.respondWith(
      caches.match(event.request).then(function(cached) {
        if (cached) return cached;
        return fetch(event.request).then(function(response) {
          if (response.ok) {
            var clone = response.clone();
            caches.open(CACHE_NAME).then(function(cache) {
              cache.put(event.request, clone);
            });
          }
          return response;
        });
      }).catch(function() {
        return caches.match('/static/img/placeholder.svg');
      })
    );
    return;
  }

  // HTML pages: network-first with offline fallback
  var accept = event.request.headers.get('accept') || '';
  if (accept.indexOf('text/html') !== -1) {
    event.respondWith(
      fetch(event.request).then(function(response) {
        if (response.ok) {
          var clone = response.clone();
          caches.open(CACHE_NAME).then(function(cache) {
            cache.put(event.request, clone);
          });
        }
        return response;
      }).catch(function() {
        return caches.match(event.request).then(function(cached) {
          return cached || caches.match(OFFLINE_URL);
        });
      })
    );
    return;
  }

  // Everything else: network with cache fallback
  event.respondWith(
    fetch(event.request).catch(function() {
      return caches.match(event.request);
    })
  );
});

// Push notifications
self.addEventListener('push', function(event) {
  var data = event.data ? event.data.json() : {};
  var title = data.title || 'Total.kz';
  var options = {
    body: data.body || 'Новая статья на Total.kz',
    icon: '/static/img/favicon.svg',
    badge: '/static/img/favicon.svg',
    image: data.image || '',
    data: { url: data.url || '/' },
    vibrate: [100, 50, 100],
    actions: [
      { action: 'open', title: 'Читать' },
      { action: 'close', title: 'Закрыть' }
    ]
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  if (event.action === 'close') return;
  var url = event.notification.data.url || '/';
  event.waitUntil(clients.openWindow(url));
});
