// Service Worker for Total.kz Push Notifications
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
