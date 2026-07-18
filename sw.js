// ===== Rwa7el Service Worker =====
// كل ما تغير VERSION_TAG → الـ SW بيكتشف التحديث ويطبقه تلقائياً بدون تدخل السواق
const VERSION_TAG = 'rwa7el-v1';

// ===== INSTALL: cache الملفات الأساسية =====
self.addEventListener('install', event => {
  // skipWaiting: متستناش — طبّق التحديث فوراً حتى لو في tab تانية مفتوحة
  self.skipWaiting();
  event.waitUntil(
    caches.open(VERSION_TAG).then(cache =>
      cache.addAll(['/', '/join', '/join.html', '/index.html'])
        .catch(() => { /* تجاهل لو الملفات مش موجودة */ })
    )
  );
});

// ===== ACTIVATE: امسح الـ cache القديم =====
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => k !== VERSION_TAG)
          .map(k => caches.delete(k))
      )
    ).then(() => {
      // خد control على كل الـ tabs الحالية فوراً بدون refresh
      return self.clients.claim();
    })
  );
});

// ===== FETCH: Network First (مهم — دايماً جيب الكود الأحدث من السيرفر) =====
self.addEventListener('fetch', event => {
  // الـ WebSocket مش بيمشي عبر SW
  if (event.request.url.includes('/ws/')) return;

  // HTML pages: Network First عشان التحديثات تتطبق فوراً
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .catch(() => caches.match(event.request))
    );
    return;
  }

  // باقي الموارد: Network First مع Cache fallback
  event.respondWith(
    fetch(event.request)
      .then(response => {
        if (response && response.status === 200) {
          const clone = response.clone();
          caches.open(VERSION_TAG).then(cache => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});

// ===== MESSAGE: أدمن يطلب force reload لكل الـ clients =====
self.addEventListener('message', event => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
