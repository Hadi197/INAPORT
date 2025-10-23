// Enhanced Service Worker for Market Share Dashboard
const CACHE_NAME = 'market-dashboard-v1.1.0';
const STATIC_CACHE = 'market-static-v1.1.0';
const DATA_CACHE = 'market-data-v1.1.0';

// Assets to cache immediately
const STATIC_ASSETS = [
    '/',
    '/market.html',
    '/tabel.html',
    'https://cdn.tailwindcss.com',
    'https://cdn.jsdelivr.net/npm/chart.js',
    'https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels'
];

// Install event - cache static assets
self.addEventListener('install', event => {
    console.log('Service Worker installing...');
    event.waitUntil(
        caches.open(STATIC_CACHE)
            .then(cache => {
                console.log('Caching static assets...');
                return cache.addAll(STATIC_ASSETS);
            })
            .then(() => self.skipWaiting())
    );
});

// Activate event - clean up old caches
self.addEventListener('activate', event => {
    console.log('Service Worker activating...');
    event.waitUntil(
        caches.keys().then(cacheNames => {
            return Promise.all(
                cacheNames.map(cacheName => {
                    if (cacheName !== CACHE_NAME && cacheName !== STATIC_CACHE && cacheName !== DATA_CACHE) {
                        console.log('Deleting old cache:', cacheName);
                        return caches.delete(cacheName);
                    }
                })
            );
        }).then(() => self.clients.claim())
    );
});

// Fetch event - serve from cache when possible
self.addEventListener('fetch', event => {
    const url = new URL(event.request.url);

    // Handle CSV data with special caching strategy
    if (url.pathname.endsWith('.csv') || url.pathname.includes('ina.csv')) {
        event.respondWith(
            caches.open(DATA_CACHE).then(cache => {
                return cache.match(event.request).then(response => {
                    // Return cached version if available
                    if (response) {
                        // Update cache in background for future requests
                        fetch(event.request)
                            .then(freshResponse => {
                                if (freshResponse.ok) {
                                    cache.put(event.request, freshResponse.clone());
                                }
                            })
                            .catch(() => {
                                // Ignore fetch errors for background updates
                            });
                        return response;
                    }

                    // Fetch and cache
                    return fetch(event.request).then(response => {
                        if (response.ok) {
                            cache.put(event.request, response.clone());
                        }
                        return response;
                    });
                });
            })
        );
        return;
    }

    // Handle static assets
    if (STATIC_ASSETS.includes(url.href) || STATIC_ASSETS.includes(url.pathname)) {
        event.respondWith(
            caches.match(event.request).then(response => {
                return response || fetch(event.request).then(response => {
                    // Cache successful responses
                    if (response.ok && response.type === 'basic') {
                        const responseClone = response.clone();
                        caches.open(STATIC_CACHE).then(cache => {
                            cache.put(event.request, responseClone);
                        });
                    }
                    return response;
                });
            })
        );
        return;
    }

    // Default fetch behavior with network-first for HTML
    if (event.request.destination === 'document') {
        event.respondWith(
            fetch(event.request).then(response => {
                // Cache successful HTML responses
                if (response.ok) {
                    const responseClone = response.clone();
                    caches.open(CACHE_NAME).then(cache => {
                        cache.put(event.request, responseClone);
                    });
                }
                return response;
            }).catch(() => {
                // Return cached version if network fails
                return caches.match(event.request);
            })
        );
        return;
    }

    // Default fetch behavior
    event.respondWith(
        caches.match(event.request).then(response => {
            return response || fetch(event.request).then(response => {
                // Cache successful responses
                if (response.ok && response.type === 'basic') {
                    const responseClone = response.clone();
                    caches.open(CACHE_NAME).then(cache => {
                        cache.put(event.request, responseClone);
                    });
                }
                return response;
            });
        })
    );
});
self.addEventListener('activate', event => {
    event.waitUntil(
        caches.keys().then(cacheNames => {
            return Promise.all(
                cacheNames.map(cacheName => {
                    if (cacheName !== CACHE_NAME) {
                        console.log('Deleting old cache:', cacheName);
                        return caches.delete(cacheName);
                    }
                })
            );
        })
    );
});