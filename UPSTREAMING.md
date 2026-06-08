# Issues to Resolve Before Upstreaming

Issues that need to be addressed before this library can be merged into Django's original ORM.

- async_atomic tasks restriction — https://github.com/Arfey/django-async-backend/pull/18
- Lock ensure_connection() to prevent gather-child connection race — https://github.com/Arfey/django-async-backend/pull/27
- Shield pool.getconn() from caller cancellation — https://github.com/Arfey/django-async-backend/pull/29
