# Issues to Resolve Before Upstreaming

Issues that need to be addressed before this library can be merged into Django's original ORM.

- async_atomic tasks restriction — https://github.com/Arfey/django-async-backend/pull/18
- Lock ensure_connection() to prevent gather-child connection race — https://github.com/Arfey/django-async-backend/pull/27
- Shield pool.getconn() from caller cancellation — https://github.com/Arfey/django-async-backend/pull/29
- acreate does not yet resolve GenericForeignKey async - https://github.com/Arfey/django-async-backend/pull/40#discussion_r3419836145
- Signal.asend() runs sync receivers through sync_to_async, so pre_delete/post_delete receivers execute on a separate sync connection outside the async transaction
- Collector.collect() only()s away the parent columns of multi-table inheritance children, so `getattr(obj, ptr.name)` refetches each parent one query at a time — we widen the only() mask instead, which is one query rather than N
