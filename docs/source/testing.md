# Writing async tests

## AsyncioTestCase

Use for async tests that do **not** require database transactions.

```python
from django_async_backend.test import AsyncioTestCase


class MyAsyncTests(AsyncioTestCase):
    async def asyncSetUp(self):
        # Setup code

    async def asyncTearDown(self):
        # Cleanup code

    async def test_something(self):
        # Your async test logic
        await do_async_stuff()
```

## AsyncioTransactionTestCase

Use for async tests that need database transaction support (rollbacks, atomic
blocks).

```python
from django_async_backend.test import AsyncioTransactionTestCase


class MyTransactionTests(AsyncioTransactionTestCase):
    async def asyncSetUp(self):
        # Setup database

    async def asyncTearDown(self):
        # Cleanup database

    async def test_something(self):
        async with async_atomic():
            # DB operations here
            await do_db_stuff()
```

Both test cases close every configured async connection during teardown.

## Running the test suite

See [Contributing](contribute.md) for running this project's own tests.
