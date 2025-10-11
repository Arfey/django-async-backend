# Django Async Backend

## ⚙️ Development Setup

**Install pre-commit hooks:**
```bash
pip install pre-commit
pre-commit install
```

**Install dependencies:**
```bash
poetry install
```

## 🧪 Running Tests

This project uses a comprehensive test suite powered by `unittest`.

**To run tests:**
```bash
docker-compose up postgres -d
DJANGO_SETTINGS_MODULE=settings poetry run python -m unittest discover -s tests
```

**Integration tests run locally.**

The `django_async_backend.db.backends.postgresql` backend is fully compatible with Django's default `django.db.backends.postgresql` backend, as it leverages the default implementation under the hood. To confirm this compatibility, run Django's test suite using the custom backend.

```python
DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
    "other": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
}
```

To execute them:

```bash
cd tests_django
docker-compose run --build --rm test_django_integration
```
