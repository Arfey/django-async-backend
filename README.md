# Django Async Backend

# ORM support:

### Manager:

| methods                             | supported | comments |
| ----------------------------------- | --------- | -------- |
| `Model.objects.aget`                | ❌        |          |
| `Model.objects.acreate`             | ❌        |          |
| `Model.objects.acount`              | ❌        |          |
| `Model.objects.anone`               | ❌        |          |
| `Model.objects.abulk_create`        | ❌        |          |
| `Model.objects.abulk_update`        | ❌        |          |
| `Model.objects.aget_or_create`      | ❌        |          |
| `Model.objects.aupdate_or_create`   | ❌        |          |
| `Model.objects.aearliest`           | ❌        |          |
| `Model.objects.alatest`             | ❌        |          |
| `Model.objects.afirst`              | ❌        |          |
| `Model.objects.alast`               | ❌        |          |
| `Model.objects.ain_bulk`            | ❌        |          |
| `Model.objects.adelete`             | ❌        |          |
| `Model.objects.aupdate`             | ❌        |          |
| `Model.objects.aexists`             | ❌        |          |
| `Model.objects.aexplain`            | ❌        |          |
| `Model.objects.araw`                | ❌        |          |
| `Model.objects.aall`                | ❌        |          |
| `Model.objects.afilter`             | ❌        |          |
| `Model.objects.aexclude`            | ❌        |          |
| `Model.objects.acomplex_filter`     | ❌        |          |
| `Model.objects.aunion`              | ❌        |          |
| `Model.objects.aintersection`       | ❌        |          |
| `Model.objects.adifference`         | ❌        |          |
| `Model.objects.aselect_for_update`  | ❌        |          |
| `Model.objects.aprefetch_related`   | ❌        |          |
| `Model.objects.aannotate`           | ❌        |          |
| `Model.objects.aorder_by`           | ❌        |          |
| `Model.objects.adistinct`           | ❌        |          |
| `Model.objects.adifference`         | ❌        |          |
| `Model.objects.aextra`              | ❌        |          |
| `Model.objects.areverse`            | ❌        |          |
| `Model.objects.adefer`              | ❌        |          |
| `Model.objects.aonly`               | ❌        |          |
| `Model.objects.ausing`              | ❌        |          |
| `Model.objects.aresolve_expression` | ❌        |          |
| `Model.objects.aordered`            | ❌        |          |
| `__aiter__`                         | ❌        |          |
| `__repr__`                          | ❌        |          |
| `__len__`                           | ❌        |          |
| `__getitem__`                       | ❌        |          |
| `Model.objects.aiterator`           | ❌        |          |

### RawQuerySet

Not supported ❌

### Model:

| methods         | supported | comments |
| --------------- | --------- | -------- |
| `Model.asave`   | ❌        |          |
| `Model.aupdate` | ❌        |          |
| `Model.adelete` | ❌        |          |
| `...`           | ❌        |          |

### User Model / Manager

| methods                     | supported | comments |
| --------------------------- | --------- | -------- |
| `User.is_authenticated`     | ❌        |          |
| `User.is_super_user`        | ❌        |          |
| `User.objects.acreate_user` | ❌        |          |
| `...`                       | ❌        |          |


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
