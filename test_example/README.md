poetry run python manage.py runserver 0.0.0.0:8000 - sync version

PYTHONPATH=. DJANGO_SETTINGS_MODULE='test_example.settings' poetry run uvicorn test_example.asgi:application --reload --host 0.0.0.0 --port 8000  - async version
