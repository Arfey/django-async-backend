FROM python:3.12-slim

RUN apt-get update && apt-get install -y git postgresql-client build-essential libmemcached-dev zlib1g-dev && rm -rf /var/lib/apt/lists/*

# Clone Django repository
RUN git clone https://github.com/django/django.git /django-repo

WORKDIR /django-repo/tests

# # Install Django in editable mode and requirements
RUN pip install --upgrade pip && \
    pip install -e .. && \
    pip install -r ./requirements/py3.txt

RUN pip install psycopg_pool

# Install Poetry
RUN pip install poetry

# Copy the django_async_backend folder into the image
COPY . /django_async_backend
RUN chmod -R 755 /django_async_backend

RUN cd /django_async_backend && pip install .

# Script to create databases and run tests
COPY --chmod=755 /tests_django/entrypoint.sh /entrypoint.sh

COPY /tests_django/test_postgres.py /django-repo/tests/test_postgres.py

# Ensure all directories and files in the cloned repo have correct permissions
RUN chmod -R 755 /django-repo

ENTRYPOINT ["/entrypoint.sh"]
