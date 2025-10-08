#!/bin/bash
set -e

# Wait for PostgreSQL to be available
until pg_isready -h postgres -U postgres; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

# Create test databases if they don't exist
PGPASSWORD=postgres psql -h postgres -U postgres -d postgres -c "CREATE DATABASE django1;" || true
PGPASSWORD=postgres psql -h postgres -U postgres -d postgres -c "CREATE DATABASE django2;" || true

# Run Django tests with the specified settings
python runtests.py --settings=test_postgres
