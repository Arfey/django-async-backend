"""PostgreSQL testcontainer fixtures."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from os import environ

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

DEFAULT_POSTGRES_IMAGE = "postgres:15"


@dataclass
class PostgresContainerInfo:
    host: str
    port: int
    user: str
    password: str
    dbname: str
    container: DockerContainer


def _start_postgres_container(image: str = DEFAULT_POSTGRES_IMAGE) -> PostgresContainerInfo:
    """Start a PostgreSQL container."""
    user = "postgres"
    password = "postgres"
    dbname = "postgres"

    container = DockerContainer(image)
    container.with_exposed_ports(5432)
    container.with_env("POSTGRES_USER", user)
    container.with_env("POSTGRES_PASSWORD", password)
    container.with_env("POSTGRES_DB", dbname)
    container.start()
    # Postgres logs "ready to accept connections" twice: once during init
    # (stdout) and once after the restart (stderr). The init completion
    # message only appears once, right before the final startup.
    wait_for_logs(container, "PostgreSQL init process complete", timeout=30)
    wait_for_logs(container, "database system is ready to accept connections", timeout=30)

    return PostgresContainerInfo(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(5432)),
        user=user,
        password=password,
        dbname=dbname,
        container=container,
    )


@pytest.fixture(scope="session")
def postgres_container():
    """Session-scoped PostgreSQL container."""
    info = _start_postgres_container()

    environ["POSTGRES_HOST"] = info.host
    environ["POSTGRES_PORT"] = str(info.port)

    yield info

    with suppress(Exception):
        info.container.stop()
