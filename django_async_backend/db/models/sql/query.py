from django.db.models.sql.query import Query

from django_async_backend.db import async_connections


class AsyncQuery(Query):
    compiler = "AsyncSQLCompiler"

    def get_compiler(self, using=None, connection=None, elide_empty=True):
        if using is None and connection is None:
            raise ValueError("Need either using or connection")
        if using:
            connection = async_connections[using]

        return connection.ops.compiler(self.compiler)(
            self, connection, using, elide_empty
        )
