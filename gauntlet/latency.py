"""A TCP proxy that holds Postgres responses back by a fixed delay.

Injecting latency widens the window in which two tasks can race for the
same connection, which is where the concurrency bugs in this layer live.
`tc`/netem would be the obvious tool, but it needs NET_ADMIN and, on
loopback, would delay the driver's own HTTP as much as the database
traffic. This delays only what the application sends to and receives from
Postgres, from an ordinary unprivileged process.

The delay is applied to each chunk travelling from the server back to the
client, so a query costs roughly one delay more than it otherwise would,
and the task awaiting it holds its connection for that much longer.
"""

import asyncio
import threading


class LatencyProxy:
    """Forward 127.0.0.1:<port> to an upstream, delaying responses."""

    def __init__(self, host, port, delay_seconds):
        self.host = host
        self.port = int(port)
        self.delay = delay_seconds
        self.local_port = None
        self._loop = None
        self._server = None
        self._thread = None
        self._ready = threading.Event()

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("latency proxy did not start")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=10)
        return False

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._listen())
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            self._loop.run_until_complete(self._close())
            self._loop.close()

    async def _listen(self):
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.local_port = self._server.sockets[0].getsockname()[1]

    async def _close(self):
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, client_reader, client_writer):
        try:
            server_reader, server_writer = await asyncio.open_connection(
                self.host, self.port
            )
        except OSError:
            client_writer.close()
            return
        try:
            await asyncio.gather(
                self._pump(client_reader, server_writer, 0),
                self._pump(server_reader, client_writer, self.delay),
                return_exceptions=True,
            )
        finally:
            for writer in (client_writer, server_writer):
                writer.close()

    async def _pump(self, reader, writer, delay):
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                if delay:
                    await asyncio.sleep(delay)
                writer.write(chunk)
                await writer.drain()
        except (OSError, asyncio.IncompleteReadError):
            pass
