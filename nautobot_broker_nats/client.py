#  SPDX-FileCopyrightText: Copyright (c) "2025" NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#  SPDX-License-Identifier: APACHE 2.0


import asyncio
import atexit
import os
import threading
import typing
import uuid

import nats
import orjson

from .log import log


class NATS:
    """Wraps the official NATS library for Python, which requires asyncio."""

    def __init__(
        self,
        attempt: int = 10,
        servers: typing.Iterable[str] = ["nats://127.0.0.1:4222"],
        stream: typing.Optional[str] = None,
        subject: str = "nautobot",
        **kwargs,
    ) -> None:
        self.attempt = attempt
        self.servers = servers
        self.stream = stream
        self.subject = subject

        # All other arguments are treated as connection parameters.
        self.connect = kwargs

        # Initialize the NATS connection and JetStream context.
        self.nc = None
        self.js = None

        # nats-py schedules keepalives and reconnect handling on its asyncio
        # event loop. Keep that loop running between synchronous publish calls
        # so an idle connection can process server pings and disconnects.
        self._loop = None
        self._loop_thread = None
        self._publish_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._pid = os.getpid()

        # Application servers such as uWSGI may import this module before
        # forking workers. Threads and event loops do not survive a fork, so
        # reset process-local state in the child and reconnect lazily.
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._reset_after_fork)

        # Ensure a graceful disconnect on process exit.
        atexit.register(self.disconnect)

    def disconnect(self) -> None:  # noqa: D102
        if self._pid != os.getpid():
            self._reset_after_fork()

        with self._publish_lock:
            loop = self._loop
            loop_thread = self._loop_thread
            if not loop or not loop_thread or not loop_thread.is_alive():
                self.nc = None
                self.js = None
                self._loop = None
                self._loop_thread = None
                return

            try:
                asyncio.run_coroutine_threadsafe(self._disconnect(), loop).result()
            finally:
                loop.call_soon_threadsafe(loop.stop)
                if threading.current_thread() is not loop_thread:
                    loop_thread.join()

                with self._lifecycle_lock:
                    self.nc = None
                    self.js = None
                    self._loop = None
                    self._loop_thread = None

    def _reset_after_fork(self) -> None:
        """Reset state inherited from a parent process."""
        self.nc = None
        self.js = None
        self._loop = None
        self._loop_thread = None
        self._publish_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._pid = os.getpid()

    def _run_event_loop(self, ready: threading.Event) -> None:
        """Run the nats-py event loop until disconnect stops it."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        ready.set()

        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    def _ensure_event_loop(self) -> asyncio.AbstractEventLoop:
        """Start the process-local event loop thread when first needed."""
        if self._pid != os.getpid():
            self._reset_after_fork()

        with self._lifecycle_lock:
            if self._loop and self._loop_thread and self._loop_thread.is_alive():
                return self._loop

            # A connection belongs to the loop that created it. If that loop
            # exited unexpectedly, discard its connection state rather than
            # attempting to reuse it from the replacement loop.
            self.nc = None
            self.js = None
            self._loop = None

            ready = threading.Event()
            self._loop_thread = threading.Thread(
                target=self._run_event_loop,
                args=(ready,),
                name="nautobot-broker-nats",
                daemon=True,
            )
            self._loop_thread.start()
            ready.wait()

            if not self._loop:
                raise RuntimeError("Failed to start the NATS event loop")
            return self._loop

    def _run(self, coroutine: typing.Coroutine[typing.Any, typing.Any, typing.Any]) -> typing.Any:
        """Run a coroutine on the persistent NATS event loop."""
        loop = self._ensure_event_loop()
        return asyncio.run_coroutine_threadsafe(coroutine, loop).result()

    def publish(self, data: dict) -> None:  # noqa: D102
        msg = orjson.dumps(data, default=lambda obj: str(obj))

        with self._publish_lock:
            self._run(self._publish(msg))

    async def _connect(self) -> None:
        # Connect to NATS.
        self.nc = await nats.connect(servers=self.servers, **self.connect)

        # Retrieve the JetStream context, and ensure the stream exists. This
        # will raise an exception if it does not.
        if self.stream:
            self.js = self.nc.jetstream()

            await self.js.stream_info(self.stream)

    async def _disconnect(self) -> None:
        # If necessary, disconnect from NATS. The close is best-effort; if the
        # server already closed the TLS connection (e.g. due to missed pings),
        # the SSL teardown will fail harmlessly.
        if self.nc:
            # drain() flushes any buffered outgoing messages before closing,
            # which is safer than close(). Fall back to close() if drain fails
            # (e.g. the connection is already broken).
            try:
                await self.nc.drain()
            except Exception:
                try:
                    await self.nc.close()
                except Exception as e:
                    log.warning("disconnect: %s" % e)

        self.nc = None
        self.js = None

    # Publish the message, retrying if necessary with an increasing delay
    # between attempts.
    async def _publish(self, msg: bytes) -> None:
        # Generate a stable message ID for this publish call so that JetStream
        # can deduplicate retries. Without a stable ID, a retry after an ACK
        # timeout could land the same message in the stream twice.
        msg_id = str(uuid.uuid4())

        for n in range(self.attempt):
            try:
                # Connect (or reconnect) if the connection is absent or closed.
                if not self.nc or self.nc.is_closed:
                    await self._connect()

                if self.stream:
                    # JetStream publish. The server sends an ACK confirming the
                    # message is durably stored before this returns; a missing
                    # ACK means the message was NOT stored and we must retry.
                    await self.js.publish(self.subject, msg, headers={"Nats-Msg-Id": msg_id})
                else:
                    # Core publish. There is no server-side ACK; flush ensures
                    # the bytes have left the TCP send buffer.
                    await self.nc.publish(self.subject, msg)
                    await self.nc.flush()

            except Exception as e:
                log.warning("publish [%d]: %s" % (n, e))

                # Force a reconnect before the next attempt.
                await self._disconnect()

                # Last attempt? Propagate the exception to the caller.
                if n + 1 == self.attempt:
                    raise e

                # Non-blocking sleep: yield control to the event loop so that
                # keepalives and other callbacks can run during the backoff.
                await asyncio.sleep(n)

            else:
                # Success.
                return
