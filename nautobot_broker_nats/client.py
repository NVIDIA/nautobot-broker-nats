#  SPDX-FileCopyrightText: Copyright (c) "2025" NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#  SPDX-License-Identifier: APACHE 2.0


import asyncio
import atexit
import concurrent.futures
import os
import threading
import typing
import uuid
import weakref

import nats
import orjson

from .log import log

EVENT_LOOP_START_TIMEOUT = 5
# Keep worker shutdown bounded even though nats-py's default drain timeout is
# longer. A worker recycle must not wait indefinitely to flush a connection.
DISCONNECT_TIMEOUT = 5
EVENT_LOOP_STOP_TIMEOUT = 5
PUBLISH_TIMEOUT = 60


_INSTANCES: weakref.WeakSet["NATS"] = weakref.WeakSet()


def _new_event_loop() -> asyncio.AbstractEventLoop:
    """Create an event loop through a module-local test seam."""
    return asyncio.new_event_loop()


def _reset_instances_after_fork() -> None:
    """Reset every live client inherited by a forked child process."""
    for instance in list(_INSTANCES):
        instance._reset_after_fork()


def _disconnect_instances_at_exit() -> None:
    """Disconnect every client that is still live at process exit."""
    for instance in list(_INSTANCES):
        instance.disconnect()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_instances_after_fork)
atexit.register(_disconnect_instances_at_exit)


class NATS:
    """Wraps the official NATS library for Python, which requires asyncio."""

    def __init__(
        self,
        attempt: int = 10,
        servers: typing.Iterable[str] = ["nats://127.0.0.1:4222"],
        stream: typing.Optional[str] = None,
        subject: str = "nautobot",
        publish_timeout: float = PUBLISH_TIMEOUT,
        **kwargs,
    ) -> None:
        self.attempt = attempt
        self.publish_timeout = publish_timeout
        self.servers = servers
        self.stream = stream
        self.subject = subject

        if self.publish_timeout <= 0:
            raise ValueError("publish_timeout must be greater than zero")

        # All other arguments are treated as connection parameters.
        self.connect = kwargs

        # Initialize the NATS connection and JetStream context.
        self.js = None
        self.nc = None

        # nats-py schedules keepalives and reconnect handling on its asyncio
        # event loop. Keep that loop running between synchronous publish calls
        # so an idle connection can process server pings and disconnects.
        self._lifecycle_lock = threading.Lock()
        self._loop = None
        self._loop_ready = None
        self._loop_start_error = None
        self._loop_thread = None
        self._pid = os.getpid()
        self._publish_lock = threading.Lock()

        # Module-level process hooks retain only a weak reference to this
        # client, so short-lived instances can still be garbage-collected.
        _INSTANCES.add(self)

    def disconnect(self) -> None:  # noqa: D102
        if self._pid != os.getpid():
            self._reset_after_fork()

        # A synchronous wait on a coroutine submitted to the current event
        # loop would deadlock. Schedule the shutdown and let the loop stop
        # itself after the disconnect completes instead.
        if threading.current_thread() is self._loop_thread:
            self._disconnect_from_event_loop()
            return

        with self._publish_lock:
            with self._lifecycle_lock:
                loop = self._loop
                loop_thread = self._loop_thread
                if not loop or not loop_thread or not loop_thread.is_alive():
                    self._loop = None
                    self._loop_ready = None
                    self._loop_start_error = None
                    self._loop_thread = None
                    self.js = None
                    self.nc = None
                    return

            future = None
            coroutine = self._disconnect()
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
                future.result(timeout=DISCONNECT_TIMEOUT)
            except concurrent.futures.TimeoutError:
                # run_coroutine_threadsafe chains cancellation from this
                # future to the asyncio task. Event-loop teardown below also
                # cancels any task that has not observed cancellation yet.
                future.cancel()
                log.warning("disconnect timed out after %s seconds", DISCONNECT_TIMEOUT)
            except Exception as exc:  # Best-effort cleanup, especially during atexit.
                if future is None:
                    coroutine.close()
                log.warning("disconnect: %s", exc)
            finally:
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except RuntimeError:
                    pass

                loop_thread.join(timeout=EVENT_LOOP_STOP_TIMEOUT)
                if loop_thread.is_alive():
                    log.warning(
                        "event loop thread did not stop after %s seconds",
                        EVENT_LOOP_STOP_TIMEOUT,
                    )

                with self._lifecycle_lock:
                    if self._loop is loop:
                        self._loop = None
                        self.js = None
                        self.nc = None
                    if self._loop_thread is loop_thread:
                        self._loop_ready = None
                        self._loop_start_error = None
                        self._loop_thread = None

    def _disconnect_from_event_loop(self) -> None:
        """Disconnect without blocking when called by the event loop thread."""
        loop = self._loop
        if not loop or loop.is_closed():
            return

        async def disconnect_and_stop() -> None:
            try:
                await self._disconnect()
            finally:
                loop.stop()

        loop.create_task(disconnect_and_stop())

    def _reset_after_fork(self) -> None:
        """Reset state inherited from a parent process."""
        # Do not drain the inherited connection: protocol I/O from the child
        # would use the parent's underlying socket. Drop process-local object
        # state and let the child establish its own connection lazily.
        self._lifecycle_lock = threading.Lock()
        self._loop = None
        self._loop_ready = None
        self._loop_start_error = None
        self._loop_thread = None
        self._pid = os.getpid()
        self._publish_lock = threading.Lock()
        self.js = None
        self.nc = None

    def _run_event_loop(self, ready: threading.Event) -> None:
        """Run the nats-py event loop until disconnect stops it."""
        loop = None
        try:
            loop = _new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
        except BaseException as exc:  # Always release a waiter if thread setup fails.
            self._loop_start_error = exc
        finally:
            ready.set()

        if not loop or self._loop_start_error:
            if loop and not loop.is_closed():
                loop.close()
            return

        try:
            loop.run_forever()
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()
                with self._lifecycle_lock:
                    if self._loop is loop:
                        self._loop = None
                        self.js = None
                        self.nc = None
                    if self._loop_thread is threading.current_thread():
                        self._loop_ready = None
                        self._loop_thread = None

    def _ensure_event_loop(self) -> asyncio.AbstractEventLoop:
        """Start the process-local event loop thread when first needed."""
        if self._pid != os.getpid():
            self._reset_after_fork()

        with self._lifecycle_lock:
            if (
                self._loop
                and not self._loop.is_closed()
                and self._loop_thread
                and self._loop_thread.is_alive()
            ):
                return self._loop

            if self._loop_thread and self._loop_thread.is_alive() and self._loop_ready:
                ready = self._loop_ready
            else:
                # A connection belongs to the loop that created it. If that
                # loop exited unexpectedly, discard its connection state rather
                # than attempting to reuse it from the replacement loop.
                self._loop = None
                self._loop_start_error = None
                self.js = None
                self.nc = None

                ready = threading.Event()
                self._loop_ready = ready
                self._loop_thread = threading.Thread(
                    target=self._run_event_loop,
                    args=(ready,),
                    name=f"nautobot-broker-nats-{id(self):x}",
                    daemon=True,
                )
                self._loop_thread.start()

            if not ready.wait(timeout=EVENT_LOOP_START_TIMEOUT):
                raise RuntimeError(
                    f"NATS event loop did not start within {EVENT_LOOP_START_TIMEOUT} seconds"
                )

            if self._loop_start_error:
                raise RuntimeError(
                    "Failed to start the NATS event loop"
                ) from self._loop_start_error
            if (
                not self._loop
                or self._loop.is_closed()
                or not self._loop_thread
                or not self._loop_thread.is_alive()
            ):
                raise RuntimeError("NATS event loop exited during startup")
            return self._loop

    def _discard_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Discard a loop that closed immediately before coroutine submission."""
        with self._lifecycle_lock:
            if self._loop is loop:
                self._loop = None
                self._loop_ready = None
                self._loop_start_error = None
                self._loop_thread = None
                self.js = None
                self.nc = None

    def _run(
        self,
        coroutine: typing.Coroutine[typing.Any, typing.Any, typing.Any],
        timeout: float,
    ) -> typing.Any:
        """Run a coroutine on the persistent NATS event loop."""
        retry_closed_loop = True
        while True:
            try:
                loop = self._ensure_event_loop()
            except BaseException:
                coroutine.close()
                raise
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            except RuntimeError:
                if not retry_closed_loop or not loop.is_closed():
                    coroutine.close()
                    raise
                retry_closed_loop = False
                self._discard_event_loop(loop)
            else:
                try:
                    return future.result(timeout=timeout)
                except concurrent.futures.TimeoutError:
                    # A coroutine may itself raise TimeoutError. Only cancel
                    # here when the submitted operation is still incomplete.
                    if future.done():
                        raise
                    future.cancel()
                    raise TimeoutError(f"NATS publish timed out after {timeout} seconds") from None

    def publish(self, data: dict) -> None:  # noqa: D102
        if self._pid != os.getpid():
            self._reset_after_fork()

        msg = orjson.dumps(data, default=lambda obj: str(obj))

        with self._publish_lock:
            self._run(self._publish(msg), timeout=self.publish_timeout)

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
                    log.warning("disconnect: %s", e)

        self.js = None
        self.nc = None

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
                log.warning("publish [%d]: %s", n, e)

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
