#  SPDX-FileCopyrightText: Copyright (c) "2025" NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#  SPDX-License-Identifier: APACHE 2.0


import asyncio
import atexit
import threading
import typing
import uuid

import nats
import orjson

from .log import log

# Serialize event loop access.
lock = threading.Lock()

# Create the event loop.
loop = asyncio.new_event_loop()


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

        # Ensure a graceful disconnect on process exit.
        atexit.register(self.disconnect)

    def disconnect(self) -> None:  # noqa: D102
        # Guard against calling run_until_complete on a closed loop (e.g. if
        # Python's own asyncio cleanup has already closed it during interpreter
        # shutdown).
        if loop.is_closed():
            return

        with lock:
            loop.run_until_complete(self._disconnect())

    def publish(self, data: dict) -> None:  # noqa: D102
        msg = orjson.dumps(data, default=lambda obj: str(obj))

        with lock:
            loop.run_until_complete(self._publish(msg))

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
