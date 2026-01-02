#  SPDX-FileCopyrightText: Copyright (c) "2025" NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#  SPDX-License-Identifier: APACHE 2.0


import asyncio
import atexit
import nats
import orjson
import threading
import time
import typing

from .log import log

# Serialize event loop access.
lock = threading.Lock()

# Create the event loop.
loop = asyncio.new_event_loop()

# Maximum connection age.
max_age = 60


# NATS wraps the official NATS library for Python, which requires asyncio.
class NATS:
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

        # Initialize the NATS connection, JetStream context, and last used
        # timestamp attributes.
        self.nc = None
        self.js = None
        self.ts = None

        # Ensure a graceful disconnect.
        atexit.register(self.disconnect)

    def disconnect(self) -> None:
        with lock:
            loop.run_until_complete(self._disconnect())

    def publish(self, data: dict) -> None:
        msg = orjson.dumps(data, default=lambda obj: str(obj))

        with lock:
            loop.run_until_complete(self._publish(msg))

    async def _connect(self) -> None:
        # Connect to NATS.
        self.nc = await nats.connect(servers=self.servers, **self.connect)
        self.ts = time.time()

        # Retrieve the JetStream context, and ensure the stream exists. This
        # will raise an exception if it does not.
        if self.stream:
            self.js = self.nc.jetstream()

            await self.js.stream_info(self.stream)

    async def _disconnect(self) -> None:
        # If necessary, disconnect from NATS.
        if self.nc:
            await self.nc.close()

        self.nc = None
        self.js = None
        self.ts = None

    # Publish the message, retrying if necessary with an increasing delay
    # between attempts.
    async def _publish(self, msg: bytes) -> None:
        # Force a reconnect if the connection has not been used recently.
        if self.ts and time.time() - self.ts >= max_age:
            await self._disconnect()

        for n in range(self.attempt):
            try:
                # Connect if necessary.
                if not self.nc:
                    await self._connect()

                if self.stream:
                    # JetStream publish. There is no need to flush.
                    await self.js.publish(self.subject, msg)
                else:
                    # Core publish.
                    await self.nc.publish(self.subject, msg)
                    await self.nc.flush()

            except Exception as e:
                log.warning("publish [%d]: %s" % (n, e))

                # Force a reconnect.
                await self._disconnect()

                # Last attempt? Propagate the exception to the caller.
                if n + 1 == self.attempt:
                    raise e

                # Trying again? Sleep for a bit.
                time.sleep(n)

            else:
                # Success!
                self.ts = time.time()

                return
