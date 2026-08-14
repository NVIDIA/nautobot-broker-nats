# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: APACHE 2.0

import asyncio
import threading

from nautobot_broker_nats import client as client_module


class FakeJetStream:
    """Minimal JetStream client used by the connection lifecycle tests."""

    def __init__(self, connection, published):
        self.connection = connection
        self.published = published

    async def stream_info(self, stream):
        """Accept stream validation during connection setup."""
        return stream

    async def publish(self, subject, message, headers):
        """Record a successful publish on an open connection."""
        if self.connection.is_closed:
            raise ConnectionError("connection is closed")
        self.published.append((subject, message, headers))


class FakeConnection:
    """Minimal nats-py connection used by the connection lifecycle tests."""

    def __init__(self, published):
        self.is_closed = False
        self._jetstream = FakeJetStream(self, published)

    def jetstream(self):
        """Return the fake JetStream context."""
        return self._jetstream

    async def drain(self):
        """Mark the fake connection closed."""
        self.is_closed = True

    async def close(self):
        """Mark the fake connection closed."""
        self.is_closed = True


def test_idle_disconnect_is_processed_before_next_publish(monkeypatch):
    """Keep nats-py callbacks running between synchronous publishes."""
    connections = []
    published = []
    idle_disconnect_processed = threading.Event()

    async def connect(**_kwargs):
        connection = FakeConnection(published)
        connections.append(connection)

        async def close_when_idle():
            await asyncio.sleep(0.01)
            connection.is_closed = True
            idle_disconnect_processed.set()

        asyncio.create_task(close_when_idle())
        return connection

    monkeypatch.setattr(client_module.nats, "connect", connect)
    broker = client_module.NATS(stream="nautobot")

    try:
        broker.publish({"event": "first"})

        assert idle_disconnect_processed.wait(timeout=1)

        broker.publish({"event": "second"})
    finally:
        broker.disconnect()

    assert len(connections) == 2
    assert [message for _, message, _ in published] == [
        b'{"event":"first"}',
        b'{"event":"second"}',
    ]
    assert published[0][2]["Nats-Msg-Id"] != published[1][2]["Nats-Msg-Id"]


def test_disconnect_stops_event_loop_thread(monkeypatch):
    """Release the background thread when the broker disconnects."""

    async def connect(**_kwargs):
        return FakeConnection([])

    monkeypatch.setattr(client_module.nats, "connect", connect)
    broker = client_module.NATS(stream="nautobot")
    broker.publish({"event": "test"})
    loop_thread = broker._loop_thread  # pylint: disable=protected-access

    broker.disconnect()

    assert loop_thread is not None
    assert not loop_thread.is_alive()
    assert broker._loop is None  # pylint: disable=protected-access
