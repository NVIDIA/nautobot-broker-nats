# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: APACHE 2.0

import asyncio
import logging
import os
import threading

import pytest

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

        def close_when_idle():
            connection.is_closed = True
            idle_disconnect_processed.set()

        asyncio.get_running_loop().call_soon(close_when_idle)
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


def test_event_loop_start_failure_releases_waiter(monkeypatch):
    """Raise promptly if the event loop thread cannot initialize."""

    def fail_to_create_loop():
        raise OSError("event loop unavailable")

    monkeypatch.setattr(client_module.asyncio, "new_event_loop", fail_to_create_loop)
    broker = client_module.NATS(stream="nautobot")

    with pytest.raises(RuntimeError, match="Failed to start") as exc_info:
        broker.publish({"event": "test"})

    assert isinstance(exc_info.value.__cause__, OSError)
    broker.disconnect()


def test_event_loop_start_wait_is_bounded(monkeypatch):
    """Stop waiting if an event loop thread never reports readiness."""
    release_thread = threading.Event()
    broker = client_module.NATS(stream="nautobot")

    def never_ready(_ready):
        release_thread.wait(timeout=1)

    monkeypatch.setattr(client_module, "EVENT_LOOP_START_TIMEOUT", 0.01)
    monkeypatch.setattr(broker, "_run_event_loop", never_ready)

    with pytest.raises(RuntimeError, match="did not start within"):
        broker.publish({"event": "test"})

    release_thread.set()
    broker._loop_thread.join(timeout=1)  # pylint: disable=protected-access
    broker.disconnect()


def test_disconnect_before_publish_is_idempotent():
    """Allow repeated cleanup before the event loop has started."""
    broker = client_module.NATS(stream="nautobot")

    broker.disconnect()
    broker.disconnect()

    assert broker._loop is None  # pylint: disable=protected-access
    assert broker._loop_thread is None  # pylint: disable=protected-access


def test_disconnect_timeout_still_stops_event_loop(monkeypatch, caplog):
    """Bound shutdown if draining an unreachable connection does not finish."""
    drain_started = threading.Event()

    class HangingConnection(FakeConnection):
        async def drain(self):
            drain_started.set()
            await asyncio.Event().wait()

    async def connect(**_kwargs):
        return HangingConnection([])

    monkeypatch.setattr(client_module.nats, "connect", connect)
    monkeypatch.setattr(client_module, "DISCONNECT_TIMEOUT", 0.01)
    broker = client_module.NATS(stream="nautobot")
    broker.publish({"event": "test"})
    loop_thread = broker._loop_thread  # pylint: disable=protected-access

    with caplog.at_level(logging.WARNING, logger="nautobot.broker.nats"):
        broker.disconnect()

    assert drain_started.is_set()
    assert "disconnect timed out" in caplog.text
    assert loop_thread is not None
    assert not loop_thread.is_alive()
    assert broker._loop is None  # pylint: disable=protected-access


def test_pid_mismatch_replaces_inherited_locked_state(monkeypatch):
    """Reset process-local locks before publish tries to acquire them."""
    published = []

    async def connect(**_kwargs):
        return FakeConnection(published)

    monkeypatch.setattr(client_module.nats, "connect", connect)
    broker = client_module.NATS(stream="nautobot")
    inherited_publish_lock = broker._publish_lock  # pylint: disable=protected-access
    inherited_publish_lock.acquire()
    broker._pid = -1  # pylint: disable=protected-access

    try:
        broker.publish({"event": "child"})
    finally:
        inherited_publish_lock.release()
        broker.disconnect()

    assert broker._pid == os.getpid()  # pylint: disable=protected-access
    assert broker._publish_lock is not inherited_publish_lock  # pylint: disable=protected-access
    assert [message for _, message, _ in published] == [b'{"event":"child"}']


def test_unexpected_loop_exit_discards_stale_connection(monkeypatch):
    """Create a fresh connection if the persistent event loop exits."""
    connections = []
    published = []

    async def connect(**_kwargs):
        connection = FakeConnection(published)
        connections.append(connection)
        return connection

    monkeypatch.setattr(client_module.nats, "connect", connect)
    broker = client_module.NATS(stream="nautobot")

    try:
        broker.publish({"event": "first"})
        old_loop = broker._loop  # pylint: disable=protected-access
        old_loop_thread = broker._loop_thread  # pylint: disable=protected-access
        old_loop.call_soon_threadsafe(old_loop.stop)
        old_loop_thread.join(timeout=1)

        assert not old_loop_thread.is_alive()

        broker.publish({"event": "second"})
    finally:
        broker.disconnect()

    assert len(connections) == 2
    assert [message for _, message, _ in published] == [
        b'{"event":"first"}',
        b'{"event":"second"}',
    ]


def test_closed_loop_submission_is_retried_once(monkeypatch):
    """Recover if a loop closes immediately before coroutine submission."""
    published = []

    async def connect(**_kwargs):
        return FakeConnection(published)

    monkeypatch.setattr(client_module.nats, "connect", connect)
    broker = client_module.NATS(stream="nautobot")
    ensure_event_loop = broker._ensure_event_loop  # pylint: disable=protected-access
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()
    calls = 0

    def race_once():
        nonlocal calls
        calls += 1
        if calls == 1:
            return closed_loop
        return ensure_event_loop()

    monkeypatch.setattr(broker, "_ensure_event_loop", race_once)

    try:
        broker.publish({"event": "test"})
    finally:
        broker.disconnect()

    assert calls == 2
    assert [message for _, message, _ in published] == [b'{"event":"test"}']


def test_disconnect_called_from_event_loop_does_not_deadlock(monkeypatch):
    """Let the event loop schedule its own asynchronous shutdown."""

    async def connect(**_kwargs):
        return FakeConnection([])

    monkeypatch.setattr(client_module.nats, "connect", connect)
    broker = client_module.NATS(stream="nautobot")
    broker.publish({"event": "test"})
    loop = broker._loop  # pylint: disable=protected-access
    loop_thread = broker._loop_thread  # pylint: disable=protected-access
    disconnect_called = threading.Event()

    def disconnect_on_loop():
        broker.disconnect()
        disconnect_called.set()

    loop.call_soon_threadsafe(disconnect_on_loop)

    assert disconnect_called.wait(timeout=1)
    loop_thread.join(timeout=1)
    assert not loop_thread.is_alive()
