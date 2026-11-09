import os
import queue
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
import redis

from redis_route_worker import QueueRoute, RedisWorker, TaskContext


class MemoryRedis:
    """Thread-safe Redis double; preserves atomic transaction and Pub/Sub ordering."""

    def __init__(self):
        self.lists = defaultdict(deque)
        self.lock = threading.RLock()
        self.messages = queue.Queue()
        self.published = []
        self.channels = set()
        self.closed = False
        self.pubsub_closed = False
        self.subscribed = threading.Event()

    def lpop(self, key):
        with self.lock:
            return self.lists[key].popleft() if self.lists[key] else None

    def rpush(self, key, value):
        with self.lock:
            self.lists[key].append(value)

    def publish(self, channel, data):
        with self.lock:
            self.published.append((channel, data))
            if channel in self.channels:
                self.messages.put({"type": "message", "channel": channel, "data": data})

    def pipeline(self, transaction=True):
        assert transaction
        client = self

        class Transaction:
            def __init__(self):
                self.commands = []

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def rpush(self, *args):
                self.commands.append((client.rpush, args))

            def publish(self, *args):
                self.commands.append((client.publish, args))

            def execute(self):
                with client.lock:
                    for command, args in self.commands:
                        command(*args)

        return Transaction()

    def pubsub(self):
        client = self

        class PubSub:
            def subscribe(self, *channels):
                client.channels.update(channels)
                client.subscribed.set()

            def get_message(self, **kwargs):
                try:
                    return client.messages.get(timeout=0.01)
                except queue.Empty:
                    return None

            def close(self):
                client.pubsub_closed = True

        return PubSub()

    def close(self):
        self.closed = True


@pytest.fixture
def client(monkeypatch):
    client = MemoryRedis()
    monkeypatch.setattr("redis_route_worker.worker.redis.Redis", lambda **_: client)
    return client


def test_routes_and_backlog(client):
    done = threading.Event()

    def handle(raw, context):
        context.complete(raw)
        if len(client.published) == 2:
            done.set()

    worker = RedisWorker([
        QueueRoute("a", "newA", "doneA", handle),
        QueueRoute("b", "newB", "doneB", handle),
    ], prefix="test")
    client.rpush("test_aList", "A")
    client.rpush("test_bList", "B")
    with ThreadPoolExecutor() as pool:
        future = pool.submit(worker.run)
        try:
            assert done.wait(3)
        finally:
            worker.stop()
        future.result(timeout=3)
    assert list(client.lists["test_aCompleteList"]) == ["A"]
    assert list(client.lists["test_bCompleteList"]) == ["B"]
    assert set(client.published) == {("test_a", "doneA"), ("test_b", "doneB")}
    assert client.closed and client.pubsub_closed


def test_dispatch_ignores_unknown_events(client):
    worker = RedisWorker([QueueRoute("a", "new", "done", Mock())], prefix="test")
    wakeup = Mock()
    worker._wakeups["a"] = wakeup
    # Keep the workers out of this test to observe listener dispatch only.
    worker._worker_loop = lambda *_: worker._stop.wait()
    with ThreadPoolExecutor() as pool:
        future = pool.submit(worker.run)
        assert client.subscribed.wait(3)
        consumed = threading.Event()
        wakeup.set.side_effect = consumed.set
        client.publish("test_a", "unknown")
        client.publish("test_a", "new")
        try:
            # An ordered sentinel tells us both messages passed through the listener.
            assert consumed.wait(3)
            assert wakeup.set.call_count == 1
        finally:
            worker.stop()
        future.result(timeout=3)


def test_route_limits_and_graceful_stop(client):
    lock = threading.Lock()
    active = 0
    peak = 0
    both_started = threading.Event()
    release = threading.Event()
    other_finished = threading.Event()

    def slow(raw, context):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == 2:
                both_started.set()
        assert release.wait(5)
        context.complete(raw)
        with lock:
            active -= 1

    worker = RedisWorker([
        QueueRoute("slow", "new", "done", slow, workers=2),
        QueueRoute("fast", "new", "done", lambda *_: other_finished.set()),
    ], prefix="test")
    for raw in ("1", "2", "3"):
        client.rpush("test_slowList", raw)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(worker.run)
        try:
            assert both_started.wait(3)
            client.rpush("test_fastList", "fast")
            client.publish("test_fast", "new")
            assert other_finished.wait(3)
            worker.stop()
            assert not future.done()
        finally:
            worker.stop()
            release.set()
        future.result(timeout=3)
    assert peak == 2
    assert list(client.lists["test_slowList"]) == ["3"]
    assert len(client.lists["test_slowCompleteList"]) == 2


def test_exception_requeues_original_task(client):
    def fail(raw, context):
        worker.stop()
        raise ValueError("bad task")

    route = QueueRoute("a", "new", "done", fail)
    worker = RedisWorker([route], prefix="test")
    client.rpush("test_aList", '{"original":true}')
    worker._worker_loop(client, route)
    assert list(client.lists["test_aList"]) == ['{"original":true}']
    assert client.published == [("test_a", "new")]


@pytest.mark.parametrize("kwargs", [
    {"queue": " "}, {"event": ""}, {"complete_event": ""},
    {"handler": None}, {"workers": 0}, {"workers": True}, {"workers": 1.5},
])
def test_invalid_route(kwargs):
    options = {"queue": "a", "event": "new", "complete_event": "done", "handler": lambda *_: None}
    options.update(kwargs)
    with pytest.raises((ValueError, TypeError)):
        QueueRoute(**options)


def test_duplicate_and_empty_routes():
    route = QueueRoute("a", "new", "done", lambda *_: None)
    with pytest.raises(ValueError):
        RedisWorker([route, route])
    with pytest.raises(ValueError):
        RedisWorker([])


def test_transport_failure_closes_resources(client):
    client.lpop = Mock(side_effect=redis.ConnectionError("offline"))
    worker = RedisWorker([QueueRoute("a", "new", "done", lambda *_: None)])
    with pytest.raises(redis.ConnectionError):
        worker.run()
    assert client.closed and client.pubsub_closed


def test_completion_on_local_redis():
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to run against local Redis")
    from uuid import uuid4

    client = redis.Redis.from_url(url, decode_responses=True)
    key = f"queue-test-{uuid4().hex}"
    pubsub = client.pubsub()
    try:
        pubsub.subscribe(key)
        assert pubsub.get_message(timeout=3)["type"] == "subscribe"
        context = TaskContext(client, key, f"{key}CompleteList", "finished")
        context.complete("result")
        assert client.lpop(f"{key}CompleteList") == "result"
        assert pubsub.get_message(timeout=3)["data"] == "finished"
        assert context.execution_duration() >= 0
    finally:
        client.delete(f"{key}CompleteList")
        pubsub.close()
        client.close()


def test_multiple_routes_on_local_redis():
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to run against local Redis")
    from uuid import uuid4

    client = redis.Redis.from_url(url, decode_responses=True)
    options = client.connection_pool.connection_kwargs
    prefix = f"queue-test-{uuid4().hex}"
    completed = {name: threading.Event() for name in ("a", "b")}

    def handler(name):
        def process(raw, context):
            context.complete(raw)
            completed[name].set()
        return process

    worker = RedisWorker([
        QueueRoute("a", "newA", "doneA", handler("a")),
        QueueRoute("b", "newB", "doneB", handler("b")),
    ], prefix=prefix, host=options["host"], port=options["port"])
    # The worker uses DB 0; use the same DB for this integration scenario.
    transport = redis.Redis(host=options["host"], port=options["port"], decode_responses=True)
    keys = [f"{prefix}_{name}{suffix}" for name in ("a", "b")
            for suffix in ("List", "CompleteList")]
    try:
        transport.rpush(f"{prefix}_aList", "backlog")
        with ThreadPoolExecutor() as pool:
            future = pool.submit(worker.run)
            try:
                assert completed["a"].wait(3)
                transport.rpush(f"{prefix}_bList", "new task")
                transport.publish(f"{prefix}_b", "newB")
                assert completed["b"].wait(3)
            finally:
                worker.stop()
            future.result(timeout=3)
        assert transport.lrange(f"{prefix}_aCompleteList", 0, -1) == ["backlog"]
        assert transport.lrange(f"{prefix}_bCompleteList", 0, -1) == ["new task"]
    finally:
        transport.delete(*keys)
        transport.close()
        client.close()
