"""Redis Lists + Pub/Sub worker with independent, explicitly registered routes."""

import logging
import signal
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import redis


@dataclass(frozen=True)
class QueueRoute:
    queue: str
    event: str
    complete_event: str
    handler: Callable[[str, "TaskContext"], None]
    workers: int = 1

    def __post_init__(self):
        for name in ("queue", "event", "complete_event"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not callable(self.handler):
            raise TypeError("handler must be callable")
        if isinstance(self.workers, bool) or not isinstance(self.workers, int) or self.workers < 1:
            raise ValueError("workers must be a positive integer")


@dataclass(frozen=True)
class TaskContext:
    """Per-task timing and completion bound to the originating route."""

    _redis: redis.Redis
    _channel: str
    _complete_list: str
    _complete_event: str
    _started: float = field(default_factory=time.perf_counter)

    def execution_duration(self) -> float:
        return time.perf_counter() - self._started

    def complete(self, data: str = "") -> None:
        with self._redis.pipeline(transaction=True) as transaction:
            transaction.rpush(self._complete_list, data)
            transaction.publish(self._channel, self._complete_event)
            transaction.execute()


class RedisWorker:
    def __init__(
        self,
        routes: Iterable[QueueRoute],
        prefix: str = "redis_worker",
        host: str = "localhost",
        port: int = 6379,
    ):
        self.routes = tuple(routes)
        if not self.routes:
            raise ValueError("at least one route is required")
        if any(not isinstance(route, QueueRoute) for route in self.routes):
            raise ValueError("routes must contain QueueRoute instances")
        if len({route.queue for route in self.routes}) != len(self.routes):
            raise ValueError("queue names must be unique")
        if not isinstance(prefix, str) or not prefix.strip():
            raise ValueError("prefix must be a non-empty string")
        self._prefix = prefix
        self._host = host
        self._port = port
        self._stop = threading.Event()
        self._wakeups = {route.queue: threading.Event() for route in self.routes}
        self._logger = logging.getLogger("RedisWorker")
        self._started = False

    def _channel(self, route: QueueRoute) -> str:
        return f"{self._prefix}_{route.queue}"

    def stop(self, *_):
        """Stop consuming and wait for active handlers when run() exits."""
        self._stop.set()
        for wakeup in self._wakeups.values():
            wakeup.set()

    def run(self):
        if self._started:
            raise RuntimeError("RedisWorker can only be run once")
        self._started = True
        client = redis.Redis(host=self._host, port=self._port, decode_responses=True)
        pubsub = None
        executors = []
        futures = []
        previous_signals = {}
        try:
            if threading.current_thread() is threading.main_thread():
                for signum in (signal.SIGINT, signal.SIGTERM):
                    previous_signals[signum] = signal.signal(signum, self.stop)
            pubsub = client.pubsub()
            routes_by_message = {(self._channel(route), route.event): route for route in self.routes}
            pubsub.subscribe(*(self._channel(route) for route in self.routes))
            for route in self.routes:
                executor = ThreadPoolExecutor(
                    max_workers=route.workers, thread_name_prefix=f"redis-{route.queue}"
                )
                executors.append(executor)
                for _ in range(route.workers):
                    futures.append(executor.submit(self._worker_loop, client, route))
            while not self._stop.is_set():
                for future in futures:
                    if future.done():
                        future.result()  # Surface transport failures instead of silently losing a worker.
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message and message["type"] == "message":
                    route = routes_by_message.get((message["channel"], message["data"]))
                    if route is not None:
                        self._wakeups[route.queue].set()
        finally:
            self.stop()
            for executor in executors:
                executor.shutdown(wait=True)
            if pubsub is not None:
                pubsub.close()
            client.close()
            for signum, previous in previous_signals.items():
                signal.signal(signum, previous)

    def _worker_loop(self, client: redis.Redis, route: QueueRoute):
        channel = self._channel(route)
        task_list = f"{channel}List"
        wakeup = self._wakeups[route.queue]
        while not self._stop.is_set():
            # Clear before reading: a notification during LPOP must remain visible.
            wakeup.clear()
            raw = client.lpop(task_list)
            if raw is None:
                # Poll as a fallback for missed Pub/Sub notifications.
                wakeup.wait(timeout=1)
                continue
            context = TaskContext(client, channel, f"{channel}CompleteList", route.complete_event)
            try:
                route.handler(raw, context)
            except Exception:
                self._logger.exception("Task failed; returning to queue %s", route.queue)
                with client.pipeline(transaction=True) as transaction:
                    transaction.rpush(task_list, raw)
                    transaction.publish(channel, route.event)
                    transaction.execute()
