from __future__ import annotations

import queue
import threading
from collections.abc import Iterator
from typing import Any


class LocalTopicBus:
    """Small in-process topic bus for local simulator-to-live replay."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: dict[str, set[queue.Queue[dict[str, Any]]]] = {}

    def publish(self, topic: str, payload: dict[str, Any]) -> int:
        with self._lock:
            subscribers = list(self._subscribers.get(topic, set()))
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(payload)
            except queue.Full:
                try:
                    subscriber.get_nowait()
                except queue.Empty:
                    pass
                subscriber.put_nowait(payload)
        return len(subscribers)

    def open_subscription(self, topic: str) -> queue.Queue[dict[str, Any]]:
        subscriber: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=2000)
        with self._lock:
            self._subscribers.setdefault(topic, set()).add(subscriber)
        return subscriber

    def close_subscription(self, topic: str, subscriber: queue.Queue[dict[str, Any]]) -> None:
        with self._lock:
            subscribers = self._subscribers.get(topic)
            if not subscribers:
                return
            subscribers.discard(subscriber)
            if not subscribers:
                self._subscribers.pop(topic, None)

    def subscribe(self, topic: str, timeout_s: float = 1.0) -> Iterator[dict[str, Any] | None]:
        subscriber = self.open_subscription(topic)
        try:
            while True:
                try:
                    yield subscriber.get(timeout=timeout_s)
                except queue.Empty:
                    yield None
        finally:
            self.close_subscription(topic, subscriber)


LOCAL_TOPIC_BUS = LocalTopicBus()
