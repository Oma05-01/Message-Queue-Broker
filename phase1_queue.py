"""
Phase 1 — In-Memory Queue
=========================
The simplest possible message broker: a deque.
Producer pushes to the back, consumer pops from the front (FIFO).

Run:
    python phase1_queue.py
"""

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Message:
    value: Any
    key: Optional[str] = None


class InMemoryQueue:
    """
    A single-topic, single-consumer in-memory queue.
    Messages are destroyed once consumed — no replay possible.
    """

    def __init__(self) -> None:
        self._queue: deque[Message] = deque()

    def publish(self, value: Any, key: Optional[str] = None) -> None:
        """Push a message to the back of the queue."""
        msg = Message(value=value, key=key)
        self._queue.append(msg)
        print(f"  [publish]  '{value}'  (queue size: {len(self._queue)})")

    def consume(self) -> Optional[Message]:
        """
        Pop the oldest message from the front.
        Returns None if the queue is empty.
        """
        if not self._queue:
            print("  [consume]  queue is empty")
            return None
        msg = self._queue.popleft()
        print(f"  [consume]  '{msg.value}'  (queue size: {len(self._queue)})")
        return msg

    def size(self) -> int:
        return len(self._queue)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print("Phase 1 — In-Memory Queue Demo")
    print("=" * 50)

    q = InMemoryQueue()

    print("\n--- Publishing 3 messages ---")
    q.publish("order.placed",   key="order-1")
    q.publish("order.shipped",  key="order-2")
    q.publish("order.delivered",key="order-3")

    print("\n--- Consuming messages (FIFO) ---")
    while True:
        msg = q.consume()
        if msg is None:
            break

    print("\n✓ Queue drained. Messages are gone — no replay possible.")
    print("  → Next: Phase 2 adds a persistent log so messages survive.")