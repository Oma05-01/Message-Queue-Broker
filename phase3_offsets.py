"""
Phase 3 — Consumer Offsets
===========================
Multiple consumers can read the same log independently by tracking
their own position (offset). Consumers are completely decoupled from
each other — one slow consumer doesn't block another.

New concepts:
  - OffsetStore: persists each consumer's position to disk
  - Consumer: wraps the log + stores its own offset
  - seekTo(): lets a consumer replay from any point

Run:
    python phase3_offsets.py
"""

import json
import os
from typing import Iterator, Optional

# Reuse Phase 2's log — we're building on top of it, not replacing it.
from phase2_log import PersistentLog, LogEntry


OFFSET_STORE_FILE = "consumer_offsets.json"


class OffsetStore:
    """
    Persists consumer offsets to a JSON file.

    In a real broker (Kafka), offsets are stored in an internal topic
    called __consumer_offsets. Here we use a simple JSON file.

    Format:
        {
            "consumer-A": 3,
            "consumer-B": 0
        }
    """

    def __init__(self, path: str = OFFSET_STORE_FILE) -> None:
        self.path = path
        self._offsets: dict[str, int] = self._load()

    def get(self, consumer_id: str) -> int:
        return self._offsets.get(consumer_id, 0)

    def set(self, consumer_id: str, offset: int) -> None:
        self._offsets[consumer_id] = offset
        self._flush()

    def all(self) -> dict[str, int]:
        return dict(self._offsets)

    def _load(self) -> dict[str, int]:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _flush(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._offsets, f, indent=2)


class Consumer:
    """
    A named consumer that reads from a PersistentLog.

    Key insight: the log is never modified by a consumer. Consuming
    just means "read the entry at my current offset, then increment."
    Two consumers can be at completely different positions simultaneously.
    """

    def __init__(
        self,
        consumer_id: str,
        log: PersistentLog,
        offset_store: OffsetStore,
    ) -> None:
        self.id = consumer_id
        self._log = log
        self._store = offset_store
        # Resume from last committed offset (survives restarts)
        self._offset = self._store.get(consumer_id)
        print(f"  [consumer:{self.id}]  resumed at offset {self._offset}")

    @property
    def offset(self) -> int:
        return self._offset

    def consume(self) -> Optional[LogEntry]:
        """
        Read the next message and advance the offset.

        The offset is committed AFTER processing — this is "at-least-once"
        delivery. If we crash between read and commit, we re-read on restart.
        (Phase 6 explores this tradeoff properly.)
        """
        entry = self._log.read(self._offset)
        if entry is None:
            return None
        self._offset += 1
        self._store.set(self.id, self._offset)  # commit
        return entry

    def consume_all(self) -> Iterator[LogEntry]:
        """Drain all remaining messages from current offset."""
        while True:
            entry = self.consume()
            if entry is None:
                break
            yield entry

    def seek_to(self, offset: int) -> None:
        """
        Jump to any offset — enables replay.

        seek_to(0)  → replay everything from the beginning
        seek_to(-1) → not valid; offsets start at 0
        """
        self._offset = max(0, offset)
        self._store.set(self.id, self._offset)
        print(f"  [consumer:{self.id}]  seeked to offset {self._offset}")

    def lag(self) -> int:
        """How many messages behind the end of the log is this consumer?"""
        return max(0, self._log.size() - self._offset)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print("Phase 3 — Consumer Offsets Demo")
    print("=" * 50)

    # Fresh log for demo clarity
    for f in ("demo3.log", "demo3_offsets.json"):
        if os.path.exists(f):
            os.remove(f)

    log = PersistentLog("demo3.log")
    store = OffsetStore("demo3_offsets.json")

    print("\n--- Producer: appending 5 messages ---")
    for i in range(5):
        log.append(f"event-{i}", key=f"key-{i}")

    print("\n--- Creating two independent consumers ---")
    a = Consumer("consumer-A", log, store)
    b = Consumer("consumer-B", log, store)

    print("\n--- Consumer A reads first 3 messages ---")
    for _ in range(3):
        entry = a.consume()
        if entry:
            print(f"  [A] offset={entry.offset}  value={entry.value}")

    print(f"\n  Consumer A lag: {a.lag()} message(s) behind")
    print(f"  Consumer B lag: {b.lag()} message(s) behind  (hasn't read anything)")

    print("\n--- Consumer B reads ALL messages ---")
    for entry in b.consume_all():
        print(f"  [B] offset={entry.offset}  value={entry.value}")

    print("\n--- Consumer A replays from the beginning ---")
    a.seek_to(0)
    for entry in a.consume_all():
        print(f"  [A replay] offset={entry.offset}  value={entry.value}")

    print("\n--- Offset store state ---")
    for cid, off in store.all().items():
        print(f"  {cid}: committed offset = {off}")

    print("\n✓ Two consumers, one log, completely independent positions.")
    print("  → Next: Phase 4 adds partitions for parallel throughput.")