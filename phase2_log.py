"""
Phase 2 — Persistent Log
========================
Replace the in-memory queue with an append-only log on disk.

Key ideas:
  - Every write is an APPEND — we never modify or delete past entries.
  - Each message gets a monotonically increasing offset (its position).
  - On restart, the log is read back from disk — messages survive crashes.

Log file format (one JSON line per message):
    {"offset": 0, "timestamp": "...", "key": "order-1", "value": "order.placed"}
    {"offset": 1, "timestamp": "...", "key": "order-2", "value": "order.shipped"}
    ...

Run:
    python phase2_log.py
    python phase2_log.py          # run again — messages are still there!
"""

import json
import os
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Any, Iterator, Optional


LOG_FILE = "topic.log"


@dataclass
class LogEntry:
    offset: int
    timestamp: str
    key: Optional[str]
    value: Any


class PersistentLog:
    """
    An append-only log backed by a plain text file.

    Why append-only?
    ----------------
    Sequential disk writes are the fastest I/O pattern — no random seeks.
    This is the core reason Kafka achieves high throughput even on spinning
    disks. Reads can happen at any offset independently of writes.
    """

    def __init__(self, path: str = LOG_FILE) -> None:
        self.path = path
        self._next_offset = self._recover_offset()

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def append(self, value: Any, key: Optional[str] = None) -> int:
        """
        Append a message to the log.
        Returns the offset assigned to this message.

        This is a sequential write — the OS appends bytes to the end of the
        file. No seeking, no overwriting, no locking contention on the
        existing data.
        """
        entry = LogEntry(
            offset=self._next_offset,
            timestamp=datetime.now(timezone.utc).isoformat(),
            key=key,
            value=value,
        )
        # 'a' mode = open for appending; creates the file if it doesn't exist
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(entry)) + "\n")

        assigned = self._next_offset
        self._next_offset += 1
        print(f"  [append]  offset={assigned}  '{value}'")
        return assigned

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def read(self, offset: int) -> Optional[LogEntry]:
        """Read the entry at a specific offset. O(offset) scan — fine for now."""
        for entry in self._scan():
            if entry.offset == offset:
                return entry
        return None

    def read_from(self, start_offset: int = 0) -> Iterator[LogEntry]:
        """Yield every entry at or after start_offset."""
        for entry in self._scan():
            if entry.offset >= start_offset:
                yield entry

    def size(self) -> int:
        return self._next_offset

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scan(self) -> Iterator[LogEntry]:
        """Iterate through every line in the log file."""
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    data = json.loads(line)
                    yield LogEntry(**data)

    def _recover_offset(self) -> int:
        """
        On startup, scan the log to find the highest offset written so far.
        This is how the broker knows where to resume after a crash or restart.
        """
        highest = -1
        for entry in self._scan():
            highest = max(highest, entry.offset)
        return highest + 1  # next offset = highest seen + 1


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print("Phase 2 — Persistent Log Demo")
    print("=" * 50)

    log = PersistentLog(LOG_FILE)

    print(f"\n--- Log opened. Recovered offset: {log._next_offset} ---")

    print("\n--- Appending messages ---")
    log.append("order.placed",    key="order-1")
    log.append("order.shipped",   key="order-2")
    log.append("order.delivered", key="order-3")

    print(f"\n--- Reading full log (all {log.size()} entries) ---")
    for entry in log.read_from(0):
        print(f"  offset={entry.offset}  key={entry.key}  value={entry.value}")

    print("\n--- Reading from offset 1 (skip first message) ---")
    for entry in log.read_from(1):
        print(f"  offset={entry.offset}  value={entry.value}")

    print("\n--- Point read at offset 0 ---")
    e = log.read(0)
    print(f"  {e}")

    print(f"\n✓ Log persisted to '{LOG_FILE}'.")
    print("  Run this script again — the old messages will still be there.")
    print("  → Next: Phase 3 lets multiple consumers track their own position.")