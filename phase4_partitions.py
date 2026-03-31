"""
Phase 4 — Partitioning
======================
A single log becomes a bottleneck when you have many producers writing
simultaneously. Partitioning solves this by splitting one topic into N
independent logs (partitions) that can be written and read in parallel.

Key rule: messages with the same key ALWAYS go to the same partition.
This preserves ordering per key — e.g. all events for user-42 land in
the same partition, in order.

Run:
    python phase4_partitions.py
"""

import os
from typing import Any, Iterator, Optional

from phase2_log import PersistentLog, LogEntry
from phase3_offsets import OffsetStore, Consumer


class PartitionedTopic:
    """
    A topic backed by N independent PersistentLog instances.

    Each partition is a separate log file:
        topic_orders_p0.log
        topic_orders_p1.log
        topic_orders_p2.log

    Why partition?
    --------------
    - N partitions = N parallel writers = N× throughput
    - Each partition can live on a different disk or machine
    - Consumers can be assigned to specific partitions (Phase 5)

    Ordering guarantee:
    -------------------
    - Within a partition: strictly ordered (append-only log)
    - Across partitions: NO ordering guarantee
      → If order matters, use the same key so messages hash to same partition
    """

    def __init__(self, name: str, num_partitions: int = 3) -> None:
        self.name = name
        self.num_partitions = num_partitions
        # Each partition is its own PersistentLog file
        self.partitions: list[PersistentLog] = [
            PersistentLog(f"topic_{name}_p{i}.log")
            for i in range(num_partitions)
        ]
        print(f"  [topic:{name}]  opened with {num_partitions} partitions")

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def publish(self, value: Any, key: str = "default") -> tuple[int, int]:
        """
        Hash the key to select a partition, then append the message.
        Returns (partition_index, offset) so the caller knows where it landed.

        Same key → same partition, every time. This is the contract.
        """
        partition_index = self._hash(key)
        offset = self.partitions[partition_index].append(value, key=key)
        print(
            f"  [publish]  key='{key}'  "
            f"→ partition={partition_index}  offset={offset}"
        )
        return partition_index, offset

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def read(
        self, partition: int, offset: int
    ) -> Optional[LogEntry]:
        """Read a specific message from a specific partition."""
        return self.partitions[partition].read(offset)

    def read_from(
        self, partition: int, start_offset: int = 0
    ) -> Iterator[LogEntry]:
        """Read all messages from a partition starting at an offset."""
        return self.partitions[partition].read_from(start_offset)

    def total_messages(self) -> int:
        """Total messages across all partitions."""
        return sum(p.size() for p in self.partitions)

    # ------------------------------------------------------------------
    # Hashing
    # ------------------------------------------------------------------

    def _hash(self, key: str) -> int:
        """
        Map a key to a partition index using a simple polynomial hash.

            partition = hash(key) % N

        Properties:
        - Deterministic: same key always → same partition
        - Roughly uniform: keys spread across partitions evenly
        - Fast: O(len(key))

        Python's built-in hash() is non-deterministic across runs
        (randomised for security). We implement our own so the mapping
        is stable between restarts — critical for ordering guarantees.
        """
        h = 0
        for char in key:
            h = (h * 31 + ord(char)) % self.num_partitions
        return h

    def partition_stats(self) -> None:
        """Print how many messages are in each partition."""
        for i, p in enumerate(self.partitions):
            bar = "█" * p.size()
            print(f"  P{i}  [{bar:<20}]  {p.size()} msgs")


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 55)
    print("Phase 4 — Partitioning Demo")
    print("=" * 55)

    # Clean up previous runs
    for i in range(3):
        path = f"topic_orders_p{i}.log"
        if os.path.exists(path):
            os.remove(path)

    topic = PartitionedTopic("orders", num_partitions=3)

    print("\n--- Publishing messages with different keys ---")
    messages = [
        ("user-1",  "login"),
        ("user-2",  "login"),
        ("user-1",  "add_to_cart"),
        ("user-3",  "login"),
        ("user-1",  "checkout"),   # all user-1 events → same partition
        ("user-2",  "add_to_cart"),
        ("user-3",  "checkout"),
    ]
    for key, value in messages:
        topic.publish(value, key=key)

    print("\n--- Partition distribution ---")
    topic.partition_stats()

    print("\n--- Reading partition 0 in full ---")
    p0_idx = topic._hash("user-1")
    print(f"  (user-1 hashes to partition {p0_idx})")
    for entry in topic.read_from(p0_idx, start_offset=0):
        print(f"  offset={entry.offset}  key={entry.key}  value={entry.value}")

    print("\n--- Key ordering guarantee ---")
    print("  Publish 3 more user-1 events and check they're all in the same partition:")
    for event in ["payment.init", "payment.confirmed", "order.dispatched"]:
        p, off = topic.publish(event, key="user-1")
        assert p == p0_idx, "ordering guarantee broken!"
    print(f"  ✓ All user-1 events landed in partition {p0_idx}")

    print(f"\n✓ {topic.total_messages()} total messages across {topic.num_partitions} partitions.")
    print("  → Next: Phase 5 assigns partitions to consumers in a group.")