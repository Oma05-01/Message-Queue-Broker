"""
Phase 5 — Consumer Groups
==========================
A consumer group is a set of consumers that cooperate to read a topic.
Each partition is assigned to exactly ONE consumer in the group at a time.

This gives you two things:
  1. Parallelism — multiple consumers share the work
  2. No duplicates — within a group, each message is processed once

When consumers join or leave, partitions are redistributed automatically.
This redistribution is called a REBALANCE.

Run:
    python phase5_groups.py
"""

import os
from typing import Iterator, Optional

from phase2_log import LogEntry
from phase3_offsets import OffsetStore
from phase4_partitions import PartitionedTopic


class GroupConsumer:
    """
    A consumer that belongs to a group and is assigned specific partitions.

    It only reads from its assigned partitions — it ignores the rest.
    When the group rebalances, its assignments may change.
    """

    def __init__(
        self,
        consumer_id: str,
        group_id: str,
        topic: PartitionedTopic,
        offset_store: OffsetStore,
    ) -> None:
        self.id = consumer_id
        self.group_id = group_id
        self._topic = topic
        self._store = offset_store
        self.assigned_partitions: list[int] = []
        # One offset cursor per partition this consumer might own
        self._offsets: dict[int, int] = {}

    def assign(self, partitions: list[int]) -> None:
        """
        Called by the ConsumerGroup during rebalance.
        Updates which partitions this consumer is responsible for.
        """
        self.assigned_partitions = partitions
        # Load saved offsets for newly assigned partitions
        for p in partitions:
            store_key = f"{self.group_id}:{self.id}:p{p}"
            self._offsets[p] = self._store.get(store_key)
        print(
            f"  [consumer:{self.id}]  assigned partitions: "
            f"{partitions}"
        )

    def consume(self) -> Optional[LogEntry]:
        """
        Read the next available message from any assigned partition.
        Checks partitions in order — round-robin would be fairer but
        this is simpler for learning purposes.
        """
        for partition in self.assigned_partitions:
            offset = self._offsets.get(partition, 0)
            entry = self._topic.read(partition, offset)
            if entry is not None:
                # Advance and commit offset for this partition
                self._offsets[partition] = offset + 1
                store_key = f"{self.group_id}:{self.id}:p{partition}"
                self._store.set(store_key, offset + 1)
                return entry
        return None  # all assigned partitions are caught up

    def consume_all(self) -> Iterator[LogEntry]:
        """Drain all messages from all assigned partitions."""
        while True:
            entry = self.consume()
            if entry is None:
                break
            yield entry

    def lag(self) -> dict[int, int]:
        """
        Return per-partition lag: how many messages behind is this consumer?
        """
        result = {}
        for p in self.assigned_partitions:
            total = self._topic.partitions[p].size()
            consumed = self._offsets.get(p, 0)
            result[p] = max(0, total - consumed)
        return result


class ConsumerGroup:
    """
    Manages a set of GroupConsumers reading from one PartitionedTopic.

    The core responsibility is REBALANCING — deciding which consumer
    gets which partitions whenever membership changes.

    Rule: every partition must be assigned to exactly one consumer.
    No partition can be assigned to two consumers (would cause duplicates).
    No partition can be unassigned (would cause messages to be skipped).

    Strategy used here: round-robin
        partitions [0, 1, 2], consumers [A, B]
        → A gets [0, 2], B gets [1]
    """

    def __init__(
        self,
        group_id: str,
        topic: PartitionedTopic,
    ) -> None:
        self.group_id = group_id
        self.topic = topic
        self._store = OffsetStore(f"offsets_{group_id}.json")
        self._members: list[GroupConsumer] = []

    def join(self, consumer_id: str) -> GroupConsumer:
        """
        Add a new consumer to the group and trigger a rebalance.
        Returns the new consumer so the caller can use it.
        """
        consumer = GroupConsumer(
            consumer_id=consumer_id,
            group_id=self.group_id,
            topic=self.topic,
            offset_store=self._store,
        )
        self._members.append(consumer)
        print(f"\n  [group:{self.group_id}]  '{consumer_id}' joined  "
              f"({len(self._members)} member(s))")
        self._rebalance()
        return consumer

    def leave(self, consumer_id: str) -> None:
        """Remove a consumer and rebalance the remaining members."""
        self._members = [m for m in self._members if m.id != consumer_id]
        print(f"\n  [group:{self.group_id}]  '{consumer_id}' left  "
              f"({len(self._members)} member(s))")
        if self._members:
            self._rebalance()

    def _rebalance(self) -> None:
        """
        Redistribute all partitions across current members using round-robin.

        Example with 3 partitions, 2 consumers:
            partition 0 → consumer 0 (0 % 2 = 0)
            partition 1 → consumer 1 (1 % 2 = 1)
            partition 2 → consumer 0 (2 % 2 = 0)
        Result: consumer 0 → [P0, P2], consumer 1 → [P1]

        In a real broker (Kafka), rebalancing is more sophisticated:
        it tries to minimise partition movement to avoid re-reading.
        """
        if not self._members:
            return

        print(f"  [group:{self.group_id}]  rebalancing "
              f"{self.topic.num_partitions} partitions "
              f"across {len(self._members)} consumer(s)...")

        # Build empty assignment list for each member
        assignments: dict[str, list[int]] = {
            m.id: [] for m in self._members
        }

        # Round-robin: partition i → member at index (i % num_members)
        for partition_idx in range(self.topic.num_partitions):
            member = self._members[partition_idx % len(self._members)]
            assignments[member.id].append(partition_idx)

        # Push assignments to each consumer
        for member in self._members:
            member.assign(assignments[member.id])

    def print_assignments(self) -> None:
        """Show the current partition → consumer mapping."""
        print(f"\n  Partition assignments for group '{self.group_id}':")
        for member in self._members:
            lag = member.lag()
            total_lag = sum(lag.values())
            print(
                f"    {member.id:15s}  "
                f"partitions={member.assigned_partitions}  "
                f"lag={total_lag}"
            )


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 55)
    print("Phase 5 — Consumer Groups Demo")
    print("=" * 55)

    # Clean up previous runs
    for i in range(3):
        path = f"topic_events_p{i}.log"
        if os.path.exists(path):
            os.remove(path)
    if os.path.exists("offsets_analytics.json"):
        os.remove("offsets_analytics.json")

    # Set up a topic and publish some messages
    topic = PartitionedTopic("events", num_partitions=3)

    print("\n--- Publishing 9 messages across partitions ---")
    events = [
        ("user-1", "page_view"),  ("user-2", "page_view"),
        ("user-3", "page_view"),  ("user-1", "click"),
        ("user-2", "purchase"),   ("user-3", "click"),
        ("user-1", "logout"),     ("user-2", "logout"),
        ("user-3", "purchase"),
    ]
    for key, value in events:
        topic.publish(value, key=key)

    # ---------------------------------------------------------------
    print("\n\n--- Scenario 1: one consumer, all partitions ---")
    group = ConsumerGroup("analytics", topic)
    c1 = group.join("consumer-1")
    group.print_assignments()

    print("\n  consumer-1 reads everything:")
    for entry in c1.consume_all():
        print(f"    [{entry.key}] {entry.value}")

    # ---------------------------------------------------------------
    print("\n\n--- Scenario 2: second consumer joins → rebalance ---")
    # Reset offsets so we can re-read for the demo
    if os.path.exists("offsets_analytics.json"):
        os.remove("offsets_analytics.json")
    group2 = ConsumerGroup("analytics-v2", topic)
    c_a = group2.join("consumer-A")
    c_b = group2.join("consumer-B")
    group2.print_assignments()

    print("\n  consumer-A reads its partitions:")
    for entry in c_a.consume_all():
        print(f"    [P? key={entry.key}] {entry.value}")

    print("\n  consumer-B reads its partitions:")
    for entry in c_b.consume_all():
        print(f"    [P? key={entry.key}] {entry.value}")

    # ---------------------------------------------------------------
    print("\n\n--- Scenario 3: consumer-B leaves → rebalance back ---")
    group2.leave("consumer-B")
    group2.print_assignments()
    print("  consumer-A now owns all partitions again")

    print("\n✓ Same messages, different consumers — no duplicates within a group.")
    print("  → Next: Phase 6 adds ACK/NACK so failures are handled safely.")