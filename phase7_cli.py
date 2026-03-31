"""
Phase 7 — CLI / API
====================
Everything from Phases 1–6 wired together behind a text interface.
This is your broker as a running service — you interact with it by
typing commands, just like early message brokers worked.

Commands:
    CREATE_TOPIC <name> [partitions]
    LIST_TOPICS
    PUBLISH <topic> <key> <message>
    CONSUME <topic> <consumer_id> [group_id]
    SEEK <topic> <consumer_id> <offset> <partition>
    LAG <topic> <consumer_id>
    STATUS
    HELP
    EXIT

Run:
    python phase7_cli.py
"""

import os
import sys
from typing import Optional

from phase3_offsets import OffsetStore, Consumer
from phase4_partitions import PartitionedTopic


# ---------------------------------------------------------------------------
# Broker — holds all runtime state
# ---------------------------------------------------------------------------

class Broker:
    """
    The central object that owns all topics, consumers, and offset stores.
    The CLI is just a thin layer on top of this.

    In a real broker (Kafka), this would be distributed across multiple
    machines. Here it's all in one Python process in memory + local files.
    """

    def __init__(self) -> None:
        self.topics:    dict[str, PartitionedTopic] = {}
        self.stores:    dict[str, OffsetStore] = {}      # one per topic
        self.consumers: dict[str, Consumer] = {}         # id → Consumer

    # ------------------------------------------------------------------
    # Topic management
    # ------------------------------------------------------------------

    def create_topic(self, name: str, num_partitions: int = 3) -> str:
        if name in self.topics:
            return f"ERROR topic '{name}' already exists"
        self.topics[name] = PartitionedTopic(name, num_partitions)
        self.stores[name] = OffsetStore(f"cli_offsets_{name}.json")
        return f"OK topic '{name}' created ({num_partitions} partitions)"

    def list_topics(self) -> str:
        if not self.topics:
            return "  (no topics)"
        lines = []
        for name, topic in self.topics.items():
            total = topic.total_messages()
            lines.append(
                f"  {name}  "
                f"partitions={topic.num_partitions}  "
                f"messages={total}"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Producing
    # ------------------------------------------------------------------

    def publish(self, topic_name: str, key: str, value: str) -> str:
        topic = self.topics.get(topic_name)
        if not topic:
            return f"ERROR topic '{topic_name}' not found"
        partition, offset = topic.publish(value, key=key)
        return f"OK partition={partition} offset={offset}"

    # ------------------------------------------------------------------
    # Consuming
    # ------------------------------------------------------------------

    def _get_or_create_consumer(
        self, topic_name: str, consumer_id: str
    ) -> Optional[Consumer]:
        """
        Look up an existing consumer or create a new one.
        Consumers are keyed by 'topic:consumer_id' so the same consumer
        ID can be used across different topics independently.
        """
        topic = self.topics.get(topic_name)
        if not topic:
            return None

        full_id = f"{topic_name}:{consumer_id}"
        if full_id not in self.consumers:
            # Consumers in Phase 3 read a single-partition log.
            # Here we wrap the first partition for simplicity —
            # a full implementation would read across all partitions.
            self.consumers[full_id] = Consumer(
                consumer_id=full_id,
                log=topic.partitions[0],
                offset_store=self.stores[topic_name],
            )
        return self.consumers[full_id]

    def consume(self, topic_name: str, consumer_id: str) -> str:
        """
        Consume the next message for this consumer.
        Reads across all partitions, returning the first available message.
        """
        topic = self.topics.get(topic_name)
        if not topic:
            return f"ERROR topic '{topic_name}' not found"

        # Try each partition in order
        store = self.stores[topic_name]
        for p_idx, partition_log in enumerate(topic.partitions):
            store_key = f"{consumer_id}:p{p_idx}"
            offset = store.get(store_key)
            entry = partition_log.read(offset)
            if entry is not None:
                store.set(store_key, offset + 1)
                return (
                    f"OK  value='{entry.value}'  "
                    f"key='{entry.key}'  "
                    f"partition={p_idx}  "
                    f"offset={entry.offset}"
                )
        return "EMPTY no messages available"

    def seek(
        self,
        topic_name: str,
        consumer_id: str,
        offset: int,
        partition: int,
    ) -> str:
        topic = self.topics.get(topic_name)
        if not topic:
            return f"ERROR topic '{topic_name}' not found"
        store = self.stores[topic_name]
        store_key = f"{consumer_id}:p{partition}"
        store.set(store_key, offset)
        return f"OK {consumer_id} seeked to offset {offset} on partition {partition}"

    def lag(self, topic_name: str, consumer_id: str) -> str:
        topic = self.topics.get(topic_name)
        if not topic:
            return f"ERROR topic '{topic_name}' not found"
        store = self.stores[topic_name]
        lines = []
        total_lag = 0
        for p_idx, partition_log in enumerate(topic.partitions):
            store_key = f"{consumer_id}:p{p_idx}"
            consumed = store.get(store_key)
            partition_lag = max(0, partition_log.size() - consumed)
            total_lag += partition_lag
            lines.append(
                f"  partition={p_idx}  "
                f"consumed={consumed}  "
                f"total={partition_log.size()}  "
                f"lag={partition_lag}"
            )
        lines.append(f"  total lag: {total_lag}")
        return "\n".join(lines)

    def status(self) -> str:
        lines = [
            f"  topics:    {len(self.topics)}",
            f"  consumers: {len(self.consumers)}",
        ]
        for name, topic in self.topics.items():
            lines.append(
                f"  [{name}]  "
                f"{topic.num_partitions} partitions  "
                f"{topic.total_messages()} messages"
            )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI — parses text commands and calls the broker
# ---------------------------------------------------------------------------

HELP_TEXT = """
Commands:
  CREATE_TOPIC <name> [partitions]     Create a new topic (default: 3 partitions)
  LIST_TOPICS                          List all topics and message counts
  PUBLISH <topic> <key> <message>      Publish a message to a topic
  CONSUME <topic> <consumer_id>        Consume the next message
  SEEK <topic> <consumer> <offset> <partition>  Move consumer offset
  LAG <topic> <consumer_id>            Show how far behind a consumer is
  STATUS                               Show broker overview
  HELP                                 Show this message
  EXIT                                 Quit
"""


def run_command(broker: Broker, line: str) -> Optional[str]:
    """
    Parse one command line and call the appropriate broker method.
    Returns the output string, or None if the command was EXIT.
    """
    parts = line.strip().split()
    if not parts:
        return ""

    cmd = parts[0].upper()

    if cmd == "EXIT":
        return None  # signal to stop the loop

    elif cmd == "HELP":
        return HELP_TEXT

    elif cmd == "CREATE_TOPIC":
        if len(parts) < 2:
            return "ERROR usage: CREATE_TOPIC <name> [partitions]"
        name = parts[1]
        partitions = int(parts[2]) if len(parts) > 2 else 3
        return broker.create_topic(name, partitions)

    elif cmd == "LIST_TOPICS":
        return broker.list_topics()

    elif cmd == "PUBLISH":
        if len(parts) < 4:
            return "ERROR usage: PUBLISH <topic> <key> <message>"
        topic, key = parts[1], parts[2]
        message = " ".join(parts[3:])   # allow spaces in message
        return broker.publish(topic, key, message)

    elif cmd == "CONSUME":
        if len(parts) < 3:
            return "ERROR usage: CONSUME <topic> <consumer_id>"
        return broker.consume(parts[1], parts[2])

    elif cmd == "SEEK":
        if len(parts) < 5:
            return "ERROR usage: SEEK <topic> <consumer> <offset> <partition>"
        return broker.seek(parts[1], parts[2], int(parts[3]), int(parts[4]))

    elif cmd == "LAG":
        if len(parts) < 3:
            return "ERROR usage: LAG <topic> <consumer_id>"
        return broker.lag(parts[1], parts[2])

    elif cmd == "STATUS":
        return broker.status()

    else:
        return f"ERROR unknown command '{cmd}'. Type HELP."


def repl(broker: Broker) -> None:
    """
    REPL = Read, Evaluate, Print, Loop.
    The standard pattern for interactive command-line tools.
    """
    print("\nMessage Broker CLI  (type HELP for commands, EXIT to quit)")
    print("─" * 55)

    while True:
        try:
            line = input("\n❯ ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not line:
            continue

        result = run_command(broker, line)

        if result is None:   # EXIT command
            print("Goodbye.")
            break

        print(result)


# ---------------------------------------------------------------------------
# Demo — runs a scripted session so you can see output without typing
# ---------------------------------------------------------------------------

def demo(broker: Broker) -> None:
    """Run a pre-scripted set of commands to show all features."""
    commands = [
        "CREATE_TOPIC orders",
        "CREATE_TOPIC notifications 2",
        "LIST_TOPICS",
        "PUBLISH orders user-1 order.placed",
        "PUBLISH orders user-2 order.placed",
        "PUBLISH orders user-1 order.shipped",
        "PUBLISH notifications user-1 email.welcome",
        "STATUS",
        "CONSUME orders consumer-A",
        "CONSUME orders consumer-A",
        "CONSUME orders consumer-A",
        "LAG orders consumer-A",
        "SEEK orders consumer-A 0 0",
        "CONSUME orders consumer-A",
        "CONSUME orders consumer-B",
        "LIST_TOPICS",
    ]

    print("\nMessage Broker CLI — Demo Mode")
    print("─" * 55)
    for cmd in commands:
        print(f"\n❯ {cmd}")
        result = run_command(broker, cmd)
        if result:
            print(result)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Clean up leftover files from previous runs
    for f in os.listdir("."):
        if f.startswith("topic_") or f.startswith("cli_offsets_"):
            os.remove(f)

    broker = Broker()

    if "--interactive" in sys.argv:
        # Run the live REPL: python phase7_cli.py --interactive
        repl(broker)
    else:
        # Run the scripted demo by default
        demo(broker)
        print("\n" + "─" * 55)
        print("Run with --interactive to use the live CLI:")
        print("  python phase7_cli.py --interactive")