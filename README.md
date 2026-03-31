## Message-Queue-Broker
A from-scratch implementation of a Kafka-like distributed message broker to understand append-only logs, consumer offsets, partitioning, and delivery reliability tradeoffs, not a production replacement for Kafka or RabbitMQ.

## Why This Project Exists
Modern systems rely heavily on message brokers and event-streaming platforms to decouple services, process data asynchronously, and handle high-throughput event streams. Systems like Kafka and RabbitMQ sit at the heart of distributed architectures.

This project was built to understand the internal mechanics behind event brokers, particularly:
- How append-only disk I/O enables high-speed persistent storage
- How independent consumer offsets allow decoupled reading and message replay
- How partitioning solves single-log bottlenecks to enable parallel throughput
- How consumer groups cooperate and automatically rebalance workloads
- How reliability mechanisms (ACKs, NACKs, and Dead Letter Queues) enforce delivery guarantees.

The goal was to close the gap between using message brokers via client libraries and understanding the distributed system primitives that power them.

## Scope & Constraints
**Included**
- Append-only persistent log storage to disk
- Monotonically increasing message offsets
- Independent consumer offset tracking (enabling log replays)
- Key-based partitioning with deterministic hashing for strict ordering
- Consumer groups with automatic round-robin partition rebalancing
- At-least-once delivery semantics via ACK/NACK
- Timeout-based message redelivery and Dead Letter Queues (DLQ)
- Interactive CLI interface for managing topics, publishing, and consuming

**Explicitly Excluded**
- These features were intentionally not implemented to keep the project focused:
- Distributed multi-node clustering and broker replication
- Leader election and consensus protocols (e.g., ZooKeeper or KRaft)
- Custom network wire protocols (e.g., Kafka's TCP protocol)
- Advanced consumer rebalance algorithms (e.g., sticky assignors)
- Log compaction and time-based retention policiesExactly-once delivery semantics

These omissions simplify the system while still demonstrating core distributed messaging concepts.

## Architecture Overview
The broker is composed of several core components
```
           CLI / API Interface
                     │
                     ▼
                Broker Engine
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
 Partitioned     Consumer       Reliable
   Topics         Groups         Queue
(Log Files)    (Rebalancing)   (ACK/NACK)
       │             │             │
       ▼             ▼             ▼
  Append-Only   Offset Store   Dead Letter
 Storage (Disk)    (JSON)      Queue (DLQ)
 ```
 
## Core Components
**Broker Engine**
- Central coordinator holding all runtime state.
- Manages topic creation, routing publications, and servicing consumer requests.

**Partitioned Topic**
- Splits a single logical topic into $N$ independent log files (partitions).
- Hashes message keys to guarantee that events for the same entity always land in the same partition in strict order.

**Persistent Log**
- An append-only file structure where messages are permanently stored.
- Assigns a monotonically increasing offset to every incoming message.

**Consumer Group**
- Manages a set of cooperating consumers reading from a partitioned topic.
- Handles partition assignment, ensuring no two consumers read the same partition (preventing duplicates).

**Offset Store**
- Tracks the furthest read position of each consumer.
- Persisted to JSON, allowing consumers to survive crashes and resume exactly where they left off.

**Reliable Queue**
- Wraps consumption with state tracking (_ready, _pending, _dlq).
- Enforces at-least-once delivery by requiring explicit ACKs, automatically retrying timed-out messages, and routing "poison pills" to a dead letter queue.

## Key Design Decisions
**1 — Append-Only Logs for Storage**
The underlying storage mechanism relies strictly on appending JSON lines to text files.

**Tradeoff:**
- Extremely fast sequential disk writes with no locking contention.
- Requires external processes (like log compaction/retention, omitted here) to prevent the disk from eventually filling up.


**2 — Client-Side Offset Tracking**
The logs themselves do not track what has been read; consumers update an external OffsetStore.

**Tradeoff:**
- Unlocks the ability to have dozens of independent consumers (or groups) reading the same log without modifying it, and enables trivial message replay via seek_to().
- Shifts the burden of managing and persisting state onto the consumer/group coordinator.


**3 — Deterministic Hashing for Partitioning**
Partition routing uses a custom polynomial hash: hash(key) % N.

**Tradeoff:**
- Guarantees strict ordering for related events (e.g., all events for user-1 process in order).
- Can result in "hot partitions" if a single key accounts for a disproportionate amount of traffic.


**4 — Round-Robin Rebalancing**
When a consumer joins or leaves, partitions are reassigned strictly via index modulo math.

**Tradeoff:**
- Very simple to implement and guarantees even distribution.
- Can cause unnecessary partition shuffling (a partition might move from Consumer A to Consumer B unnecessarily), which real brokers solve with "sticky" assignors.


**5 — At-Least-Once Delivery Semantics**
The broker registers a message as pending until explicitly acknowledged.

**Tradeoff:**
- High safety: crashes between processing and ACKing result in redelivery, preventing data loss.
- Consumers are responsible for idempotency, as they will occasionally process the exact same message twice during failure scenarios.


## Performance Characteristics
This broker prioritizes educational clarity and predictable behavior over raw I/O performance.

**Write Performance**
Publishing operations are O(1).

**This is achieved through:**
- sequential file appends directly to disk
- simple constant-time key hashing to find the correct partition
- entirely lock-free writes against the underlying logs

**Read Performance**
Consumption operations are generally O(1) sequential reads.
However, point-in-time replays (seeking to a random offset) are O(n) because the system must scan the text file to find the correct line. Real systems solve this with binary index files mapping offsets to byte positions.

**Concurrency Limits**
Parallelism is bound by the number of partitions.
- $N$ partitions allow exactly $N$ concurrent consumers in a single group.
- Adding consumers beyond $N$ results in idle consumers.

**Reliability Overhead**
The ACK/NACK state machine introduces slight overhead:
tracking _pending messages in memory
a periodic $O(P)$ scan (where $P$ is pending messages) to detect and redeliver timeouts

## Failure Modes & Limitations

**Unbounded Disk Growth**
Because the system never deletes old messages (no retention policy), high-volume publishing will eventually exhaust local disk space.

**Hot Partitions**
If a high volume of messages shares the same key, one partition log will grow massively larger than the others, bottlenecking consumer throughput and defeating parallelism.

**Duplicate Processing (At-Least-Once Caveat)**
If a consumer processes a message successfully but crashes before sending the ACK, the timeout mechanism will redeliver the message to another consumer.

**Poison Pill Blocking**
A message that repeatedly crashes the consumer (e.g., malformed payload) will be consumed and NACKed multiple times. It blocks standard processing for that thread until max_attempts is reached and it is sent to the dead letter queue.

## System Experiments
Several experiments were run to observe distributed broker behavior.

## Partition Distribution & Ordering
**Test:**
Published a sequence of varied events, but forced several specific events to share the key user-1.

**Observation:**
Regardless of overall volume, every event tagged with user-1 was routed by the hash function to the exact same partition (e.g., Partition 0).

**Result:**
Strict event ordering per key is successfully guaranteed.

## Consumer Group Rebalancing
**Test:**
Initialized a topic with 3 partitions and a group with 1 consumer. Then, spun up a 2nd consumer in the same group.

**Observation:**
The group dynamically revoked the initial assignment and split the load (e.g., Consumer A took P0 and P2; Consumer B took P1).

**Insight:**
Workloads can scale horizontally without manual partition assignment or risk of duplicate reads.

## Unacknowledged Message Crash
**Test:**
A consumer pulled a message from the queue, simulating a crash by halting execution without calling ack().

**Observation:**After the 2.0-second timeout, the broker scanned the _pending state, moved the message back to _ready, and incremented its attempt counter.
**Impact:**
No data loss occurred, but the system demonstrated why idempotency on the consumer side is mandatory for distributed reliability.

## What I Learned
Building this broker revealed several non-obvious insights about distributed streaming systems:

**Storage relies on simplicity**
At their core, massively scalable brokers like Kafka are essentially just highly optimized sequential file writers. The append-only design bypasses the complexities of B-trees and random disk I/O.

**Offsets are the secret to multi-tenancy**
By decoupling the read state from the data itself and pushing offset tracking to the clients, you can have thousands of downstream systems analyzing the same raw data at their own pace without impacting the broker.

**Reliability forces tradeoffs**
You cannot have a perfect system. You must choose between speed and potential data loss (at-most-once) or safety and potential duplicate data (at-least-once).

## How Production Systems Do This Differently
Systems like Apache Kafka and RabbitMQ introduce immense complexity to operate safely at enterprise scale.

**Production brokers typically include:**
- Zero-copy data transfer (using sendfile to stream disk bytes directly to network sockets)
- Binary wire protocols instead of string-heavy JSON to minimize serialization cost
- Index files that map logical message offsets to physical byte locations on disk for $O(1)$ lookups
- Distributed consensus algorithms (KRaft/ZooKeeper) to replicate partitions across multiple physical machines
- Log compaction algorithms that squish logs down to only the latest value for a given key
- Memory-mapped files (mmap) to let the OS handle page caching automatically

**How to Run**
```Bash```
```python phase7_cli.py --interactive```

**Example usage:**
```Plaintext
❯ CREATE_TOPIC events 3
OK topic 'events' created (3 partitions)

❯ PUBLISH events user-42 login
OK partition=0 offset=0

❯ PUBLISH events user-42 checkout
OK partition=0 offset=1

❯ CONSUME events consumer-A
OK  value='login'  key='user-42'  partition=0  offset=0

❯ STATUS
  topics:    1
  consumers: 1
  [events]  3 partitions  2 messages

❯ LAG events consumer-A
  partition=0  consumed=1  total=2  lag=1
  partition=1  consumed=0  total=0  lag=0
  partition=2  consumed=0  total=0  lag=0
  total lag: 1

❯ EXIT
Goodbye.
