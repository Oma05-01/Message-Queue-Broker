"""
Phase 6 — Acknowledgements & Reliability
=========================================
What happens when a consumer reads a message but then crashes before
finishing the work? Without ACKs, that message is lost forever.

This phase adds:
  - ACK  (acknowledge): "I processed this successfully, move on"
  - NACK (negative ack): "I failed, put it back for retry"
  - Timeout retry: if no ACK arrives within N seconds, redeliver

Delivery semantics:
-------------------
  At-most-once:   commit offset BEFORE processing
                  → fast, but messages lost on crash
  At-least-once:  commit offset AFTER ACK
                  → safe, but duplicates possible on crash
  Exactly-once:   at-least-once + idempotency key on consumer side
                  → safest, most complex

This phase implements at-least-once.

Run:
    python phase6_acks.py
"""

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class PendingMessage:
    """A message that has been delivered but not yet acknowledged."""
    msg_id: str
    value: Any
    key: Optional[str]
    delivered_at: float          # time.monotonic() timestamp
    attempt: int = 1             # how many times delivered so far


class ReliableQueue:
    """
    A queue with acknowledgement-based delivery.

    Internal state:
        _ready    — messages waiting to be delivered
        _pending  — messages delivered, awaiting ACK
        _dlq      — dead letter queue: messages that exceeded max_attempts

    Flow:
        publish()  → lands in _ready
        consume()  → moves from _ready to _pending
        ack()      → removes from _pending (done)
        nack()     → moves from _pending back to _ready (retry)
        check_timeouts() → moves expired _pending back to _ready
    """

    def __init__(
        self,
        ack_timeout: float = 5.0,   # seconds before auto-redeliver
        max_attempts: int = 3,       # max deliveries before dead-letter
    ) -> None:
        self.ack_timeout = ack_timeout
        self.max_attempts = max_attempts
        self._ready:   dict[str, PendingMessage] = {}
        self._pending: dict[str, PendingMessage] = {}
        self._dlq:     dict[str, PendingMessage] = {}

    # ------------------------------------------------------------------
    # Producer side
    # ------------------------------------------------------------------

    def publish(self, value: Any, key: Optional[str] = None) -> str:
        """
        Add a message to the ready queue.
        Returns the message ID so the producer can track it if needed.
        """
        msg_id = str(uuid.uuid4())[:8]   # short ID for readable output
        self._ready[msg_id] = PendingMessage(
            msg_id=msg_id,
            value=value,
            key=key,
            delivered_at=0.0,
        )
        print(f"  [publish]   id={msg_id}  '{value}'  "
              f"(ready: {len(self._ready)})")
        return msg_id

    # ------------------------------------------------------------------
    # Consumer side
    # ------------------------------------------------------------------

    def consume(self) -> Optional[PendingMessage]:
        """
        Deliver the next message to the consumer.

        The message moves from _ready → _pending.
        It stays in _pending until the consumer calls ack() or nack().
        If neither happens within ack_timeout seconds, check_timeouts()
        moves it back to _ready automatically.
        """
        if not self._ready:
            return None

        msg_id, msg = next(iter(self._ready.items()))
        self._ready.pop(msg_id)

        msg.delivered_at = time.monotonic()
        self._pending[msg_id] = msg

        print(f"  [consume]   id={msg_id}  '{msg.value}'  "
              f"attempt={msg.attempt}  "
              f"(pending: {len(self._pending)})")
        return msg

    def ack(self, msg_id: str) -> None:
        """
        Confirm successful processing.
        Removes the message permanently — it will never be redelivered.
        """
        if msg_id not in self._pending:
            print(f"  [ack]       id={msg_id}  WARNING: not in pending")
            return
        msg = self._pending.pop(msg_id)
        print(f"  [ack]  ✓    id={msg_id}  '{msg.value}'  done")

    def nack(self, msg_id: str, reason: str = "") -> None:
        """
        Signal processing failure — requeue for retry.
        If max_attempts exceeded, moves to dead letter queue instead.
        """
        if msg_id not in self._pending:
            print(f"  [nack]      id={msg_id}  WARNING: not in pending")
            return

        msg = self._pending.pop(msg_id)
        msg.attempt += 1

        if msg.attempt > self.max_attempts:
            self._dlq[msg_id] = msg
            print(f"  [nack]  ✗   id={msg_id}  '{msg.value}'  "
                  f"→ dead letter queue (exceeded {self.max_attempts} attempts)")
        else:
            self._ready[msg_id] = msg
            print(f"  [nack]  ↺   id={msg_id}  '{msg.value}'  "
                  f"requeued (attempt {msg.attempt}/{self.max_attempts})")
        if reason:
            print(f"            reason: {reason}")

    def check_timeouts(self) -> int:
        """
        Scan pending messages for expired deliveries.
        Any message pending longer than ack_timeout is redelivered.

        Call this periodically in a real system (e.g. every second).
        Returns how many messages were requeued.
        """
        now = time.monotonic()
        timed_out = [
            msg_id
            for msg_id, msg in self._pending.items()
            if now - msg.delivered_at > self.ack_timeout
        ]
        for msg_id in timed_out:
            msg = self._pending.pop(msg_id)
            msg.attempt += 1
            if msg.attempt > self.max_attempts:
                self._dlq[msg_id] = msg
                print(f"  [timeout]  id={msg_id}  → dead letter queue")
            else:
                self._ready[msg_id] = msg
                print(f"  [timeout]  id={msg_id}  '{msg.value}'  "
                      f"requeued after {self.ack_timeout}s "
                      f"(attempt {msg.attempt}/{self.max_attempts})")
        return len(timed_out)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def status(self) -> None:
        print(f"\n  Queue status:")
        print(f"    ready:       {len(self._ready)}")
        print(f"    pending ACK: {len(self._pending)}")
        print(f"    dead letter: {len(self._dlq)}")
        if self._dlq:
            for msg in self._dlq.values():
                print(f"      ✗ id={msg.msg_id}  '{msg.value}'  "
                      f"failed {msg.attempt} times")


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 55)
    print("Phase 6 — Acknowledgements & Reliability Demo")
    print("=" * 55)

    q = ReliableQueue(ack_timeout=2.0, max_attempts=3)

    # ---------------------------------------------------------------
    print("\n--- Scenario 1: normal happy path ---")
    q.publish("payment.process", key="order-1")
    q.publish("email.send",      key="order-2")

    msg1 = q.consume()
    msg2 = q.consume()
    q.ack(msg1.msg_id)   # processed successfully
    q.ack(msg2.msg_id)
    q.status()

    # ---------------------------------------------------------------
    print("\n--- Scenario 2: consumer fails → NACK → retry ---")
    q.publish("invoice.generate", key="order-3")
    msg = q.consume()

    print("  (consumer fails to process the message)")
    q.nack(msg.msg_id, reason="database connection timeout")

    print("  (consumer retries)")
    msg = q.consume()
    q.ack(msg.msg_id)
    q.status()

    # ---------------------------------------------------------------
    print("\n--- Scenario 3: repeated failures → dead letter queue ---")
    q.publish("report.export", key="order-4")

    for attempt in range(3):
        msg = q.consume()
        if msg is None:
            break
        print(f"  (consumer fails attempt {attempt + 1})")
        q.nack(msg.msg_id, reason="downstream service unavailable")

    q.status()

    # ---------------------------------------------------------------
    print("\n--- Scenario 4: timeout → automatic redeliver ---")
    q.publish("webhook.deliver", key="order-5")
    msg = q.consume()
    print(f"  (consumer hangs — waiting {q.ack_timeout + 0.1:.1f}s for timeout...)")
    time.sleep(q.ack_timeout + 0.1)
    requeued = q.check_timeouts()
    print(f"  {requeued} message(s) requeued after timeout")

    msg = q.consume()
    if msg:
        q.ack(msg.msg_id)
        print("  (successfully processed on retry)")
    q.status()

    print("\n✓ At-least-once delivery: messages retried until ACK received.")
    print("  Tradeoff: on crash between consume() and ack(), message is redelivered.")
    print("  → Next: Phase 7 wraps everything in a CLI you can interact with.")