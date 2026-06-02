---
name: feedback-watcher-signal-design
description: koprs watcher sends () signal intentionally — do not suggest adding resource payload to the channel
metadata:
  type: feedback
---

The watcher in `koprs::watcher` sends `()` on the mpsc channel rather than the resource payload. This is intentional — it acts as a pure reconcile trigger, forcing the operator to always fetch fresh state rather than acting on potentially stale event data.

**Why:** Correctness tradeoff — event data can be stale or partial; always fetching current state is safer for operator reconcile loops.

**How to apply:** Do not suggest "the watcher loses the payload" as a weakness or recommend adding `T` to the channel type. The generic signal design is a deliberate choice.
