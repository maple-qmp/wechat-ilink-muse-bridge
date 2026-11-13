# The Muse hook variant: long polling inside Muse's hook mechanism

[中文](README.md) | [English](README.en.md)

`daemon.py` is a resident long-polling loop for ordinary environments. But
the Muse runtime already has agent scheduling built in — there is no need
to run another process just to call an LLM. This directory holds the other
wiring, the one actually used in production:

## Layout

```
WeChat user ──message──> iLink server
                              │  getupdates (long poll, server holds ≤35s)
                              ▼
                      ilink-watch.sh        ← Muse hook script, runs every 5s;
                              │               each run is one long poll
                     new msgs │  persist first, then wake
                              ▼
                ilink-pending.json          ← pending queue (atomic writes,
                              │               survives power loss)
                              │  worker: ilink.py claim → ilink-processing.json
                              ▼
                      Muse worker agent     ← pre-send queue re-check, merges
                                              bursts, sends exactly one reply;
                                              batches unprocessed after 10 min
                                              are returned to pending by the
                                              script and woken again
```

## Why the indirection

- **No lost messages**: the hook script writes messages to disk *before*
  waking the agent. If the agent crashes or times out, the messages are
  still in the queue and the next script run wakes it again.
- **No duplicate replies**: claiming goes through `ilink.py claim`
  under a queue lock, and it refuses while a processing file exists
  (a bare `mv` would silently replace another worker's batch) — of
  several overlapping wakes, exactly one gets the queue; the rest exit
  quietly. While a worker holds a fresh claim, the script does not wake
  another one for newer messages: the holder's pre-send
  `ilink.py claim --merge <claim_id>` folds them into the same reply,
  and `ilink.py release <claim_id>` drops the claim afterwards (refusing
  if the claim was recycled to someone else).
- **Burst merging**: when a user fires off several messages, iLink splits
  them into multiple responses. The worker does not wait a fixed interval;
  it does a **pre-send re-check** — after composing a reply, right before
  sending, it looks at the queue once more. Anything new is merged in and
  the reply is re-composed against the newest context, so exactly one
  reply goes out per burst.
- **Instant "typing…"**: the moment a batch lands on disk, the script
  verifies the sender is the owner and lights the typing indicator right
  away, instead of waiting for the worker's cold start; the worker's own
  typing calls are just keep-alive refreshes.
- **Warm wait (no cold start mid-conversation)**: after replying, the
  worker runs `ilink-warm-wait.sh` (default 180s idle window, 30-minute
  session cap) instead of exiting. The waiter heartbeats a warm marker
  (`ilink-warm.json`); while it is fresh the script lands new batches
  without waking a second worker, and the warm worker claims them
  within seconds and replies without re-reading context. A stale
  heartbeat (dead worker) re-arms normal waking automatically.
- **Token expiry nags once**: when the script sees `TOKEN_EXPIRED` it
  creates a `rescan_notified` marker and wakes the agent to ask the user
  to re-scan; while the marker exists, it never nags again.
- **Closed-loop alerting** (an `alert-state.json` state machine): an
  expiry left unfixed is re-reported hourly; poll failures alert only
  after 10 continuous minutes (so network jitter never cries wolf),
  then hourly; when an alerted problem recovers, the agent wakes once
  to say it's back (un-alerted blips clear silently); when a worker's
  claim times out and is recycled, the pending wake carries a
  `stuck_alert` field. Recovery (re-scan / network back) clears the
  expiry marker automatically — the old never-clear behavior would
  have suppressed the *next* expiry notice; fixed.
- **Delivery semantics: at-least-once.** Polling is two-phase:
  `ilink.py poll` only fetches — it neither advances the cursor nor
  records dedupe ids. The script durably writes the batch to the
  pending queue first, then calls `ilink.py commit`. A crash anywhere
  means the batch is simply re-fetched next round: a reply may
  duplicate, but a message is never silently lost. Supporting rules:
  stale-claim recycling merges the stuck batch ahead of any newer
  pending batch (it never overwrites either one), one poll round's
  media downloads share a budget (`ILINK_POLL_MEDIA_BUDGET`, default
  60s) so a slow CDN delivers the item with `media_error` instead of
  pushing the round past the poll timeout into an endless re-fetch, and empty/invalid poll
  output is treated as a poll failure (a hard crash enters the alert
  state machine instead of masquerading as "no new messages").

Paths in the script are parameterized (`ILINK_DATA_DIR`, `ILINK_HOOK_STATE_DIR`,
`ILINK_CLIENT`; the legacy state-dir name `ILINK_HOOK_STATE` still works, and
the default matches `ilink.py health`). `$HATCH_HOOK_RUNTIME` is provided by the Muse hook runtime;
outside Muse, use `daemon.py` in the repo root instead.
