#!/usr/bin/env python3
"""Integration tests for the example Muse hook driver script.

Runs examples/muse-hook/ilink-watch.sh in a sandbox against a fake
ilink.py, pinning the two-phase poll contract end to end:

  * a batch is queued BEFORE commit is called, exactly once
  * an empty batch still commits (the cursor must advance even when
    there is nothing to wake the worker for)
  * a failing commit leaves the queue intact (never lost, never
    overwritten — the batch will simply be re-fetched next round)
  * stale-claim recycling never overwrites a non-empty pending queue
  * a hard crash (empty poll output) is a poll failure, not silence
  * a corrupt pending queue is quarantined (PENDING_CORRUPT), never
    silently treated as empty; a valid empty one stays quiet
  * a second instance cannot run while the state-dir lock is held
  * a failed commit persists in the commit ledger and is retried at
    the START of the next round, before any new poll — the batch is
    queued exactly once and the cursor ends up advanced

The script is copied into the sandbox with ONE test-only substitution:
`-cmin +10` becomes `-mmin +10` so `touch -d` can age queue files
(ctime cannot be backdated). The logic under test is unchanged.

    python3 -m unittest discover -s tests -v
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
SCRIPT_SRC = os.path.join(ROOT, "examples", "muse-hook", "ilink-watch.sh")

RUNTIME_STUB = """# Test stub for the Muse hook runtime library (the script sources it).
log() { echo "LOG $*" >> "$TRACE_CALLS"; }
silent() { echo "SILENT $*" >> "$TRACE_CALLS"; }
wake() { echo "WAKE $1 $2" >> "$TRACE_CALLS"; }
"""

FAKE_ILINK = """#!/usr/bin/env python3
import json, os, sys
trace = os.environ["TRACE_CALLS"]
HERE = os.path.dirname(os.path.abspath(__file__))
CURSOR_FILE = os.path.join(HERE, "cursor")
FAIL_ONCE_FILE = os.path.join(HERE, "fail_commit_once")
if len(sys.argv) > 1 and sys.argv[1] == "poll":
    cur = ""
    if os.path.exists(CURSOR_FILE):
        with open(CURSOR_FILE) as f:
            cur = f.read().strip()
    # Once the cursor has advanced to B2 the server has nothing new:
    # serve FAKE_OUT2 instead of redelivering the batch.
    out = os.environ.get("FAKE_OUT2", "") if cur == "B2" \\
        else os.environ.get("FAKE_OUT", "")
    if out:
        print(out)
elif len(sys.argv) > 1 and sys.argv[1] == "commit":
    data = sys.stdin.read()
    if os.environ.get("FAKE_COMMIT_FAIL") or os.path.exists(FAIL_ONCE_FILE):
        if os.path.exists(FAIL_ONCE_FILE):
            os.remove(FAIL_ONCE_FILE)
        with open(trace, "a") as f:
            f.write("COMMIT_FAIL " + data.strip() + "\\n")
        sys.exit(1)
    pend = os.path.join(os.environ.get("ILINK_HOOK_STATE_DIR")
                        or os.environ["ILINK_HOOK_STATE"], "ilink-pending.json")
    order = "ORDER_OK" if os.path.exists(pend) else "ORDER_NA"
    with open(trace, "a") as f:
        f.write("COMMIT " + data.strip() + " " + order + "\\n")
    try:
        payload = json.loads(data)
    except Exception:
        payload = {}
    if payload.get("cursor"):
        with open(CURSOR_FILE, "w") as f:
            f.write(payload["cursor"])
"""

BATCH = json.dumps({
    "msgs": [{"from_user_id": "owner1", "message_type": 1, "message_id": 7,
              "item_list": [{"type": 1, "text_item": {"text": "hi"}}]}],
    "commit": {"cursor": "B2", "ids": ["7"]},
})
EMPTY = json.dumps({"msgs": [], "commit": {"cursor": "B9", "ids": []}})


@unittest.skipUnless(shutil.which("jq") and shutil.which("bash"),
                     "needs bash + jq")
class HookIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="ilink-hook-it-")
        self.bridge = os.path.join(self.dir, "bridge")
        self.state = os.path.join(self.dir, "state")
        os.makedirs(self.bridge)
        os.makedirs(self.state)
        self.calls = os.path.join(self.dir, "calls.log")
        open(self.calls, "w").close()
        with open(os.path.join(self.bridge, "ilink.py"), "w") as f:
            f.write(FAKE_ILINK)
        with open(os.path.join(self.bridge, "owner.json"), "w") as f:
            json.dump({"owner_id": "owner1"}, f)
        self.runtime = os.path.join(self.dir, "runtime.sh")
        with open(self.runtime, "w") as f:
            f.write(RUNTIME_STUB)
        with open(SCRIPT_SRC) as f:
            script = f.read().replace("-cmin +10", "-mmin +10")
        self.script = os.path.join(self.dir, "watch.sh")
        with open(self.script, "w") as f:
            f.write(script)
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def run_hook(self, fake_out=None, dry=False, extra_env=None,
                 legacy_env=False):
        env = dict(os.environ)
        env.update({
            "ILINK_DATA_DIR": self.bridge,
            # Pin BOTH names to this test's dir: the suite shares one
            # process env, and an unpinned name would leak another
            # module's state dir into the script (canonical wins).
            "ILINK_HOOK_STATE_DIR": self.state,
            "ILINK_HOOK_STATE": self.state,
            "HATCH_HOOK_RUNTIME": self.runtime,
            "HATCH_HOOK_DRY_RUN": "1" if dry else "0",
            "TRACE_CALLS": self.calls,
        })
        if legacy_env:
            # Only the legacy variable set: the script must still find
            # the state dir through the alias.
            env.pop("ILINK_HOOK_STATE_DIR", None)
            env["ILINK_HOOK_STATE"] = self.state
        if fake_out is not None:
            env["FAKE_OUT"] = fake_out
        if extra_env:
            env.update(extra_env)
        r = subprocess.run(["bash", self.script], env=env,
                           capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, r.stderr
        with open(self.calls) as f:
            return f.read()

    def pending(self):
        p = os.path.join(self.state, "ilink-pending.json")
        if not os.path.exists(p):
            return None
        with open(p) as f:
            return json.load(f)

    def test_batch_is_queued_before_commit(self):
        calls = self.run_hook(BATCH)
        self.assertIn("new_wechat_messages", calls)
        self.assertEqual(self.pending(), {"msgs": json.loads(BATCH)["msgs"]})
        self.assertIn('COMMIT {"cursor":"B2","ids":["7"]} ORDER_OK', calls)

    def test_legacy_state_env_alias_still_works(self):
        # Deployments made against the old example used ILINK_HOOK_STATE;
        # with only that variable set the batch must land in the same dir.
        calls = self.run_hook(BATCH, legacy_env=True)
        self.assertIn("new_wechat_messages", calls)
        self.assertEqual(self.pending(), {"msgs": json.loads(BATCH)["msgs"]})

    def test_empty_batch_still_commits(self):
        calls = self.run_hook(EMPTY)
        self.assertNotIn("new_wechat_messages", calls)
        self.assertIsNone(self.pending())
        self.assertIn('COMMIT {"cursor":"B9","ids":[]}', calls)

    def test_failed_commit_keeps_queue(self):
        calls = self.run_hook(BATCH, extra_env={"FAKE_COMMIT_FAIL": "1"})
        self.assertIn("new_wechat_messages", calls)
        self.assertEqual(self.pending(), {"msgs": json.loads(BATCH)["msgs"]})
        with open(os.path.join(self.bridge, "poll.log")) as f:
            self.assertIn("COMMIT_FAILED", f.read())

    def test_recycling_never_overwrites_pending(self):
        proc = os.path.join(self.state, "ilink-processing.json")
        with open(proc, "w") as f:
            json.dump({"msgs": [{"message_id": 1}]}, f)
        subprocess.run(["touch", "-d", "15 minutes ago", proc], check=True)
        pend_path = os.path.join(self.state, "ilink-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": [{"message_id": 2}]}, f)
        calls = self.run_hook(EMPTY)
        # 2026-11-13: the stale batch is merged AHEAD of the newer one
        # (oldest first) instead of being deferred. Deferring left the
        # stale file where the next worker's claim would overwrite it.
        self.assertEqual(self.pending(),
                         {"msgs": [{"message_id": 1}, {"message_id": 2}]})
        self.assertFalse(os.path.exists(proc))
        self.assertIn("WAKE pending_wechat_messages", calls)

    def test_stale_recycle_drops_claim_id(self):
        proc = os.path.join(self.state, "ilink-processing.json")
        with open(proc, "w") as f:
            json.dump({"msgs": [{"message_id": 1}], "claim_id": "dead-1"}, f)
        subprocess.run(["touch", "-d", "15 minutes ago", proc], check=True)
        self.run_hook()
        self.assertEqual(self.pending(), {"msgs": [{"message_id": 1}]})
        self.assertFalse(os.path.exists(proc))

    def write_proc(self, age_minutes=0):
        proc = os.path.join(self.state, "ilink-processing.json")
        with open(proc, "w") as f:
            json.dump({"msgs": [{"from_user_id": "owner1",
                                 "message_id": 30}], "claim_id": "w-1"}, f)
        if age_minutes:
            subprocess.run(["touch", "-d", f"{age_minutes} minutes ago",
                            proc], check=True)
        return proc

    def test_fresh_claim_suppresses_pending_wake(self):
        # A worker holds a fresh claim: a newer pending batch is left to
        # its pre-send merge. Waking a second worker used to let that
        # worker's `mv` overwrite the first worker's processing file.
        proc = self.write_proc()
        with open(os.path.join(self.state, "ilink-pending.json"), "w") as f:
            json.dump({"msgs": [{"from_user_id": "owner1",
                                 "message_id": 31}]}, f)
        calls = self.run_hook()
        self.assertNotIn("WAKE", calls)
        self.assertIn("worker on duty (fresh claim)", calls)
        self.assertTrue(os.path.exists(proc))

    def test_fresh_claim_suppresses_new_batch_wake(self):
        self.write_proc()
        calls = self.run_hook(BATCH)
        self.assertEqual(self.pending(), {"msgs": json.loads(BATCH)["msgs"]})
        self.assertIn('COMMIT {"cursor":"B2","ids":["7"]} ORDER_OK', calls)
        self.assertNotIn("WAKE", calls)

    def test_stale_claim_does_not_suppress_wake(self):
        self.write_proc(age_minutes=15)
        with open(os.path.join(self.state, "ilink-pending.json"), "w") as f:
            json.dump({"msgs": [{"from_user_id": "owner1",
                                 "message_id": 31}]}, f)
        calls = self.run_hook()
        self.assertIn("WAKE pending_wechat_messages", calls)
        self.assertEqual([m["message_id"] for m in self.pending()["msgs"]],
                         [30, 31])

    def test_crash_output_is_a_poll_failure(self):
        self.run_hook("")
        with open(os.path.join(self.bridge, "poll.log")) as f:
            self.assertIn("POLL_CRASH", f.read())
        with open(os.path.join(self.bridge, "alert-state.json")) as f:
            self.assertIn("err_since", json.load(f))

    def write_warm(self, heartbeat_age=0, until_offset=1800):
        import time
        now = int(time.time())
        warm = {"since": now - 60, "activity": now - 10,
                "heartbeat": now - heartbeat_age,
                "until": now + until_offset}
        with open(os.path.join(self.state, "ilink-warm.json"), "w") as f:
            json.dump(warm, f)

    def test_warm_worker_suppresses_pending_wake(self):
        # A warm worker (fresh heartbeat) is already watching the queue:
        # the script must light typing but NOT wake a second worker.
        pend_path = os.path.join(self.state, "ilink-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": [{"from_user_id": "owner1", "message_id": 21}]}, f)
        self.write_warm()
        calls = self.run_hook()
        self.assertNotIn("WAKE", calls)
        self.assertIn("warm worker on duty", calls)

    def test_stale_warm_marker_rearms_wake(self):
        # Heartbeat older than 45s = the warm worker died; the script
        # must fall back to a normal wake (self-healing).
        pend_path = os.path.join(self.state, "ilink-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": [{"from_user_id": "owner1", "message_id": 22}]}, f)
        self.write_warm(heartbeat_age=120)
        calls = self.run_hook()
        self.assertIn("WAKE pending_wechat_messages", calls)

    def test_warm_worker_gets_new_batch_without_wake(self):
        # Fresh poll batch + warm worker: batch lands and commits as
        # usual, but no wake is emitted — the waiter claims it itself.
        self.write_warm()
        calls = self.run_hook(BATCH)
        self.assertEqual(self.pending(), {"msgs": json.loads(BATCH)["msgs"]})
        self.assertIn('COMMIT {"cursor":"B2","ids":["7"]} ORDER_OK', calls)
        self.assertNotIn("WAKE", calls)

    def test_corrupt_pending_is_quarantined_not_treated_as_empty(self):
        pend_path = os.path.join(self.state, "ilink-pending.json")
        with open(pend_path, "w") as f:
            f.write("{corrupt queue")
        calls = self.run_hook(EMPTY)
        # The corrupt file is preserved under a quarantine name (never
        # overwritten), the marker lands in both logs, and the round
        # carries on polling normally afterwards.
        self.assertFalse(os.path.exists(pend_path))
        quarantined = [n for n in os.listdir(self.state)
                       if n.startswith("ilink-pending.json.corrupt-")]
        self.assertEqual(len(quarantined), 1)
        self.assertIn("PENDING_CORRUPT", calls)
        with open(os.path.join(self.bridge, "poll.log")) as f:
            self.assertIn("PENDING_CORRUPT", f.read())
        self.assertIn('COMMIT {"cursor":"B9","ids":[]}', calls)

    def test_structurally_invalid_pending_is_quarantined(self):
        # JSON-valid but wrong-shaped queues must quarantine like
        # corrupt ones. The old check (top-level object only) counted
        # {"msgs": "invalid"} as 0, and the pending branch then exits
        # silently without polling — a quiet total stall.
        pend_path = os.path.join(self.state, "ilink-pending.json")
        for content in ('{"msgs": "invalid"}', '{}', '{"msgs": 5}'):
            with open(pend_path, "w") as f:
                f.write(content)
            before = {n for n in os.listdir(self.state)
                      if n.startswith("ilink-pending.json.corrupt-")}
            calls = self.run_hook(EMPTY)
            self.assertFalse(os.path.exists(pend_path),
                             f"misshapen pending survived: {content}")
            after = {n for n in os.listdir(self.state)
                     if n.startswith("ilink-pending.json.corrupt-")}
            self.assertEqual(len(after - before), 1, content)
            self.assertIn("PENDING_CORRUPT", calls)
            with open(os.path.join(self.bridge, "poll.log")) as f:
                self.assertIn("PENDING_CORRUPT", f.read())
            # The round carries on polling normally afterwards.
            self.assertIn('COMMIT {"cursor":"B9","ids":[]}', calls)

    def test_valid_empty_pending_is_quiet(self):
        pend_path = os.path.join(self.state, "ilink-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": []}, f)
        calls = self.run_hook()
        self.assertIn("pending file empty", calls)
        self.assertNotIn("PENDING_CORRUPT", calls)
        self.assertTrue(os.path.exists(pend_path))  # left untouched

    def test_lock_excludes_second_instance(self):
        # Hold the state-dir lock the way a running instance does: a
        # second invocation must exit quietly with no wake, no poll
        # and no queue side effects at all.
        import fcntl
        lock_path = os.path.join(self.state, "ilink-watch.lock")
        with open(lock_path, "w") as lock_f:
            fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            calls = self.run_hook(BATCH)
        self.assertEqual(calls.strip(), "")
        self.assertIsNone(self.pending())

    def test_failed_commit_is_deferred_via_ledger_not_duplicated(self):
        # Round 1: the batch lands, commit fails -> the payload must
        # persist in the commit ledger and COMMIT_FAILED is logged.
        open(os.path.join(self.bridge, "fail_commit_once"), "w").close()
        ledger = os.path.join(self.state, "ilink-commit-pending.json")
        calls1 = self.run_hook(BATCH)
        self.assertIn("new_wechat_messages", calls1)
        self.assertIn("COMMIT_FAIL", calls1)
        self.assertEqual(self.pending(),
                         {"msgs": json.loads(BATCH)["msgs"]})
        with open(ledger) as f:
            self.assertEqual(json.load(f), {"cursor": "B2", "ids": ["7"]})
        with open(os.path.join(self.bridge, "poll.log")) as f:
            self.assertIn("COMMIT_FAILED", f.read())
        # The worker claims the batch (queue drained) before round 2.
        os.remove(os.path.join(self.state, "ilink-pending.json"))
        open(self.calls, "w").close()  # trace only round 2 from here
        # Round 2: the ledger is retried BEFORE polling and succeeds;
        # with the cursor now at B2 the poll serves nothing new, so the
        # batch is never queued a second time and the ledger is gone.
        calls2 = self.run_hook(BATCH, extra_env={"FAKE_OUT2": EMPTY})
        self.assertIn('COMMIT {"cursor":"B2","ids":["7"]}', calls2)
        # the retry lands before round 2's own (empty-batch) commit
        self.assertLess(calls2.index('COMMIT {"cursor":"B2"'),
                        calls2.index('COMMIT {"cursor":"B9"'))
        self.assertNotIn("new_wechat_messages", calls2)
        self.assertIsNone(self.pending())
        self.assertFalse(os.path.exists(ledger))
        # Final cursor: B2 via the retried ledger, then B9 via round
        # 2's own empty-batch commit — monotonically advanced, and the
        # B2 batch was committed exactly once (see COMMIT order above).
        with open(os.path.join(self.bridge, "cursor")) as f:
            self.assertEqual(f.read(), "B9")

    def test_pending_wake_reports_real_message_count(self):
        # Regression (2026-10-22 review): the pending branch counted with
        # `jq 'length'`, which counts the queue object's KEYS (always 1),
        # not the queued messages. The wake payload must carry the real
        # message count.
        pend_path = os.path.join(self.state, "ilink-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": [{"from_user_id": "owner1", "message_id": n}
                                  for n in (11, 12, 13)]}, f)
        calls = self.run_hook()
        self.assertIn('WAKE pending_wechat_messages {"count":3', calls)


if __name__ == "__main__":
    unittest.main()
