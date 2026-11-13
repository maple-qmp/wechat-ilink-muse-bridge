#!/usr/bin/env python3
"""Daemon handle-then-commit tests (daemon demoted to example driver,
2026-10-24, BACKLOG item 1 route B).

Run from the repo root:

    python3 -m unittest discover -s tests -v
    # or: python3 tests/test_daemon_commit.py

The daemon used to commit the cursor the moment a batch arrived and
process it afterwards — a crash in between lost the batch for good,
and commit failures were swallowed without even a return-code check.
Now, as an explicitly demoted example driver, it handles first and
commits after, with these pinned semantics:

  - handler raises        -> no commit attempted, traceback logged as
                             HANDLE_FAILED, the loop carries on;
  - normal path           -> commit strictly after handling;
  - commit exits nonzero  -> COMMIT_FAILED logged, nothing raises,
                             and the un-advanced cursor re-delivers
                             the same batch next round, which then
                             completes (at-least-once recovery).
"""
import contextlib
import io
import json
import os
import subprocess
import sys
import unittest
from unittest import mock

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, ROOT)
import daemon  # noqa: E402

MSG = {"from_user_id": "u1", "context_token": "ctx-1",
       "text": "hello", "kind": "text"}
MSG2 = {"from_user_id": "u1", "context_token": "ctx-2",
        "text": "again", "kind": "text"}
COMMIT = {"cursor": "BUF-2", "ids": ["4242"]}
COMMIT2 = {"cursor": "BUF-3", "ids": ["4243"]}


class MainLoopHarness(unittest.TestCase):
    """Runs daemon.main() against a scripted poll_once.

    The script is a list of (status, msgs, commit) tuples; when it is
    exhausted the fake poll raises KeyboardInterrupt, which is how the
    test escapes the daemon's infinite loop.
    """

    def setUp(self):
        self.events = []
        self.poll_script = []
        self._orig_poll = daemon.poll_once
        self._orig_collect = daemon.collect_burst
        self._orig_handle = daemon.handle_burst
        self._orig_argv = sys.argv
        # The shared driver gate is tested separately (test_driver_lock)
        # and must not make this suite depend on the live hook's
        # heartbeat in ~/hooks/state: stub it out here.
        self._orig_gate = daemon.acquire_driver_lock_or_exit
        daemon.acquire_driver_lock_or_exit = lambda: None
        sys.argv = ["daemon.py"]
        daemon.poll_once = self._fake_poll
        # Burst collecting is not under test here: pass rounds through.
        daemon.collect_burst = lambda msgs, commits: (list(msgs), list(commits))
        self.addCleanup(self._restore)

    def _restore(self):
        daemon.poll_once = self._orig_poll
        daemon.collect_burst = self._orig_collect
        daemon.handle_burst = self._orig_handle
        daemon.acquire_driver_lock_or_exit = self._orig_gate
        sys.argv = self._orig_argv

    def _fake_poll(self):
        if not self.poll_script:
            raise KeyboardInterrupt
        return self.poll_script.pop(0)

    def run_main(self):
        buf_out, buf_err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(buf_out), \
                contextlib.redirect_stderr(buf_err):
            with self.assertRaises(KeyboardInterrupt):
                daemon.main()
        return buf_out.getvalue(), buf_err.getvalue()


class HandleThenCommitTest(MainLoopHarness):
    def setUp(self):
        super().setUp()
        self._orig_commit = daemon.commit_batch
        daemon.commit_batch = self._fake_commit
        self.addCleanup(setattr, daemon, "commit_batch", self._orig_commit)

    def _fake_commit(self, commit):
        self.events.append(("commit", commit))
        return True

    def _record_handle(self, msgs, echo=False, llm=False):
        self.events.append(("handle", list(msgs)))

    def test_commit_runs_after_handling(self):
        daemon.handle_burst = self._record_handle
        self.poll_script = [("ok", [MSG], COMMIT)]
        self.run_main()
        self.assertEqual([k for k, _ in self.events], ["handle", "commit"])
        self.assertEqual(self.events[1][1], COMMIT)

    def test_handler_exception_skips_commit_and_loop_continues(self):
        attempts = []

        def flaky_handle(msgs, echo=False, llm=False):
            self.events.append(("handle", list(msgs)))
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("handler boom")

        daemon.handle_burst = flaky_handle
        self.poll_script = [("ok", [MSG], COMMIT), ("ok", [MSG2], COMMIT2)]
        out, err = self.run_main()
        # Both rounds reached the handler: the loop survived the raise.
        self.assertEqual([k for k, _ in self.events].count("handle"), 2)
        # Only the healthy second round committed, with its own block.
        commits = [c for k, c in self.events if k == "commit"]
        self.assertEqual(commits, [COMMIT2])
        # Handler failure and commit failure are distinguishable in logs.
        self.assertIn("HANDLE_FAILED", out)
        self.assertIn("RuntimeError", err)  # traceback went to stderr
        self.assertNotIn("COMMIT_FAILED", out)


class CommitFailureRecoveryTest(MainLoopHarness):
    """Real commit_batch + fake subprocess: the first commit exits 1,
    so the same batch is re-delivered next round and then completes."""

    def setUp(self):
        super().setUp()
        self.commit_calls = 0
        self.handled = []
        self._orig_run = daemon.subprocess.run
        daemon.subprocess.run = self._fake_run
        daemon.handle_burst = self._record_handle
        self.addCleanup(setattr, daemon.subprocess, "run", self._orig_run)

    def _record_handle(self, msgs, echo=False, llm=False):
        self.handled.append(list(msgs))

    def _fake_run(self, args, **kwargs):
        self.assertEqual(args[-1], "commit")
        self.commit_calls += 1
        if self.commit_calls == 1:
            return subprocess.CompletedProcess(
                args, 1, stdout="COMMIT_ERROR:simulated failure")
        return subprocess.CompletedProcess(args, 0, stdout="COMMIT_OK")

    def test_commit_failure_logs_and_redelivery_completes(self):
        # Same batch twice: the second is the server re-delivering
        # because the failed commit never advanced the cursor.
        self.poll_script = [("ok", [MSG], COMMIT), ("ok", [MSG], COMMIT)]
        out, _ = self.run_main()  # must not raise despite the failure
        self.assertEqual(self.commit_calls, 2)
        self.assertEqual(out.count("COMMIT_FAILED"), 1)
        # At-least-once: handled twice, second round's commit went fine.
        self.assertEqual(len(self.handled), 2)


class CommitBatchTest(unittest.TestCase):
    def run_commit_batch(self, fake_run, commit=COMMIT):
        orig = daemon.subprocess.run
        daemon.subprocess.run = fake_run
        try:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                ok = daemon.commit_batch(commit)
            return ok, buf.getvalue()
        finally:
            daemon.subprocess.run = orig

    def test_success_requires_commit_ok(self):
        ok, _ = self.run_commit_batch(
            lambda *a, **k: subprocess.CompletedProcess(
                a[0], 0, stdout="COMMIT_OK"))
        self.assertTrue(ok)

    def test_nonzero_exit_is_failure_not_exception(self):
        ok, out = self.run_commit_batch(
            lambda *a, **k: subprocess.CompletedProcess(
                a[0], 1, stdout="COMMIT_ERROR:bad"))
        self.assertFalse(ok)
        self.assertIn("COMMIT_FAILED", out)

    def test_missing_commit_ok_is_failure(self):
        ok, out = self.run_commit_batch(
            lambda *a, **k: subprocess.CompletedProcess(
                a[0], 0, stdout="something else"))
        self.assertFalse(ok)
        self.assertIn("COMMIT_FAILED", out)

    def test_subprocess_error_is_failure_not_exception(self):
        def boom(*a, **k):
            raise OSError("no such process")

        ok, out = self.run_commit_batch(boom)
        self.assertFalse(ok)
        self.assertIn("COMMIT_FAILED", out)

    def test_empty_commit_is_noop_success(self):
        calls = []

        def fake_run(*a, **k):
            calls.append(a)
            return subprocess.CompletedProcess(a[0], 0, stdout="COMMIT_OK")

        ok, _ = self.run_commit_batch(fake_run, commit=None)
        self.assertTrue(ok)
        self.assertEqual(calls, [])


class HandleSendReturncodeTest(unittest.TestCase):
    """handle_burst must not swallow a failed send: a nonzero exit
    raises (so main skips the commit), and the send carries --key."""

    def setUp(self):
        self.send_calls = []
        self._orig_run = daemon.subprocess.run
        daemon.subprocess.run = self._fake_run
        self.addCleanup(setattr, daemon.subprocess, "run", self._orig_run)

    def _fake_run(self, args, **kwargs):
        if "typing" in args:
            return subprocess.CompletedProcess(args, 0, stdout="")
        self.send_calls.append(list(args))
        return self.send_result

    def _handle(self, msgs):
        daemon.handle_burst(msgs, echo=True)

    def test_nonzero_send_exit_raises_and_carries_key(self):
        self.send_result = subprocess.CompletedProcess([], 1,
                                                       stdout="SEND_FAILED:x")
        msg = dict(MSG, message_id="4242")
        with self.assertRaises(RuntimeError):
            self._handle([msg])
        self.assertEqual(len(self.send_calls), 1)
        args = self.send_calls[0]
        self.assertIn("--key", args)
        self.assertEqual(args[args.index("--key") + 1], "4242")

    def test_zero_send_exit_returns_normally(self):
        self.send_result = subprocess.CompletedProcess([], 0, stdout="SEND_OK")
        self._handle([dict(MSG, message_id="4243")])  # must not raise
        self.assertEqual(len(self.send_calls), 1)

    def test_no_message_id_means_no_key(self):
        self.send_result = subprocess.CompletedProcess([], 0, stdout="SEND_OK")
        self._handle([MSG])  # MSG has no message_id
        self.assertNotIn("--key", self.send_calls[0])

    def test_failed_send_skips_commit_in_main_loop(self):
        self.send_result = subprocess.CompletedProcess([], 1,
                                                       stdout="SEND_FAILED:x")
        harness = MainLoopHarness()
        harness.setUp()
        # main() decides echo/llm from argv: without --echo the
        # handler skips sending entirely and this test would prove
        # nothing. _restore puts argv back afterwards.
        sys.argv = ["daemon.py", "--echo"]
        commits = []
        orig_commit = daemon.commit_batch
        daemon.commit_batch = lambda c: commits.append(c) or True
        try:
            harness.poll_script = [("ok", [dict(MSG, message_id="4244")],
                                   COMMIT)]
            out, _ = harness.run_main()
            self.assertIn("HANDLE_FAILED", out)
            self.assertEqual(commits, [])
        finally:
            daemon.commit_batch = orig_commit
            harness._restore()


class PollOnceTest(unittest.TestCase):
    def test_poll_once_returns_commit_unsubmitted(self):
        calls = []

        def fake_run(args, **kwargs):
            calls.append(args[-1])
            payload = {"msgs": [dict(MSG)], "commit": COMMIT,
                       "get_updates_buf": "BUF-2"}
            return subprocess.CompletedProcess(
                args, 0, stdout=json.dumps(payload))

        orig = daemon.subprocess.run
        daemon.subprocess.run = fake_run
        try:
            status, msgs, commit = daemon.poll_once()
        finally:
            daemon.subprocess.run = orig
        self.assertEqual(status, "ok")
        self.assertEqual(msgs, [MSG])
        self.assertEqual(commit, COMMIT)
        # The two-phase protocol is preserved, but the commit is the
        # caller's job now: poll_once itself must not fire it.
        self.assertEqual(calls, ["poll"])


if __name__ == "__main__":
    unittest.main()


class CollectBurstDedupeTest(unittest.TestCase):
    """collect_burst dedupes by message_id in memory (ChatGPT 6th-round
    review, P1). Every poll in a burst runs before any commit, so each
    poll subprocess re-reads the un-advanced cursor; filter_seen only
    filters ids persisted at commit time. If the server replays from
    the old cursor, the same batch comes back twice — the burst must
    still contain each message once."""

    def setUp(self):
        self._orig_poll = daemon.poll_once
        self._orig_window = daemon.MERGE_WINDOW

    def tearDown(self):
        daemon.poll_once = self._orig_poll
        daemon.MERGE_WINDOW = self._orig_window

    def _scripted_poll(self, script):
        it = iter(script)

        def fake(cursor=None):
            return next(it)
        return fake

    def test_replayed_batch_appears_once(self):
        m1 = {"from_user_id": "u1", "message_id": "a",
              "context_token": "ctx-1", "text": "hello", "kind": "text"}
        m2 = {"from_user_id": "u1", "message_id": "b",
              "context_token": "ctx-2", "text": "again", "kind": "text"}
        c1 = {"cursor": "BUF-1", "ids": ["a", "b"]}
        c2 = {"cursor": "BUF-2", "ids": ["a", "b"]}
        c3 = {"cursor": "BUF-3", "ids": []}
        daemon.poll_once = self._scripted_poll([
            ("ok", [dict(m1), dict(m2)], c2),
            ("ok", [], c3),
        ])
        burst, commits = daemon.collect_burst([m1, m2], [c1])
        self.assertEqual([m["message_id"] for m in burst], ["a", "b"])
        self.assertEqual(commits, [c1, c2, c3])

    def test_first_batch_internal_duplicate_deduped(self):
        m1 = {"from_user_id": "u1", "message_id": "a",
              "context_token": "ctx-1", "text": "hello", "kind": "text"}
        daemon.poll_once = self._scripted_poll([("ok", [], None)])
        burst, _ = daemon.collect_burst([m1, dict(m1)], [])
        self.assertEqual([m["message_id"] for m in burst], ["a"])

    def test_entries_without_message_id_are_kept(self):
        # No id to dedupe on: never drop, even if the content repeats.
        n1 = {"from_user_id": "u1", "context_token": "ctx-1",
              "text": "hello", "kind": "text"}
        daemon.poll_once = self._scripted_poll([
            ("ok", [dict(n1)], None),
            ("ok", [], None),
        ])
        burst, _ = daemon.collect_burst([dict(n1)], [])
        self.assertEqual(len(burst), 2)
        self.assertTrue(all("message_id" not in m for m in burst))

    def test_replay_only_poll_does_not_extend_window(self):
        # A poll that returns only duplicates is not burst activity:
        # it must not push the idle deadline out. Fake clock: deadline
        # is computed at 1000 (window 25 -> 1025); the loop check at
        # 1015 still runs one replay poll, but the check at 1030 must
        # exit instead of polling again.
        class FakeClock:
            def __init__(self, values):
                self._it = iter(values)
                self._last = values[-1]

            def time(self):
                try:
                    self._last = next(self._it)
                except StopIteration:
                    pass
                return self._last

            def sleep(self, s):
                pass

        m1 = {"from_user_id": "u1", "message_id": "a",
              "context_token": "ctx-1", "text": "hello", "kind": "text"}
        calls = []
        replay = {"from_user_id": "u1", "message_id": "a",
                  "context_token": "ctx-1", "text": "hello", "kind": "text"}

        def fake_poll(cursor=None):
            calls.append(1)
            return "ok", [dict(replay)], None

        daemon.poll_once = fake_poll
        daemon.MERGE_WINDOW = 25
        with mock.patch.object(daemon, "time",
                               FakeClock([1000, 1015, 1030, 1060, 1090])):
            burst, _ = daemon.collect_burst([m1], [])
        self.assertEqual([m["message_id"] for m in burst], ["a"])
        self.assertEqual(len(calls), 1)
