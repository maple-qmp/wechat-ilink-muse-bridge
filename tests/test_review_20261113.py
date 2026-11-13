#!/usr/bin/env python3
"""Regression tests for the 2026-11-13 review fixes.

  1. API status: a response is judged on code / ret / errcode, not on
     `code` alone. {"code": 0, "ret": -2} used to be SEND_OK and burned
     the ledger key (the reply was lost for good); a failing second
     proactive attempt used to print PROACTIVE_OK; a poll with ret != 0
     used to look like an empty batch, so poll_failing never fired.
  2. Worker claim protocol: `ilink.py claim / claim --merge / release`
     under one queue lock. A bare `mv pending processing` replaced an
     existing claim, so a second worker destroyed the first one's batch.
     (The hook-side half is in test_hook_integration.)
  3. Media budget: poll's downloads share one deadline, so a slow CDN
     can no longer push a round past the hook's 120s timeout and turn
     the batch into a poison batch that is re-fetched forever.
  4. Burst cursor: the daemon's burst merge continues each poll from the
     previous uncommitted poll's cursor instead of replaying the
     committed one.
  5. contacts.json: read-modify-write runs under a lock and merges, so a
     typing call can no longer revert a context_token poll just wrote.

    python3 -m unittest discover -s tests -v
"""
import base64
import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, ROOT)
DATA_DIR = tempfile.mkdtemp(prefix="ilink-test-review13-")
STATE_DIR = tempfile.mkdtemp(prefix="ilink-test-review13-state-")
os.environ.setdefault("ILINK_DATA_DIR", DATA_DIR)
import daemon  # noqa: E402
import ilink  # noqa: E402


def _pin_paths():
    ilink.BRIDGE = DATA_DIR
    ilink.SESSION_FILE = os.path.join(DATA_DIR, "session.json")
    ilink.CONTACTS_FILE = os.path.join(DATA_DIR, "contacts.json")
    ilink.SEEN_FILE = os.path.join(DATA_DIR, "seen_ids.json")
    ilink.SEND_LEDGER_FILE = os.path.join(DATA_DIR, "send_ledger.json")
    ilink.SEND_LEDGER_LOCK_FILE = os.path.join(DATA_DIR, "send_ledger.lock")
    ilink.SEND_AUDIT_FILE = os.path.join(DATA_DIR, "send_audit.jsonl")
    ilink.QUOTE_CACHE_FILE = os.path.join(DATA_DIR, "quote_cache.json")
    ilink.QUOTE_CACHE_LOCK_FILE = os.path.join(DATA_DIR, "quote_cache.lock")
    ilink.TOKEN_STALE_FILE = os.path.join(DATA_DIR, "token_stale.json")
    ilink.MEDIA_DIR = os.path.join(DATA_DIR, "media")
    os.environ["ILINK_HOOK_STATE_DIR"] = STATE_DIR


class _Base(unittest.TestCase):
    def setUp(self):
        _pin_paths()
        for base in (DATA_DIR, STATE_DIR):
            for name in os.listdir(base):
                path = os.path.join(base, name)
                if os.path.isfile(path):
                    os.unlink(path)
                else:
                    shutil.rmtree(path, ignore_errors=True)
        os.makedirs(ilink.MEDIA_DIR, exist_ok=True)
        ilink.save_session({"bot_token": "tok", "cursor": "C0"})
        with open(os.path.join(DATA_DIR, "owner.json"), "w") as f:
            json.dump({"owner_id": "u1", "owner_ids": ["u1"]}, f)
        ilink.save_contacts({"u1": {"context_token": "ctx-old"}})

    def run_quiet(self, func, *args, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = func(*args, **kwargs)
        return code, out.getvalue()


# --- 1. API status ----------------------------------------------------------

class ApiStatusTest(_Base):
    def test_error_code_reads_all_three_fields(self):
        cases = [
            ({}, 0), ({"code": 0}, 0), ({"code": 0, "ret": 0}, 0),
            ({"code": 0, "ret": -2}, -2), ({"ret": -2}, -2),
            ({"errcode": 5}, 5), ({"code": 1, "ret": -14}, -14),
            ({"errcode": -14}, -14), ({"ret": "bad"}, -1),
            (None, -1), ([], -1), ({"ret": None, "code": ""}, 0),
        ]
        for data, expected in cases:
            with self.subTest(data=data):
                self.assertEqual(ilink._api_error_code(data), expected)

    def test_classify_send_response_honors_ret(self):
        self.assertEqual(ilink._classify_send_response({"code": 0, "ret": -2}),
                         "app_error")
        self.assertEqual(ilink._classify_send_response({"errcode": -14}),
                         "token_expired")

    def test_send_ret_minus_2_is_a_failure_and_keeps_the_intent(self):
        with mock.patch.object(ilink, "api",
                               return_value={"code": 0, "ret": -2}):
            code, out = self.run_quiet(ilink.cmd_send, "u1", "ctx", "hi",
                                       key="k-ret")
        self.assertEqual(code, 1)
        self.assertIn("SEND_FAILED", out)
        self.assertNotIn("SEND_OK", out)
        with open(ilink.SEND_LEDGER_FILE) as f:
            ent = json.load(f)["k-ret"]
        # Still an intent: a retry reuses the client_id and really sends.
        self.assertEqual(ent["status"], "intent")
        with mock.patch.object(ilink, "api", return_value={"ret": 0}) as api:
            code, out = self.run_quiet(ilink.cmd_send, "u1", "ctx", "hi",
                                       key="k-ret")
        self.assertEqual(code, 0)
        self.assertIn("SEND_OK", out)
        sent_cid = api.call_args[0][2]["msg"]["client_id"]
        self.assertEqual(sent_cid, ent["client_id"])

    def test_send_errcode_minus_14_goes_to_token_state_machine(self):
        with mock.patch.object(ilink, "api", return_value={"errcode": -14}):
            code, out = self.run_quiet(ilink.cmd_send, "u1", "ctx", "hi")
        self.assertEqual(code, 3)
        self.assertIn("TOKEN_STALE", out)

    def test_proactive_second_ret_failure_is_not_ok(self):
        calls = []

        def fake(method, path, body=None, token=None, timeout=40):
            calls.append(body["msg"].get("context_token"))
            return {"ret": -2, "errmsg": "prepare failed"}

        with mock.patch.object(ilink, "api", side_effect=fake):
            code, out = self.run_quiet(ilink.cmd_proactive, "u1", "hi")
        self.assertEqual(code, 1)
        self.assertIn("PROACTIVE_FAILED", out)
        self.assertNotIn("PROACTIVE_OK", out)
        self.assertEqual(calls, ["ctx-old", None])  # tokenless retry ran

    def test_proactive_media_retries_tokenless_on_ret(self):
        path = os.path.join(DATA_DIR, "x.png")
        with open(path, "wb") as f:
            f.write(b"png")
        sends = []

        def fake_send(to, ctx, item, token, client_id=None):
            sends.append(ctx)
            if ctx:
                return {"code": 0, "ret": -2, "message_id": "m1"}
            return {"ret": 0, "message_id": "m2"}

        up = {"media": {}, "cipher_size": 16, "md5": "x", "rawsize": 3}
        with mock.patch.object(ilink, "_upload_media", return_value=up), \
                mock.patch.object(ilink, "_send_item_raw",
                                  side_effect=fake_send):
            code, out = self.run_quiet(ilink.cmd_proactive_media, "u1",
                                       path, 2)
        self.assertEqual(code, 0, out)
        self.assertEqual(sends, ["ctx-old", None])

    def test_poll_api_error_is_poll_error_not_empty_batch(self):
        with mock.patch.object(ilink, "api",
                               return_value={"ret": -1, "errmsg": "boom"}):
            code, out = self.run_quiet(ilink.cmd_poll)
        self.assertEqual(code, 1)
        self.assertTrue(out.startswith("POLL_ERROR:"), out)
        self.assertIn("boom", out)
        # The daemon maps it to its error/back-off path.
        with mock.patch.object(daemon.subprocess, "run",
                               return_value=mock.Mock(stdout=out)):
            self.assertEqual(daemon.poll_once()[0], "error")

    def test_poll_errcode_minus_14_is_token_stale(self):
        with mock.patch.object(ilink, "api", return_value={"errcode": -14}):
            code, out = self.run_quiet(ilink.cmd_poll)
        self.assertEqual(code, 3)
        self.assertIn("TOKEN_STALE", out)


# --- 2. Worker claim protocol ----------------------------------------------

class ClaimProtocolTest(_Base):
    def pend(self):
        return os.path.join(STATE_DIR, "ilink-pending.json")

    def proc(self):
        return os.path.join(STATE_DIR, "ilink-processing.json")

    def write_pending(self, ids):
        with open(self.pend(), "w") as f:
            json.dump({"msgs": [{"message_id": i} for i in ids]}, f)

    def read(self, path):
        with open(path) as f:
            return json.load(f)

    def claim(self):
        code, out = self.run_quiet(ilink.main, ["ilink.py", "claim"])
        cid = None
        for line in out.splitlines():
            if line.startswith("CLAIMED:"):
                cid = line.split(":")[1]
        return code, out, cid

    def test_claim_moves_pending_and_stamps_claim_id(self):
        self.write_pending([1, 2])
        code, out, cid = self.claim()
        self.assertEqual(code, 0)
        self.assertIn("CLAIMED:", out)
        self.assertIn(f"PROCESSING_FILE:{self.proc()}", out)
        self.assertFalse(os.path.exists(self.pend()))
        data = self.read(self.proc())
        self.assertEqual([m["message_id"] for m in data["msgs"]], [1, 2])
        self.assertEqual(data["claim_id"], cid)

    def test_second_claim_never_overwrites_the_first(self):
        # The old `mv pending processing` replaced worker A's claim.
        self.write_pending([1])
        _, _, cid_a = self.claim()
        self.write_pending([2])
        code, out, _ = self.claim()
        self.assertEqual(code, 3)
        self.assertIn("CLAIM_BUSY", out)
        self.assertEqual([m["message_id"] for m in self.read(self.proc())["msgs"]], [1])
        self.assertEqual(self.read(self.pend())["msgs"], [{"message_id": 2}])
        # Worker A's pre-send re-check folds the newer batch into its
        # own claim: one reply covers both messages.
        code, out = self.run_quiet(ilink.main,
                                   ["ilink.py", "claim", "--merge", cid_a])
        self.assertEqual(code, 0)
        self.assertIn("MERGED:1:2", out)
        self.assertEqual([m["message_id"] for m in self.read(self.proc())["msgs"]],
                         [1, 2])
        self.assertFalse(os.path.exists(self.pend()))

    def test_empty_queue(self):
        code, out, _ = self.claim()
        self.assertEqual(code, 3)
        self.assertIn("CLAIM_EMPTY", out)

    def test_merge_nothing_renews_lease(self):
        self.write_pending([1])
        _, _, cid = self.claim()
        old = time.time() - 300
        os.utime(self.proc(), (old, old))
        code, out = self.run_quiet(ilink.cmd_claim, merge=True, claim_id=cid)
        self.assertEqual(code, 0)
        self.assertIn("MERGE_NOTHING", out)
        self.assertGreater(os.stat(self.proc()).st_mtime, old + 200)

    def test_recycled_claim_refuses_merge_and_release(self):
        # Worker A was slow, its claim was recycled and worker B now
        # holds the batch: A must neither merge (=> must not send) nor
        # delete B's processing file.
        self.write_pending([1])
        _, _, cid_a = self.claim()
        os.replace(self.proc(), self.pend())  # what the hook's recycle does
        _, _, cid_b = self.claim()
        self.assertNotEqual(cid_a, cid_b)
        code, out = self.run_quiet(ilink.cmd_claim, merge=True, claim_id=cid_a)
        self.assertEqual((code, out.strip()), (3, "MERGE_NOT_OWNER"))
        code, out = self.run_quiet(ilink.main, ["ilink.py", "release", cid_a])
        self.assertEqual((code, out.strip()), (3, "RELEASE_NOT_OWNER"))
        self.assertTrue(os.path.exists(self.proc()))
        code, out = self.run_quiet(ilink.main, ["ilink.py", "release", cid_b])
        self.assertEqual((code, out.strip()), (0, "RELEASED"))
        self.assertFalse(os.path.exists(self.proc()))

    def test_merge_without_claim(self):
        self.write_pending([1])
        code, out = self.run_quiet(ilink.cmd_claim, merge=True, claim_id="x")
        self.assertEqual((code, out.strip()), (3, "MERGE_NOT_OWNER"))
        self.assertTrue(os.path.exists(self.pend()))

    def test_corrupt_pending_is_reported_and_left_in_place(self):
        with open(self.pend(), "w") as f:
            f.write('{"msgs": "nope"}')
        code, out, _ = self.claim()
        self.assertEqual(code, 1)
        self.assertIn("QUEUE_CORRUPT", out)
        self.assertTrue(os.path.exists(self.pend()))

    def test_concurrent_claims_have_exactly_one_winner(self):
        self.write_pending([1, 2, 3])
        env = dict(os.environ, ILINK_DATA_DIR=DATA_DIR,
                   ILINK_HOOK_STATE_DIR=STATE_DIR)
        procs = [subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "ilink.py"), "claim"],
            env=env, stdout=subprocess.PIPE, text=True) for _ in range(6)]
        outs = [p.communicate(timeout=60)[0] for p in procs]
        winners = [o for o in outs if o.startswith("CLAIMED:")]
        self.assertEqual(len(winners), 1, outs)
        self.assertEqual(len(self.read(self.proc())["msgs"]), 3)


# --- 3. Media download budget ----------------------------------------------

class MediaBudgetTest(_Base):
    def setUp(self):
        super().setUp()
        key = base64.b64encode(b"\x00" * 16).decode("ascii")
        self.item = {"image_item": {"media": {
            "aes_key": key, "full_url": "https://example.invalid/x"}}}

    def test_expired_deadline_starts_no_download(self):
        with mock.patch.object(ilink, "_http_get_range") as get:
            code, _ = self.run_quiet(ilink.download_media_item, self.item, 2,
                                     deadline=time.time() - 1)
        self.assertIsNone(code)
        get.assert_not_called()

    def test_timeouts_and_backoff_fit_inside_the_deadline(self):
        clock = [1000.0]
        timeouts, sleeps = [], []

        def fake_get(url, start=0, timeout=60):
            timeouts.append(timeout)
            clock[0] += timeout  # the socket timeout elapses
            raise TimeoutError("slow cdn")

        def fake_sleep(s):
            sleeps.append(s)
            clock[0] += s

        with mock.patch.object(ilink, "_http_get_range", side_effect=fake_get), \
                mock.patch.object(ilink.time, "time", lambda: clock[0]), \
                mock.patch.object(ilink.time, "sleep", side_effect=fake_sleep):
            res, _ = self.run_quiet(ilink.download_media_item, self.item, 2,
                                    deadline=1010.0)
        self.assertIsNone(res)
        self.assertEqual(timeouts[0], 10.0)  # capped to the budget, not 60
        self.assertLessEqual(clock[0], 1011.0)  # never far past the deadline

    def test_incomplete_download_without_length_is_refused(self):
        # No Content-Length / Content-Range and every read cut short:
        # decrypting the partial ciphertext would yield garbage.
        with mock.patch.object(ilink, "_http_get_range",
                               return_value=(200, b"\x00" * 7, None, False)), \
                mock.patch.object(ilink.time, "sleep"):
            res, _ = self.run_quiet(ilink.download_media_item, self.item, 2)
        self.assertIsNone(res)

    def test_poll_marks_items_past_the_budget_and_still_commits(self):
        msg = {"message_id": 501, "message_type": 1, "from_user_id": "u1",
               "context_token": "ctx-new",
               "item_list": [{"type": 2, "image_item": {}},
                             {"type": 4, "file_item": {"file_name": "a.pdf"}}]}
        seen = []

        def fake_download(item, t, deadline=None):
            seen.append(deadline)
            return None

        with mock.patch.object(ilink, "api", return_value={
                "msgs": [msg], "get_updates_buf": "C1"}), \
                mock.patch.object(ilink, "download_media_item",
                                  side_effect=fake_download), \
                mock.patch.object(ilink, "POLL_MEDIA_BUDGET", 0):
            code, out = self.run_quiet(ilink.cmd_poll)
        self.assertEqual(code, 0)
        data = json.loads(out)
        self.assertEqual([m["media_error"] for m in data["msgs"]],
                         [ilink.MEDIA_BUDGET_ERROR] * 2)
        self.assertEqual(data["commit"], {"cursor": "C1", "ids": ["501"]})
        # Both items share ONE round deadline.
        self.assertEqual(len(set(seen)), 1)
        self.assertIsNotNone(seen[0])


# --- 4. Burst cursor --------------------------------------------------------

class BurstCursorTest(_Base):
    def test_poll_cursor_override_is_read_only(self):
        with mock.patch.object(ilink, "api", return_value={
                "msgs": [], "get_updates_buf": "C9"}) as api:
            code, out = self.run_quiet(ilink.main,
                                       ["ilink.py", "poll", "--cursor", "C5"])
        self.assertEqual(code, 0)
        self.assertEqual(api.call_args[0][2]["get_updates_buf"], "C5")
        self.assertEqual(json.loads(out)["commit"]["cursor"], "C9")
        self.assertEqual(ilink.load_session()["cursor"], "C0")  # not saved

    def test_poll_cursor_flag_needs_a_value(self):
        code, _ = self.run_quiet(ilink.main, ["ilink.py", "poll", "--cursor"])
        self.assertEqual(code, 2)

    def test_poll_once_forwards_cursor(self):
        with mock.patch.object(daemon.subprocess, "run",
                               return_value=mock.Mock(stdout='{"msgs": []}')) as run:
            daemon.poll_once("C7")
            daemon.poll_once()
        self.assertEqual(run.call_args_list[0][0][0][-2:], ["--cursor", "C7"])
        self.assertNotIn("--cursor", run.call_args_list[1][0][0])

    def test_collect_burst_continues_from_previous_poll(self):
        cursors = []
        script = [("ok", [{"message_id": "b"}], {"cursor": "C2", "ids": ["b"]}),
                  ("ok", [], {"cursor": "C3", "ids": []})]

        def fake_poll(cursor=None):
            cursors.append(cursor)
            return script.pop(0)

        with mock.patch.object(daemon, "poll_once", side_effect=fake_poll):
            burst, commits = daemon.collect_burst(
                [{"message_id": "a"}], [{"cursor": "C1", "ids": ["a"]}])
        self.assertEqual(cursors, ["C1", "C2"])
        self.assertEqual([m["message_id"] for m in burst], ["a", "b"])
        self.assertEqual([c["cursor"] for c in commits], ["C1", "C2", "C3"])


# --- 5. contacts.json lock --------------------------------------------------

class ContactsLockTest(_Base):
    def test_typing_ticket_save_does_not_revert_context_token(self):
        def getconfig(*a, **k):
            # poll lands a fresh context_token while getconfig is in flight
            ilink.update_contacts(lambda c: c["u1"].update(
                {"context_token": "ctx-new"}))
            return {"typing_ticket": "T1"}

        with mock.patch.object(ilink, "api", side_effect=getconfig):
            self.assertEqual(ilink._typing_ticket("u1", "tok"), "T1")
        ent = ilink.load_contacts()["u1"]
        self.assertEqual(ent["context_token"], "ctx-new")
        self.assertEqual(ent["typing_ticket"], "T1")

    def test_poll_merges_context_token_and_keeps_ticket(self):
        ilink.save_contacts({"u1": {"context_token": "ctx-old",
                                    "typing_ticket": "T0",
                                    "typing_ticket_at": 123}})
        msg = {"message_id": 601, "message_type": 1, "from_user_id": "u1",
               "context_token": "ctx-new",
               "item_list": [{"type": 1, "text_item": {"text": "hi"}}]}
        with mock.patch.object(ilink, "api", return_value={
                "msgs": [msg], "get_updates_buf": "C1"}):
            code, _ = self.run_quiet(ilink.cmd_poll)
        self.assertEqual(code, 0)
        ent = ilink.load_contacts()["u1"]
        self.assertEqual(ent["context_token"], "ctx-new")
        self.assertEqual(ent["typing_ticket"], "T0")

    def test_update_contacts_serializes_writers(self):
        env = dict(os.environ, ILINK_DATA_DIR=DATA_DIR)
        code = ("import ilink\n"
                "for i in range(40):\n"
                "    ilink.update_contacts(lambda c: c.__setitem__("
                "'n', c.get('n', 0) + 1))\n")
        ilink.save_contacts({})
        procs = [subprocess.Popen([sys.executable, "-c", code], env=env,
                                  cwd=ROOT) for _ in range(4)]
        for p in procs:
            self.assertEqual(p.wait(timeout=120), 0)
        self.assertEqual(ilink.load_contacts()["n"], 160)  # no lost update


if __name__ == "__main__":
    unittest.main()
