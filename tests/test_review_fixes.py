#!/usr/bin/env python3
"""Regression tests for the 2026-10-22 full code review fixes.

Run from the repo root:

    python3 -m unittest discover -s tests -v
    # or: python3 tests/test_review_fixes.py

Two bugs pinned here:

1. download_media_item used to print MEDIA_ERROR diagnostics to STDOUT.
   cmd_poll's stdout is a machine-readable JSON channel — one stray line
   corrupted the whole batch for every caller (the hook's jq parse failed
   and treated the batch as a poll crash: never queued, never committed,
   re-fetched forever). Diagnostics must go to stderr only.

2. Inbound voice duration was read from a `voice_duration_ms` key that
   does not exist on the wire; the raw voice_item field is `playtime`.
   Every received voice message therefore reported a duration of 0.
"""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ["ILINK_DATA_DIR"] = tempfile.mkdtemp(prefix="ilink-test-data-")
import ilink  # noqa: E402


class ReviewFixesTest(unittest.TestCase):
    def setUp(self):
        # Pin the seen-ids file to a fresh temp path: unittest discover
        # runs every module in one process and ilink's data dir is frozen
        # at first import, so never rely on the shared one.
        self._old_seen = ilink.SEEN_FILE
        fd, self._seen_path = tempfile.mkstemp(prefix="ilink-seen-")
        os.close(fd)
        os.unlink(self._seen_path)
        ilink.SEEN_FILE = self._seen_path
        self.addCleanup(self._restore)

    def _restore(self):
        ilink.SEEN_FILE = self._old_seen
        if os.path.exists(self._seen_path):
            os.unlink(self._seen_path)

    def test_media_error_goes_to_stderr_not_stdout(self):
        item = {"voice_item": {"media": {}}}  # no aes_key anywhere
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            result = ilink.download_media_item(item, 3)
        self.assertIsNone(result)
        self.assertEqual(out.getvalue(), "")
        self.assertIn("MEDIA_ERROR", err.getvalue())

    def test_poll_stdout_is_pure_json_when_media_fails(self):
        # A voice item whose download blows up must still yield ONE clean
        # JSON document on stdout — the batch may not be poisoned.
        voice_msg = {
            "message_id": 987001, "message_type": 1,
            "from_user_id": "u1", "context_token": "ctx1",
            "item_list": [{"type": 3, "voice_item": {
                "text": "你好", "playtime": 4200,
                "media": {"aes_key": "AAAA"}}}],
        }
        payload = {"msgs": [voice_msg], "get_updates_buf": "BUF2"}
        saved = (ilink.load_session, ilink.api, ilink.load_contacts,
                 ilink.save_contacts, ilink.download_media_item,
                 ilink.load_owner_ids)
        ilink.load_session = lambda: {"bot_token": "tok"}
        ilink.api = lambda *a, **k: payload
        ilink.load_contacts = lambda: {}
        ilink.save_contacts = lambda c: None
        ilink.load_owner_ids = lambda: {"u1"}  # sender is the owner here
        ilink.download_media_item = lambda item, t, **k: (_ for _ in ()).throw(
            RuntimeError("boom"))
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = ilink.cmd_poll()
        finally:
            (ilink.load_session, ilink.api, ilink.load_contacts,
             ilink.save_contacts, ilink.download_media_item,
             ilink.load_owner_ids) = saved
        self.assertEqual(rc, 0)
        data = json.loads(out.getvalue())  # must parse as a single document
        self.assertEqual(len(data["msgs"]), 1)
        self.assertEqual(data["msgs"][0]["media_error"], "boom")
        self.assertEqual(data["commit"]["ids"], ["987001"])

    def test_voice_playtime_maps_to_duration(self):
        voice_msg = {
            "message_id": 987002, "message_type": 1,
            "from_user_id": "u1", "context_token": "ctx1",
            "item_list": [{"type": 3, "voice_item": {
                "text": "在吗", "playtime": 3650}}],
        }
        payload = {"msgs": [voice_msg], "get_updates_buf": "BUF3"}
        saved = (ilink.load_session, ilink.api, ilink.load_contacts,
                 ilink.save_contacts, ilink.download_media_item,
                 ilink.load_owner_ids)
        ilink.load_session = lambda: {"bot_token": "tok"}
        ilink.api = lambda *a, **k: payload
        ilink.load_contacts = lambda: {}
        ilink.save_contacts = lambda c: None
        ilink.load_owner_ids = lambda: {"u1"}  # sender is the owner here
        ilink.download_media_item = lambda item, t, **k: None
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = ilink.cmd_poll()
        finally:
            (ilink.load_session, ilink.api, ilink.load_contacts,
             ilink.save_contacts, ilink.download_media_item,
             ilink.load_owner_ids) = saved
        self.assertEqual(rc, 0)
        entry = json.loads(out.getvalue())["msgs"][0]
        self.assertEqual(entry["kind"], "voice")
        self.assertEqual(entry["text"], "在吗")
        self.assertEqual(entry["voice_duration_ms"], 3650)


if __name__ == "__main__":
    unittest.main()
