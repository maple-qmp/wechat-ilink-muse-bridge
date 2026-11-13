#!/usr/bin/env python3
"""iLink WeChat bot client (official Tencent protocol, no OpenClaw needed).

Usage:
  ilink.py qrcode              # get login QR, save image, print QR_CODE:<id>
  ilink.py status <qrcode>     # poll scan status once (JSON)
  ilink.py login <qrcode>      # poll until confirmed (up to 300s), save token
  ilink.py bind [seconds]      # one 10-min owner-binding window (first sender wins + welcome)
  ilink.py poll [--cursor <buf>]  # fetch new messages since cursor (JSON; two-phase: see commit)
  ilink.py commit              # commit a poll round (commit block via stdin): cursor + seen ids
  ilink.py claim               # worker: take pending -> processing (refuses if a claim exists)
  ilink.py claim --merge <id>  # worker: pre-send re-check, append newer pending msgs to own claim
  ilink.py release <id>        # worker: drop processing after the reply (only if still ours)
  ilink.py send <to> <ctx> <text> [--key <id>]  # send text reply (use - for stdin)
  ilink.py proactive <to> <text> [--key <id>]   # proactive push via cached context_token
  ilink.py typing <to> <start|stop>  # show/cancel the "typing..." indicator
  ilink.py send-image <to> <ctx> <path> [--key <id>]   # reply with an image
  ilink.py send-file <to> <ctx> <path> [--key <id>]    # reply with a file
  ilink.py send-video <to> <ctx> <path> [--key <id>]   # reply with a video
  ilink.py send-voice <to> <ctx> <path> <playtime_ms>   # voice msg (SILK@24k; NOTE: channel silently drops outbound voice)
  ilink.py proactive-image <to> <path>    # proactive image via cached ctx
  ilink.py proactive-file <to> <path>     # proactive file via cached ctx
  ilink.py health [--offline]  # self-check: auth, queue, last poll (exit 0 ok / 1 warn / 2 bad)
  ilink.py smoke-test [--no-send] [--to <owner>]  # end-to-end chain check: login probe, receive readiness, real text+image to owner, ledger (exit 0 ok / 1 fail)
  ilink.py watchdog [--max-age-mins N]  # dead-man's switch: hook heartbeat freshness (exit 0 fresh/recovered/disabled, 1 stale)
  ilink.py rotate-chatlog  # archive past-month chatlog.md lines into chatlog-YYYY-MM.md (exit 0)
  ilink.py rollback [--hook-dir <dir>]  # restore ilink.py (+ hook scripts) from newest .bak-* backup (exit 0 ok / 1 fail)
  ilink.py stats  # offline snapshot: today's exchanges/sends, ledger, quote cache, media, session, queue (exit 0)
  ilink.py preflight  # pre-login env check: Python, cryptography, network, writable dirs (exit 0 ok / 1 fail)
  ilink.py proactive-video <to> <path>    # proactive video via cached ctx
  ilink.py proactive-voice <to> <path> <playtime_ms>   # proactive voice (dropped by channel, see send-voice)

  --key <id> is an idempotency key (e.g. the batch's first message_id):
  a second invocation with the same key prints SEND_ALREADY_SENT and
  does not call sendmessage again. A keyed send runs under an
  inter-process ledger lock and persists its intent (with a stable
  client_id) BEFORE calling sendmessage; only a confirmed success
  flips the entry to sent, and a retry reuses the same client_id, so
  the server's client_id dedupe covers the send-succeeded-but-
  unconfirmed window too. Legacy ledger entries without a status
  field (result 'ok') still count as sent. Every send attempt is also
  appended to send_audit.jsonl in the data dir (timestamp, direction,
  recipient, client_id, result, error type — never message text or
  tokens). A retry under the same key with different text delivers at
  most the first text — the deduped retry never re-delivers.
"""
import base64
import contextlib
import functools
import inspect
import io
import json
import os
import random
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

try:
    import fcntl
except ImportError:  # Windows has no fcntl: the ledger lock degrades
    fcntl = None  # to a no-op there (single-process use is unaffected).

BASE_URL = "https://ilinkai.weixin.qq.com"
# Data directory (session token, contacts cache, downloaded media).
# Override with the ILINK_DATA_DIR environment variable. Everything in here is
# PRIVATE (the session file is a login credential) — never commit it.
BRIDGE = os.environ.get(
    "ILINK_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
)
os.makedirs(BRIDGE, exist_ok=True)
os.makedirs(os.path.join(BRIDGE, "media"), exist_ok=True)
SESSION_FILE = os.path.join(BRIDGE, "session.json")
CONTACTS_FILE = os.path.join(BRIDGE, "contacts.json")  # from_user_id -> {context_token, updated_at}
SEEN_FILE = os.path.join(BRIDGE, "seen_ids.json")  # delivered inbound message_ids (dedupe)
SEND_AUDIT_FILE = os.path.join(BRIDGE, "send_audit.jsonl")  # one JSON line per send attempt
SEND_LEDGER_FILE = os.path.join(BRIDGE, "send_ledger.json")  # idempotency key -> intent / successful send
SEND_LEDGER_LOCK_FILE = os.path.join(BRIDGE, "send_ledger.lock")  # inter-process lock for keyed sends
SEND_AUDIT_CAP = 1000  # audit lines kept; older lines are trimmed on write
SEND_LEDGER_TTL = 86400  # ledger entries older than this are pruned on write
QUOTE_CACHE_FILE = os.path.join(BRIDGE, "quote_cache.json")  # msg_id -> quoted original {text, kind, from, at, media_path?}; plus svr_id -> msg_id
QUOTE_CACHE_LOCK_FILE = os.path.join(BRIDGE, "quote_cache.lock")  # inter-process lock for cache read-modify-write
QUOTE_CACHE_TTL = 30 * 86400  # quoted originals older than this are pruned on write (mirrors official text retention)
QUOTE_CACHE_CAP = 10000  # entries kept; oldest pruned first on write (mirrors official per-account cap)
MEDIA_MAX_BYTES = 100 * 1024 * 1024  # media sends refuse above this size:
# the protocol needs the whole file AES-encrypted and uploaded in one
# POST, so a send reads the file fully into memory (plus ciphertext).
# Refusing up front beats letting a huge file exhaust process memory;
# chunked upload would need a protocol change and is out of scope.
# Downloads share the same cap (M3): an inbound item larger than this
# is refused before its ciphertext can exhaust poll's memory.
MEDIA_DIR = os.path.join(BRIDGE, "media")
# Total seconds one poll round may spend downloading media. Must keep a
# poll (long-poll hold <=~40s + this) inside the hook's `timeout 120`.
try:
    POLL_MEDIA_BUDGET = max(1.0, float(os.environ.get(
        "ILINK_POLL_MEDIA_BUDGET", "60")))
except ValueError:
    POLL_MEDIA_BUDGET = 60.0
MEDIA_BUDGET_ERROR = "download timed out (poll media budget)"
CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"

# MessageItemType: TEXT=1, IMAGE=2, VOICE=3, FILE=4, VIDEO=5
ITEM_TYPE_NAMES = {1: "text", 2: "image", 3: "voice", 4: "file", 5: "video"}
ITEM_KEY = {2: "image_item", 3: "voice_item", 4: "file_item", 5: "video_item"}


def _aes_ecb_decrypt(ciphertext: bytes, key: bytes) -> bytes:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.padding import PKCS7
    cipher = Cipher(algorithms.AES(key), modes.ECB())
    dec = cipher.decryptor()
    padded = dec.update(ciphertext) + dec.finalize()
    unpadder = PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


def _parse_aes_key(aes_key_b64: str) -> bytes:
    raw = base64.b64decode(aes_key_b64)
    if len(raw) == 16:
        return raw
    if len(raw) == 32:
        try:
            return bytes.fromhex(raw.decode("ascii"))
        except ValueError:
            pass
    raise ValueError(f"bad aes_key length: {len(raw)}")


def _http_get_range(url, start=0, timeout=60):
    """GET url, optionally resuming from byte offset `start`.
    Returns (status, data, total_len_or_None, complete_bool). IncompleteRead 被内部消化为 complete=False。"""
    from http.client import IncompleteRead
    req_headers = {"User-Agent": "Mozilla/5.0"}
    if start:
        req_headers["Range"] = f"bytes={start}-"
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        status = r.status
        total = None
        cr = r.headers.get("Content-Range")  # e.g. "bytes 100-99183/99184"
        if cr and "/" in cr:
            try:
                total = int(cr.rsplit("/", 1)[1])
            except ValueError:
                pass
        if total is None:
            cl = r.headers.get("Content-Length")
            if cl:
                try:
                    # 206 的 Content-Length 是剩余字节数，200 的是全长
                    total = int(cl) + (start if status == 206 else 0)
                except ValueError:
                    pass
        try:
            data = r.read()
            complete = True
        except IncompleteRead as e:
            data = e.partial or b""
            complete = False
        return status, data, total, complete


def download_media_item(item: dict, item_type: int, deadline=None):
    """Download + AES-ECB decrypt an IMAGE/VOICE/FILE/VIDEO item. Returns saved path or None.
    Diagnostics go to stderr only: poll's stdout is a machine-readable JSON
    channel, and any extra line printed there corrupts the whole batch for
    callers (hook jq / daemon json.loads) — regression-tested.

    deadline (epoch seconds, optional) caps the whole download: each
    attempt's socket timeout and back-off sleep shrink to fit, and no
    new attempt starts once it has passed (MEDIA_ERROR:...budget). poll
    passes its per-round media budget here — see POLL_MEDIA_BUDGET."""
    key = ITEM_KEY.get(item_type)
    if not key:
        return None
    sub = item.get(key) or {}
    media = sub.get("media") or {}
    aes_key_b64 = media.get("aes_key")
    if not aes_key_b64:
        aes_hex = sub.get("aeskey")
        if aes_hex:
            aes_key_b64 = base64.b64encode(bytes.fromhex(aes_hex)).decode("ascii")
    if not aes_key_b64:
        print(f"MEDIA_ERROR:no aes_key", file=sys.stderr, flush=True)
        return None
    aes_key = _parse_aes_key(aes_key_b64)
    full_url = media.get("full_url")
    eqp = media.get("encrypt_query_param")
    if full_url:
        url = full_url
    elif eqp:
        from urllib.parse import quote
        url = f"{CDN_BASE_URL}/download?encrypted_query_param={quote(eqp, safe='')}"
    else:
        print(f"MEDIA_ERROR:no download url", file=sys.stderr, flush=True)
        return None
    ciphertext = b""
    total = None
    last_err = "no attempt"
    def _remaining():
        return None if deadline is None else deadline - time.time()

    def _backoff(attempt):
        rem = _remaining()
        delay = 2 * (attempt + 1)
        time.sleep(delay if rem is None else max(0, min(delay, rem)))

    for attempt in range(4):
        rem = _remaining()
        if rem is not None and rem <= 0:
            last_err = "budget exhausted"
            break
        timeout = 60 if rem is None else max(1, min(60, rem))
        try:
            status, chunk, total, complete = _http_get_range(url, start=len(ciphertext), timeout=timeout)
        except Exception as e:
            last_err = f"{type(e).__name__}:{str(e)[:100]}"
            _backoff(attempt)
            continue
        if total is not None and total > MEDIA_MAX_BYTES:
            # M3: the send side refuses above MEDIA_MAX_BYTES; the
            # download side used to have no cap at all. Refuse before
            # any more bytes accumulate.
            print(f"MEDIA_ERROR:too large {total}>{MEDIA_MAX_BYTES}",
                  file=sys.stderr, flush=True)
            return None
        if status == 206 and len(ciphertext):
            ciphertext += chunk
        else:
            # 200 或服务器忽略 Range：以本次响应为准（可能是不完整的，靠 complete/total 判断）
            ciphertext = chunk
        if len(ciphertext) > MEDIA_MAX_BYTES:
            # A server that lies about total (or omits it) still
            # cannot grow the buffer past the cap.
            print(f"MEDIA_ERROR:too large {len(ciphertext)}>{MEDIA_MAX_BYTES}",
                  file=sys.stderr, flush=True)
            return None
        last_err = "ok" if complete else f"short read {len(ciphertext)}/{total}"
        if total is not None and len(ciphertext) >= total:
            break
        if complete and ciphertext:
            break  # 无长度信息但一次读完，视为完成
        _backoff(attempt)
    if total is not None and len(ciphertext) < total:
        print(f"MEDIA_ERROR:incomplete after retries {len(ciphertext)}/{total} last={last_err}", file=sys.stderr, flush=True)
        return None
    if not ciphertext:
        print(f"MEDIA_ERROR:empty download last={last_err}", file=sys.stderr, flush=True)
        return None
    if total is None and last_err != "ok":
        # No length info and the last read was cut short (or the budget
        # ran out mid-download): a partial ciphertext would only fail
        # PKCS7 unpadding or, worse, decrypt to a truncated file.
        print(f"MEDIA_ERROR:incomplete download last={last_err}", file=sys.stderr, flush=True)
        return None
    plaintext = _aes_ecb_decrypt(ciphertext, aes_key)
    ext = {2: "jpg", 3: "silk", 4: "bin", 5: "mp4"}.get(item_type, "bin")
    if item_type == 4:
        fname = sub.get("file_name") or ""
        if "." in fname:
            ext = fname.rsplit(".", 1)[-1][:8]
    os.makedirs(MEDIA_DIR, exist_ok=True)
    path = os.path.join(MEDIA_DIR, f"{int(time.time())}_{random.randint(0, 0xFFFFFF):06x}.{ext}")
    with open(path, "wb") as f:
        f.write(plaintext)
    os.chmod(path, 0o600)
    return path


MEDIA_PRUNE_MAX_AGE_DAYS = 30  # align with QUOTE_CACHE_TTL: a quoted
# original expires from the cache after the same window
MEDIA_PRUNE_MAX_TOTAL_BYTES = 500 * 1024 * 1024


def _quote_referenced_media_paths():
    """(absolute paths, basenames) still referenced by the quote cache.
    Pruning must never delete a file a live cache entry points at."""
    try:
        cache = _load_quote_cache()
    except Exception:
        return set(), set()
    paths = set()
    for ent in cache.get("by_id", {}).values():
        p = (ent or {}).get("media_path")
        if p:
            paths.add(os.path.abspath(p))
    return paths, {os.path.basename(p) for p in paths}


def cmd_prune_media(max_age_days=MEDIA_PRUNE_MAX_AGE_DAYS,
                    max_total_bytes=MEDIA_PRUNE_MAX_TOTAL_BYTES,
                    dry_run=False):
    """Delete downloaded media older than max_age_days, then oldest-first
    down to max_total_bytes. Files still referenced by the quote cache
    are exempt from both passes. Prints PRUNE_OK (or PRUNE_DRY_RUN)
    deleted=N freed=BYTES skipped=M. A single unreadable/unremovable
    file is skipped, never fatal."""
    now = int(time.time())
    cutoff = now - max_age_days * 86400
    ref_paths, ref_names = _quote_referenced_media_paths()
    try:
        names = os.listdir(MEDIA_DIR)
    except FileNotFoundError:
        print("PRUNE_OK deleted=0 freed=0 skipped=0")
        return 0
    files = []
    skipped = 0
    for name in names:
        path = os.path.join(MEDIA_DIR, name)
        try:
            st = os.stat(path)
        except OSError:
            skipped += 1
            continue
        if not os.path.isfile(path):
            continue
        files.append({"path": path, "mtime": int(st.st_mtime),
                      "size": st.st_size})

    def _exempt(f):
        return (os.path.abspath(f["path"]) in ref_paths
                or os.path.basename(f["path"]) in ref_names)

    def _try_remove(f):
        if dry_run:
            return True
        try:
            os.remove(f["path"])
        except OSError:
            return False
        return True

    deleted = 0
    freed = 0
    kept = []
    for f in files:
        if f["mtime"] < cutoff and not _exempt(f):
            if _try_remove(f):
                deleted += 1
                freed += f["size"]
            else:
                skipped += 1
                kept.append(f)
        else:
            kept.append(f)
    total = sum(f["size"] for f in kept)
    if total > max_total_bytes:
        for f in sorted(kept, key=lambda x: x["mtime"]):
            if total <= max_total_bytes:
                break
            if _exempt(f):
                continue
            if _try_remove(f):
                deleted += 1
                freed += f["size"]
                total -= f["size"]
            else:
                skipped += 1
    print(f"{'PRUNE_DRY_RUN' if dry_run else 'PRUNE_OK'} "
          f"deleted={deleted} freed={freed} skipped={skipped}")
    return 0


# --- Version check ----------------------------------------------------------
# A deployed bridge notifies its own owner when the repo publishes a new
# release. Release discipline: bump VERSION here BEFORE tagging the
# release — a forgotten bump makes every deployment report itself
# outdated forever, since the check compares this constant with the
# latest release tag.
VERSION = "1.3.2"
VERSION_CHECK_URL = ("https://api.github.com/repos/gfydfhcf/"
                     "wechat-ilink-muse-bridge/releases/latest")
VERSION_NOTIFY_FILE = os.path.join(BRIDGE, "version_notify.json")


def _parse_version(s):
    """'v1.2.0' -> (1, 2, 0). Unparseable segments become 0; missing
    parts pad to three, so '1.2' compares equal to '1.2.0'."""
    s = (s or "").strip().lstrip("vV")
    parts = []
    for seg in s.split(".")[:3]:
        try:
            parts.append(int(seg))
        except ValueError:
            parts.append(0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


def _compare_versions(a, b):
    """-1 if a<b, 0 if equal, 1 if a>b (numeric segments only)."""
    pa, pb = _parse_version(a), _parse_version(b)
    return (pa > pb) - (pa < pb)


def _fetch_latest_release():
    """Return the latest release dict (tag_name, name, body, html_url),
    or None on any failure (never raises: a background check must not
    crash the caller)."""
    try:
        req = urllib.request.Request(
            VERSION_CHECK_URL,
            headers={"User-Agent": "wechat-ilink-muse-bridge"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.load(resp)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _update_notify_text(tag, rel):
    """Notification text for a new release: version, a short excerpt of
    the release notes (mechanical truncation to 800 chars, not a
    curated summary), the release page link, and the install/postpone
    question."""
    body = (rel.get("body") or "").strip()
    excerpt = body[:800]
    if len(body) > 800:
        excerpt += "……"
    lines = [f"微信桥有新版了：{tag}（当前 {VERSION}）。"]
    if excerpt:
        lines.append(f"【更新内容】{excerpt}")
    else:
        lines.append("【更新内容】（本次无更新说明）")
    url = rel.get("html_url") or (
        "https://github.com/gfydfhcf/wechat-ilink-muse-bridge"
        f"/releases/tag/{tag}")
    lines.append(f"详情：{url}")
    lines.append("回「安装更新」我就在后台装好告诉你，回「暂时别了」就先不装。")
    return "\n".join(lines)


def _load_version_notify():
    """Load version_notify.json: {"subscribed": [owner_ids],
    "notified": {owner_id: tag}}. Missing or corrupt reads as empty.
    NOTE: the pre-subscription shape {"last_notified_tag": ...} is
    deliberately ignored — subscriptions are opt-in from scratch, so
    every owner gets at most one notice per tag after subscribing."""
    try:
        with open(VERSION_NOTIFY_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _atomic_write(path, writer, mode=0o600):
    """Atomically replace path with writer(f) output.

    The tmp name carries pid + thread id: two writers used to share
    one fixed "<target>.tmp" name, so one's half-written tmp could be
    renamed over the other's finished write, or vanish under it and
    break os.replace with FileNotFoundError. Pid alone is not enough
    — threads of one process share a pid and still collide (caught by
    the concurrent-write regression test). With a unique tmp name
    each replace is still atomic, and the worst case degrades to
    last-writer-wins (acceptable for these state files) instead of a
    torn or missing file. mode=None skips chmod (for files that
    deliberately keep the default mode)."""
    tmp = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    with open(tmp, "w") as f:
        writer(f)
    if mode is not None:
        os.chmod(tmp, mode)
    os.replace(tmp, path)


def _save_version_notify(state):
    try:
        _atomic_write(VERSION_NOTIFY_FILE, lambda f: json.dump(state, f))
    except Exception:
        pass


SUBSCRIBE_ASK_FILE = os.path.join(BRIDGE, "subscribe_ask.json")
SUBSCRIBE_ASK_TTL = 86400


def _write_subscribe_ask(owner_id):
    """Open the subscribe-question window after the welcome is
    delivered. Best-effort: a failed write just means the worker has
    no window to interpret a bare 'yes' against."""
    try:
        _save_json_atomic(SUBSCRIBE_ASK_FILE,
                          {"owner_id": owner_id,
                           "asked_at": int(time.time())})
    except Exception:
        pass


def _clear_subscribe_ask():
    try:
        os.remove(SUBSCRIBE_ASK_FILE)
    except OSError:
        pass


def cmd_subscribe(owner_id):
    """Opt an owner into release reminders. Idempotent, and clears any
    pending subscribe question (the decision has been made)."""
    state = _load_version_notify()
    subscribed = state.get("subscribed") or []
    if owner_id not in subscribed:
        subscribed.append(owner_id)
    state["subscribed"] = subscribed
    _save_version_notify(state)
    _clear_subscribe_ask()
    print(f"SUBSCRIBED:{owner_id}")
    return 0


def cmd_unsubscribe(owner_id):
    """Opt an owner out of release reminders. Idempotent, and clears
    any pending subscribe question."""
    state = _load_version_notify()
    state["subscribed"] = [o for o in (state.get("subscribed") or [])
                           if o != owner_id]
    _save_version_notify(state)
    _clear_subscribe_ask()
    print(f"UNSUBSCRIBED:{owner_id}")
    return 0


def cmd_version_check(notify=False):
    """Compare local VERSION with the upstream latest release tag.
    Prints UP_TO_DATE, UPDATE_AVAILABLE:<tag>, or
    VERSION_CHECK_FAILED. With --notify and an update available, sends
    one proactive notification to each SUBSCRIBED owner who has a
    cached context_token and was not already notified for this tag.
    Unsubscribed owners are never notified. A failed send is not
    recorded, so the next check retries it (an offline bot's notice is
    deferred, not lost). With --notify, the message also carries a
    release-notes excerpt and the install question, and a successful
    send writes an update_offer marker (tag, owners, time) that the
    worker uses to recognize an affirmative reply as 'install it'."""
    rel = _fetch_latest_release()
    tag = (rel or {}).get("tag_name")
    if not tag:
        print("VERSION_CHECK_FAILED")
        return 1
    if _compare_versions(VERSION, tag) >= 0:
        print("UP_TO_DATE")
        return 0
    print(f"UPDATE_AVAILABLE:{tag}")
    if not notify:
        return 0
    return _version_notify_locked(tag, rel)


VERSION_NOTIFY_LOCK_FILE = os.path.join(BRIDGE, "version_notify.lock")


def _do_version_notify(tag, rel):
    """Send one proactive update notice to each subscribed owner not
    yet notified for this tag. Callers must hold the version-notify
    lock across this call (see _version_notify_locked): the
    load → send → save sequence is only once-per-owner-per-tag when
    two concurrent runs cannot both observe the same pending set."""
    state = _load_version_notify()
    subscribed = state.get("subscribed") or []
    notified = state.get("notified") or {}
    if not subscribed:
        print("NO_SUBSCRIBERS")
        return 0  # nobody to notify: quiet success, not a cron failure
    pending = [o for o in subscribed if notified.get(o) != tag]
    if not pending:
        return 0  # everyone already notified for this tag: quiet success
    contacts = load_contacts()
    reachable = [o for o in pending
                 if (contacts.get(o) or {}).get("context_token")]
    if not reachable:
        print("NO_CONTEXT_TOKEN")
        return 1
    sent_any = False
    last_code = 0
    sent_owners = []
    text = _update_notify_text(tag, rel)
    for owner in reachable:
        code = cmd_proactive(owner, text)
        if code == 0:
            notified[owner] = tag
            sent_any = True
            sent_owners.append(owner)
        else:
            last_code = code
    state["subscribed"] = subscribed
    state["notified"] = notified
    state["checked_at"] = int(time.time())
    _save_version_notify(state)
    if sent_any:
        _write_update_offer(tag, sent_owners)
    if not sent_any:
        return last_code or 1
    return 0


def _version_notify_locked(tag, rel):
    """Run _do_version_notify under an inter-process flock so two
    concurrent version-check runs cannot double-send. Falls back to
    unlocked on platforms without fcntl (same policy as the quote
    cache lock)."""
    fd = None
    if fcntl is not None:
        try:
            fd = os.open(VERSION_NOTIFY_LOCK_FILE,
                         os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
        except Exception:
            fd = None
    try:
        return _do_version_notify(tag, rel)
    finally:
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


UPDATE_OFFER_FILE = os.path.join(BRIDGE, "update_offer.json")
UPDATE_OFFER_TTL = 86400


def _write_update_offer(tag, owners):
    """Record that these owners were offered this tag, so the worker
    can recognize a fresh affirmative reply as 'install it'.
    Best-effort: a failed write just means no recognition window."""
    try:
        _save_json_atomic(UPDATE_OFFER_FILE,
                          {"tag": tag, "owners": list(owners),
                           "offered_at": int(time.time())})
    except Exception:
        pass


def _clear_update_offer():
    try:
        os.remove(UPDATE_OFFER_FILE)
    except OSError:
        pass


# --- Self-update --------------------------------------------------------------
# update-to installs a RELEASE tag (never main, never an arbitrary URL):
# ilink.py always; hook scripts when --hook-dir names the deployed hook
# directory and the release changed them; AGENTS.md is NEVER applied
# automatically — a changed contract is downloaded to
# BRIDGE/AGENTS.md.new for manual sync, and the post-update message must
# say so (see the worker contract in AGENTS.md). Two phases: download +
# verify everything first, then swap everything (backups kept as
# .bak-<tag>). Any verification failure replaces nothing. State files
# (session, owner, contacts, ledgers, caches) are never touched.
# A deployment's local BRIDGE override is preserved across the swap
# (the release file always carries the repo default); see
# _preserve_bridge.
UPDATE_RAW_BASE = ("https://raw.githubusercontent.com/gfydfhcf/"
                   "wechat-ilink-muse-bridge")
UPDATE_COMPARE_URL = ("https://api.github.com/repos/gfydfhcf/"
                      "wechat-ilink-muse-bridge/compare")
UPDATE_RELEASE_URL = ("https://api.github.com/repos/gfydfhcf/"
                      "wechat-ilink-muse-bridge/releases/tags")


def _release_ilink_sha256(head):
    """Return the ilink.py sha256 published in the release notes for
    <head>, or None when the notes carry none. The notes are the only
    channel: the compare API lists changed files but no hashes. Only
    a line like 'ilink.py sha256: <64 hex>' counts; anything else
    means 'no hash published' (warn, don't fail)."""
    rel = _github_get_json(f"{UPDATE_RELEASE_URL}/{head}")
    body = (rel or {}).get("body") or ""
    m = re.search(r"ilink\.py\s+sha256:\s*([0-9a-fA-F]{64})", body)
    if m:
        return m.group(1).lower()
    return None
HOOK_SCRIPT_FILES = ("ilink-watch.sh", "ilink-warm-wait.sh")
# Watchdog default: the hook runs every ~5s, so a heartbeat older than
# 30 minutes means ~360 missed runs — a near-zero false-positive
# signal, checked by a 15-minute cron.
WATCHDOG_MAX_AGE_MINS = 30


def _github_get_json(url):
    """GET a GitHub URL and parse JSON, or None on any failure."""
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "wechat-ilink-muse-bridge"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)
    except Exception:
        return None


def _github_get_text(url):
    """GET a URL as text, or None on any failure."""
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "wechat-ilink-muse-bridge"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return None


def _extract_version(source):
    """VERSION = "..." from ilink.py source, or None."""
    m = re.search(r"^VERSION\s*=\s*\"([^\"]+)\"", source, re.MULTILINE)
    return m.group(1) if m else None


def _verify_python_source(source):
    """Compile check only (no execution). Returns error string or None."""
    try:
        compile(source, "<downloaded>", "exec")
    except Exception as e:
        return str(e)
    return None


def _verify_shell_source(source):
    """bash -n check via a temp file. Returns error string or None."""
    import subprocess
    import tempfile
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".sh",
                                         delete=False) as f:
            f.write(source)
            tmp = f.name
        try:
            r = subprocess.run(["bash", "-n", tmp],
                               capture_output=True, timeout=30)
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass
        if r.returncode != 0:
            return (r.stderr.decode("utf-8", "replace").strip()
                    or "bash -n failed")
    except Exception as e:
        return str(e)
    return None


def _bridge_block(source):
    """Top-level BRIDGE assignment in <source> as (start, end, text),
    start/end 1-based line numbers (end exclusive), or None when the
    source has no such assignment or does not parse. Found with ast
    so both the single-line production override and the repo's
    multi-line default are extracted exactly."""
    import ast
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "BRIDGE":
                    lines = source.splitlines(keepends=True)
                    start = node.lineno
                    end = (node.end_lineno or node.lineno) + 1
                    return (start, end,
                            "".join(lines[start - 1:end - 1]))
    return None


def _preserve_bridge(old_source, new_source):
    """Return (content, preserved): <new_source> with its BRIDGE
    assignment replaced by the old file's, when the old file carries
    a BRIDGE assignment that differs from the new file's. A missing
    or unparseable BRIDGE in either file, or identical blocks, means
    'nothing to preserve' — the new file installs as-is. This keeps
    a deployment's local BRIDGE override (e.g. a production path
    that differs from the repo default) across updates; without it
    update-to would silently install the repo default and the bridge
    would look for its session in the wrong directory. A deployment
    that never customized BRIDGE is unaffected. (If the repo itself
    ever changes its BRIDGE default, a deployment keeps its old
    value until the operator edits it — still a working BRIDGE.)"""
    old = _bridge_block(old_source)
    new = _bridge_block(new_source)
    if old is None or new is None:
        return new_source, False
    if old[2] == new[2]:
        return new_source, False
    old_text = old[2]
    if not old_text.endswith("\n"):
        old_text += "\n"
    lines = new_source.splitlines(keepends=True)
    start, end = new[0], new[1]
    return ("".join(lines[:start - 1]) + old_text
            + "".join(lines[end - 1:])), True


def _copy_file(src, dst):
    with open(src, "rb") as f_in, open(dst, "wb") as f_out:
        f_out.write(f_in.read())


def cmd_update_to(tag, hook_dir=None, script_path=None):
    """Install release <tag>. Prints UPDATE_OK:<tag> plus per-file
    lines, or UPDATE_FAILED:<reason> (with ROLLED_BACK when a post-swap
    failure restored the backups). UP_TO_DATE when already on the tag;
    refuses older tags (never downgrade) and tags with '/' or '..'.
    script_path defaults to the running script file (tests pass a temp
    path so the repo file is never touched)."""
    tag = (tag or "").strip()
    if not tag or "/" in tag or ".." in tag:
        print("UPDATE_FAILED:bad_tag")
        return 1
    cmp = _compare_versions(VERSION, tag)
    if cmp >= 0:
        if cmp == 0:
            print("UP_TO_DATE")
            return 0
        print("UPDATE_FAILED:not_newer")
        return 1
    if script_path is None:
        script_path = os.path.abspath(__file__)
    head = tag if tag[:1] in ("v", "V") else "v" + tag
    base = VERSION if VERSION[:1] in ("v", "V") else "v" + VERSION
    compare = _github_get_json(f"{UPDATE_COMPARE_URL}/{base}...{head}")
    changed = set()
    compare_ok = False
    if isinstance(compare, dict) and isinstance(compare.get("files"), list):
        compare_ok = True
        for f in compare["files"]:
            name = f.get("filename")
            if name:
                changed.add(name)
    # Phase 1: download + verify everything.
    plan = []  # (kind, target_path, content)
    src = _github_get_text(f"{UPDATE_RAW_BASE}/{head}/ilink.py")
    if src is None:
        print("UPDATE_FAILED:download")
        return 1
    err = _verify_python_source(src)
    if err:
        print(f"UPDATE_FAILED:verify:ilink.py:{err[:200]}")
        return 1
    file_version = _extract_version(src)
    if file_version is None or _compare_versions(file_version, tag) != 0:
        # Also enforces release discipline: a tag whose ilink.py was
        # not bumped to the tag refuses to install.
        print("UPDATE_FAILED:verify:version_mismatch")
        return 1
    published_hash = _release_ilink_sha256(head)
    if published_hash:
        import hashlib
        actual_hash = hashlib.sha256(src.encode("utf-8")).hexdigest()
        if actual_hash != published_hash:
            print("UPDATE_FAILED:verify:hash_mismatch")
            return 1
    else:
        # Optional check by design: releases that do not publish a
        # hash install as before, but loudly, so a missing hash can
        # never pass silently.
        print(f"WARNING: no ilink.py sha256 in release notes for "
              f"{head}; installing without hash check",
              file=sys.stderr, flush=True)
    # Preserve a deployment's local BRIDGE override (see
    # _preserve_bridge): the release file always carries the repo
    # default, and installing it verbatim would move the bridge's
    # data directory. The hash above was checked on the pristine
    # download, so re-verify the spliced source before planning it.
    preserved_bridge = False
    try:
        with open(script_path) as f:
            old_src = f.read()
    except OSError:
        old_src = None
    if old_src is not None:
        src, preserved_bridge = _preserve_bridge(old_src, src)
        if preserved_bridge:
            err = _verify_python_source(src)
            if err:
                print(f"UPDATE_FAILED:verify:ilink.py:{err[:200]}")
                return 1
    plan.append(("ilink.py", script_path, src))
    script_lines = []
    if hook_dir and not compare_ok:
        script_lines.append("SKIPPED:hook-scripts:compare-unavailable")
    elif hook_dir:
        for name in HOOK_SCRIPT_FILES:
            target = os.path.join(hook_dir, name)
            repo_path = f"examples/muse-hook/{name}"
            if repo_path not in changed:
                script_lines.append(f"SKIPPED:{name}:unchanged")
                continue
            if not os.path.isfile(target):
                script_lines.append(f"SKIPPED:{name}:missing")
                continue
            content = _github_get_text(
                f"{UPDATE_RAW_BASE}/{head}/{repo_path}")
            if content is None:
                print("UPDATE_FAILED:download")
                return 1
            err = _verify_shell_source(content)
            if err:
                print(f"UPDATE_FAILED:verify:{name}:{err[:200]}")
                return 1
            plan.append((name, target, content))
    elif compare_ok and any(f"examples/muse-hook/{n}" in changed
                            for n in HOOK_SCRIPT_FILES):
        script_lines.append("SKIPPED:hook-scripts:no-hook-dir")
    contract_line = None
    if compare_ok and "AGENTS.md" in changed:
        content = _github_get_text(f"{UPDATE_RAW_BASE}/{head}/AGENTS.md")
        if content:
            plan.append(("AGENTS.md.new",
                         os.path.join(BRIDGE, "AGENTS.md.new"), content))
            contract_line = "CONTRACT:AGENTS.md.new"
        else:
            # Loud but non-fatal: the contract changed but its new
            # text could not be fetched (a transient failure must not
            # block an otherwise good update), yet the worker must
            # not walk away believing the contract is unchanged.
            print("WARNING: AGENTS.md changed in this release but its "
                  "download failed; contract NOT updated — sync "
                  "AGENTS.md manually", file=sys.stderr, flush=True)
            contract_line = "CONTRACT:AGENTS.md:download-failed"
    # Phase 2: backup + atomic swap, then re-verify from disk.
    backed = []  # (target, backup_path or None)
    try:
        for kind, target, content in plan:
            backup = None
            if os.path.exists(target):
                backup = target + f".bak-{head}"
                _copy_file(target, backup)
            backed.append((target, backup))
            tmp = target + f".tmp-update-{os.getpid()}"
            try:
                mode = os.stat(target).st_mode & 0o777
            except OSError:
                mode = 0o644
            with open(tmp, "w") as f:
                f.write(content)
            os.chmod(tmp, mode)
            os.replace(tmp, target)
        with open(script_path) as f:
            err = _verify_python_source(f.read())
        if err:
            raise RuntimeError(f"post-verify ilink.py: {err}")
        for kind, target, content in plan:
            if kind in HOOK_SCRIPT_FILES:
                with open(target) as f:
                    err = _verify_shell_source(f.read())
                if err:
                    raise RuntimeError(f"post-verify {kind}: {err}")
    except Exception as e:
        for target, backup in backed:
            try:
                if backup:
                    os.replace(backup, target)
                else:
                    os.remove(target)
            except OSError:
                pass
        print(f"UPDATE_FAILED:swap:{e}")
        print("ROLLED_BACK")
        return 1
    _clear_update_offer()
    print(f"UPDATE_OK:{head}")
    for kind, target, content in plan:
        if kind == "AGENTS.md.new":
            continue
        print(f"UPDATED:{kind}")
    if preserved_bridge:
        print("PRESERVED:BRIDGE")
    for line in script_lines:
        print(line)
    if contract_line:
        print(contract_line)
    if not compare_ok:
        print("COMPARE:unavailable")
    return 0


def load_contacts():
    if os.path.exists(CONTACTS_FILE):
        try:
            with open(CONTACTS_FILE) as f:
                data = json.load(f)
        except Exception as e:
            # Corrupt cache must not crash the whole command: warn on
            # stderr (poll's stdout is machine-readable JSON) and fall
            # back to an empty cache; the next poll rebuilds it.
            print(f"WARNING: corrupt contacts file {CONTACTS_FILE}: {e}; "
                  "treating as empty", file=sys.stderr, flush=True)
            return {}
        return data if isinstance(data, dict) else {}
    return {}


def save_contacts(c):
    os.makedirs(BRIDGE, exist_ok=True)
    _atomic_write(CONTACTS_FILE, lambda f: json.dump(c, f))


@contextlib.contextmanager
def _contacts_lock():
    """Inter-process flock for contacts.json read-modify-write. Several
    processes write it — poll (context_token on every inbound message),
    typing (typing_ticket, from the worker while the hook keeps
    polling), bind — and unlocked load -> modify -> save let one writer
    replace the other's fresh fields: a typing call holding an old
    snapshot could roll the owner's context_token back. Best-effort
    like the quote cache lock: if the lock cannot be taken, proceed
    unlocked rather than block a poll or a reply."""
    fd = None
    if fcntl is not None:
        try:
            fd = os.open(CONTACTS_FILE + ".lock", os.O_CREAT | os.O_RDWR,
                         0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
        except Exception:
            if fd is not None:
                os.close(fd)
            fd = None
    try:
        yield
    finally:
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


def update_contacts(mutate):
    """Locked read-modify-write of contacts.json: reload under the lock,
    let mutate(contacts) change it in place, save. Returns the saved
    dict. Always use this instead of a bare load_contacts() ...
    save_contacts() pair, which can lose a concurrent writer's update."""
    with _contacts_lock():
        contacts = load_contacts()
        mutate(contacts)
        save_contacts(contacts)
        return contacts


# --- Quote cache ------------------------------------------------------------
# Why this exists: when the user quotes a message, the server's ref_msg
# carries only the quoted message's id (full quotes) or id plus a
# partial_text fragment (selective quotes) — never the original text.
# Restoring the original therefore needs a local id -> text cache.
# Sources: (1) poll records every inbound owner message and every bot
# message echoed back in getupdates; (2) send/proactive/media record
# their server message_id on confirmed success, so a reply is quotable
# even before the next poll echoes it back.
#
# Deliberately NOT fail-closed like the send ledger: a lost or corrupt
# cache only costs a future quote resolution (the worker sees the new
# text without its quoted original) — it can never cause a duplicate
# send — so a corrupt cache degrades to empty with a stderr warning
# instead of aborting poll or send.

def _load_quote_cache():
    """Return {"by_id": {...}, "by_svr": {...}}. Missing or corrupt
    files read as empty (see above for why this degrades instead of
    failing closed)."""
    try:
        with open(QUOTE_CACHE_FILE) as f:
            data = json.load(f)
    except FileNotFoundError:
        return {"by_id": {}, "by_svr": {}}
    except (OSError, ValueError) as e:
        print(f"WARNING: corrupt quote cache {QUOTE_CACHE_FILE}: {e}; "
              f"treating as empty", file=sys.stderr, flush=True)
        return {"by_id": {}, "by_svr": {}}
    if not isinstance(data, dict):
        print(f"WARNING: quote cache {QUOTE_CACHE_FILE} is not an object; "
              f"treating as empty", file=sys.stderr, flush=True)
        return {"by_id": {}, "by_svr": {}}
    by_id = data.get("by_id") if isinstance(data.get("by_id"), dict) else {}
    by_svr = data.get("by_svr") if isinstance(data.get("by_svr"), dict) else {}
    return {"by_id": by_id, "by_svr": by_svr}


def _trim_quote_cache(cache):
    """Drop expired entries, then oldest-first down to the cap, then
    drop svr mappings that no longer point at a live entry."""
    now = int(time.time())
    by_id = cache["by_id"]
    for mid in [m for m, e in by_id.items()
                if now - int(e.get("at", 0) or 0) > QUOTE_CACHE_TTL]:
        del by_id[mid]
    if len(by_id) > QUOTE_CACHE_CAP:
        ordered = sorted(by_id.items(), key=lambda kv: kv[1].get("at", 0))
        for mid, _ in ordered[:len(by_id) - QUOTE_CACHE_CAP]:
            del by_id[mid]
    cache["by_svr"] = {s: m for s, m in cache["by_svr"].items()
                       if m in by_id}


def _save_quote_cache(cache):
    _trim_quote_cache(cache)
    _atomic_write(QUOTE_CACHE_FILE,
                  lambda f: json.dump(cache, f, ensure_ascii=False))


def _quote_cache_upsert(entries, svr_map=None):
    """Merge entries into the quote cache. Best-effort: never raises,
    so a cache failure cannot change a poll/send outcome. entries maps
    str(msg_id) -> {text, kind, from, at, media_path?}; svr_map maps
    str(svr_id) -> str(msg_id), learned when a ref carries both."""
    if not entries and not svr_map:
        return
    try:
        fd = None
        if fcntl is not None:
            try:
                fd = os.open(QUOTE_CACHE_LOCK_FILE,
                             os.O_CREAT | os.O_RDWR, 0o600)
                fcntl.flock(fd, fcntl.LOCK_EX)
            except Exception:
                # Proceed unlocked: a lost cache entry is harmless, and
                # blocking poll/send on a lock would be worse.
                fd = None
        try:
            cache = _load_quote_cache()
            cache["by_id"].update(entries)
            if svr_map:
                cache["by_svr"].update(svr_map)
            _save_quote_cache(cache)
        finally:
            if fd is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
    except Exception as e:
        print(f"WARNING: quote cache upsert failed: {e}",
              file=sys.stderr, flush=True)


def _quote_media_descriptor(t, item):
    if t == 2:
        return "[图片]"
    if t == 4:
        name = (item.get("file_item") or {}).get("file_name", "")
        return f"[文件：{name}]" if name else "[文件]"
    if t == 5:
        return "[视频]"
    return ""


def _quote_resolve(loaded, pending_entries, pending_svr, ref_msg):
    """Resolve a raw ref_msg against the loaded cache plus entries cached
    earlier in this same poll batch. Returns (entry_or_None,
    fragment_or_None). Full quotes carry only the quoted msg_id;
    selective quotes also carry partial_text (start/end characters +
    md5). The captured start/end indices were both 0 and ambiguous, so
    matching uses the characters, not the indices — and the fragment is
    surfaced even on a cache miss, so a selective quote is at least
    partly readable without any cache hit."""
    if not isinstance(ref_msg, dict):
        return None, None
    ref_item = ref_msg.get("message_item") or {}
    target = ref_item.get("msg_id") or ref_msg.get("msg_id")
    if not target and ref_msg.get("svr_id"):
        svr = str(ref_msg["svr_id"])
        target = (pending_svr or {}).get(svr) or \
            (loaded.get("by_svr") or {}).get(svr)
    entry = None
    if target:
        entry = (pending_entries or {}).get(str(target)) or \
            (loaded.get("by_id") or {}).get(str(target))
    fragment = None
    partial = ref_msg.get("partial_text") or {}
    start, end = partial.get("start"), partial.get("end")
    if start or end:
        fragment = {"start": start, "end": end}
    return entry, fragment


# --- Inbound dedupe ---------------------------------------------------------
# getupdates occasionally redelivers the same message (e.g. a response lost
# after the cursor advanced). Processing one message_id twice means the
# user gets two replies. poll is the only inbound path, so filter here and
# both drivers (hook and daemon) are protected. An id is recorded as seen
# when poll returns it — the same semantics as the cursor advancing
# (delivered = returned by poll). Messages without a message_id cannot be
# deduped and pass through.
SEEN_CAP = 500  # keep the most recent 500 ids; older ones roll off

def load_seen():
    try:
        with open(SEEN_FILE) as f:
            ids = json.load(f)
        return ids if isinstance(ids, list) else []
    except Exception:
        return []


def save_seen(ids, keep=SEEN_CAP):
    """Persist seen ids, keeping at most `keep` (default SEEN_CAP).

    keep is max(SEEN_CAP, latest batch size) at the mark_seen call
    site: a single batch larger than the cap must not roll off its
    own early ids before the cursor is even written, or a failed
    cursor write followed by a redelivery would slip past the filter
    and duplicate replies."""
    os.makedirs(BRIDGE, exist_ok=True)
    _atomic_write(SEEN_FILE, lambda f: json.dump(ids[-keep:], f))


def filter_seen(raw_msgs):
    """Split raw msgs into (kept, new_ids) by message_id — READ ONLY.

    Ids are persisted by mark_seen() at commit time, after the caller
    has durably queued the batch. Recording them here would lose
    messages if the process died before queueing: the cursor would
    already be advanced and the id marked, so the server would never
    redeliver and a redelivery would be filtered out.
    """
    seen_set = set(load_seen())
    kept, new_ids = [], []
    for m in raw_msgs:
        mid = m.get("message_id")
        if mid is not None:
            mid = str(mid)
            if mid in seen_set:
                continue
            seen_set.add(mid)
            new_ids.append(mid)
        kept.append(m)
    return kept, new_ids


def mark_seen(ids):
    """Persist message ids as delivered (the commit step of polling)."""
    if not ids:
        return
    seen = load_seen()
    have = set(seen)
    merged = seen + [i for i in (str(x) for x in ids) if i not in have]
    if len(merged) != len(seen):
        save_seen(merged, keep=max(SEEN_CAP, len(ids)))


def load_owner_ids():
    """The owner allowlist: owner.json's `owner_ids` (several accounts of
    the same human are fine), falling back to the legacy single
    `owner_id`. A missing/empty owner.json yields an EMPTY set and poll
    then denies everyone — deny-by-default is deliberate: the bot holds
    the owner's memories and tools, so an unconfigured bridge must fail
    closed, not open."""
    try:
        with open(os.path.join(BRIDGE, "owner.json")) as f:
            data = json.load(f)
    except Exception:
        return set()
    ids = list(data.get("owner_ids") or [])
    if data.get("owner_id"):
        ids.append(data["owner_id"])
    return {str(i) for i in ids if i}


def note_unknown_sender(from_id):
    """Record a sender the allowlist turned away, in unknown_senders.json.

    This is the approval queue: the deployer reads the ids here and adds
    their own extra accounts to owner.json by hand. Only ids and counts
    are kept — never message content. Capped at the 100 most recent
    senders so a flood cannot grow the file without bound."""
    path = os.path.join(BRIDGE, "unknown_senders.json")
    try:
        with open(path) as f:
            book = json.load(f)
    except Exception:
        book = {}
    now = int(time.time())
    ent = book.get(from_id) or {"first_seen": now, "count": 0}
    ent["last_seen"] = now
    ent["count"] = ent.get("count", 0) + 1
    book[from_id] = ent
    if len(book) > 100:
        book = dict(sorted(book.items(),
                           key=lambda kv: kv[1].get("last_seen", 0))[-100:])
    _atomic_write(path, lambda f: json.dump(book, f))


# --- First-owner binding ----------------------------------------------------
# Binding is a dedicated setup command, NOT part of normal polling: regular
# poll stays deny-by-default forever. After QR login the deployer runs
# `ilink.py bind`, which opens ONE bounded window (default 600s). The first
# account to message the bot inside that window becomes the owner and gets
# the four-part welcome below. If the window lapses, bind exits with
# BIND_TIMEOUT (exit 3) so the deploying agent can remind the human and run
# bind again — an unattended, indefinitely open "first come wins" mode
# would let a stranger seize a bridge whose owner file was lost.
BIND_DEFAULT_SECONDS = 600
WELCOME_MESSAGES = [
    "欢迎使用 WeChat iLink Muse Bridge！",
    "绑定成功，当前微信账号已设为 Owner。今后只有 Owner 以及你手动批准的账号可以与我聊天。",
    "现在可以直接给我发送消息了。",
    "顺便问一句：要不要订阅本项目的更新提醒？有新版时我会在微信上告诉你。回复「订阅」就开通，随时回「取消订阅」就能关。",
]


def _owner_path():
    return os.path.join(BRIDGE, "owner.json")


def _bind_state_path():
    return os.path.join(BRIDGE, "bind_state.json")


def _bind_lock_path():
    return os.path.join(BRIDGE, "bind.lock")


def _save_json_atomic(path, data):
    _atomic_write(path, lambda f: json.dump(data, f, ensure_ascii=False))


def save_owner(owner_id):
    _save_json_atomic(_owner_path(),
                      {"owner_id": owner_id, "owner_ids": [owner_id]})


def load_bind_state():
    try:
        with open(_bind_state_path()) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_bind_state(state):
    _save_json_atomic(_bind_state_path(), state)


def clear_bind_state():
    try:
        os.remove(_bind_state_path())
    except FileNotFoundError:
        pass


def _bind_lock_token():
    return f"{os.getpid()}-{random.randint(0, 0xFFFFFFFF):08x}"


def _acquire_bind_lock():
    """Single-window lock: two bind commands must never race for the first
    sender. Returns (fd, token), or None when another window holds the
    lock. A lock older than 15 minutes is treated as stale ONLY when
    its owner pid is dead (or the lock file is unreadable) — a slow
    holder past 900s must not lose its window to a second bind. The
    token is verified on release, so a holder can never delete a lock
    it does not own."""
    path = _bind_lock_path()
    for _ in range(2):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            try:
                if time.time() - os.path.getmtime(path) > 900:
                    try:
                        with open(path) as f:
                            old = json.load(f)
                        pid = old.get("pid")
                    except Exception:
                        pid = None
                    alive = False
                    if pid:
                        try:
                            os.kill(pid, 0)
                            alive = True
                        except OSError:
                            alive = False
                    if not alive:
                        os.remove(path)
                        continue
            except FileNotFoundError:
                continue
            return None
        token = _bind_lock_token()
        os.write(fd, json.dumps(
            {"pid": os.getpid(), "token": token,
             "started_at": int(time.time())}).encode())
        return (fd, token)
    return None


def _release_bind_lock(lock):
    """Release a lock from _acquire_bind_lock: only remove the lock
    file when its token still matches (a stolen or replaced lock is
    left alone). Accepts the (fd, token) tuple, or a bare fd for
    backward compatibility (token check skipped)."""
    if isinstance(lock, tuple):
        fd, token = lock
    else:
        fd, token = lock, None
    try:
        os.close(fd)
    except Exception:
        pass
    try:
        if token is not None:
            with open(_bind_lock_path()) as f:
                current = json.load(f)
            if current.get("token") != token:
                return
        os.remove(_bind_lock_path())
    except (FileNotFoundError, ValueError, OSError):
        pass


def _send_welcome_segment(to_user_id, context_token, text, token, client_id):
    """Send one welcome bubble. Network failures retry once with the SAME
    client_id (same rule as cmd_send); application failures do not retry."""
    for attempt in (1, 2):
        try:
            data = _send_raw(to_user_id, context_token, text, token,
                             client_id=client_id)
        except Exception as e:
            if attempt == 1:
                time.sleep(1)
                continue
            print(f"BIND_SEND_ERROR:{e}", file=sys.stderr, flush=True)
            return False, None
        code = data.get("code", 0)
        if code == 0:
            return True, code
        return False, code
    return False, None


def _deliver_pending_welcome(token):
    """Send any unsent welcome bubbles for a claimed owner, then commit the
    binding batch. Progress is persisted after every bubble, so re-running
    bind after a crash resumes where it stopped instead of re-binding or
    re-sending from scratch."""
    state = load_bind_state()
    owner_id = state.get("owner_id")
    if not owner_id:
        print("BIND_STATE_INVALID")
        return 1
    ctx = state.get("context_token")
    if not ctx:
        print("BIND_NO_CONTEXT_TOKEN")
        return 1
    claim = str(state.get("claim_id") or state.get("claimed_at") or "bind")
    sent = int(state.get("welcome_sent") or 0)
    while sent < len(WELCOME_MESSAGES):
        client_id = f"muse-wechat-bind-{claim}-{sent}"
        ok, code = _send_welcome_segment(owner_id, ctx,
                                         WELCOME_MESSAGES[sent], token,
                                         client_id)
        if code == -14:
            print("TOKEN_EXPIRED")
            return 2
        if not ok:
            print(f"BIND_WELCOME_FAILED:{sent + 1}")
            return 1
        sent += 1
        state["welcome_sent"] = sent
        save_bind_state(state)
    commit = state.get("commit") or {}
    if commit.get("cursor"):
        s = load_session()
        s["cursor"] = commit["cursor"]
        save_session(s)
    mark_seen(commit.get("ids") or [])
    # The welcome is fully delivered: open the 24h subscribe-question
    # window. The worker interprets an affirmative owner reply inside
    # the window as opting in (see the worker contract in AGENTS.md);
    # the marker is inert after expiry and is cleared by any
    # subscribe/unsubscribe decision.
    _write_subscribe_ask(owner_id)
    clear_bind_state()
    print(f"BIND_OK:{owner_id}")
    return 0


def cmd_bind(wait_secs=BIND_DEFAULT_SECONDS):
    """Open one owner-binding window and wait for the first message.

    Exit codes: 0 bound (or already bound), 1 error / welcome pending,
    2 token expired, 3 window timed out (remind the human, run again),
    4 another bind window is already open."""
    owners = load_owner_ids()
    if owners and not load_bind_state():
        primary = sorted(owners)[0]
        print(f"ALREADY_BOUND:{primary}")
        return 0
    lock = _acquire_bind_lock()
    if lock is None:
        print("BIND_IN_PROGRESS")
        return 4
    try:
        # Re-check under the lock: another window may have just finished.
        owners = load_owner_ids()
        state = load_bind_state()
        s = load_session()
        token = s.get("bot_token")
        if not token:
            print("NO_TOKEN")
            return 1
        if state and state.get("owner_id") in owners:
            return _deliver_pending_welcome(token)
        if owners:
            clear_bind_state()
            print(f"ALREADY_BOUND:{sorted(owners)[0]}")
            return 0
        if state:
            clear_bind_state()  # stale claim whose owner file is gone

        wait_secs = max(0, int(wait_secs))
        print(f"BIND_WAITING:{wait_secs}", flush=True)
        deadline = time.time() + wait_secs
        buf = s.get("cursor", "")
        while time.time() < deadline:
            try:
                data = api("POST", "ilink/bot/getupdates",
                           {"get_updates_buf": buf,
                            "base_info": {"channel_version": "1.0.2"}},
                           token=token, timeout=40)
            except Exception as e:
                print(f"BIND_ERROR:{e}", file=sys.stderr, flush=True)
                time.sleep(min(2, max(0, deadline - time.time())))
                continue
            status = _api_error_code(data)
            if status == -14:
                print("TOKEN_EXPIRED")
                return 2
            if status != 0:
                # API-level failure: nothing usable in this response
                # (same rule as cmd_poll) — retry inside the window.
                print(f"BIND_ERROR:api status {status}", file=sys.stderr,
                      flush=True)
                time.sleep(min(2, max(0, deadline - time.time())))
                continue
            new_buf = data.get("get_updates_buf") or buf
            fresh, new_ids = filter_seen(data.get("msgs") or [])
            winner = None
            winner_ctx = None
            for m in fresh:
                if m.get("message_type") != 1:
                    continue
                sender = m.get("from_user_id")
                if not sender:
                    continue
                if winner is None:
                    winner = sender
                    winner_ctx = m.get("context_token")
                elif sender == winner:
                    winner_ctx = m.get("context_token") or winner_ctx
                else:
                    # Same batch, different sender: too late — the first
                    # sender wins; this one joins the approval queue.
                    note_unknown_sender(sender)
            if winner is None:
                # Nothing claimable in this batch: commit it and keep
                # waiting, so the normal driver never replays it later.
                if new_buf != buf or new_ids:
                    if new_buf:
                        s["cursor"] = new_buf
                        save_session(s)
                    mark_seen(new_ids)
                    buf = new_buf
                continue
            # Claim first, welcome second: once owner.json exists the
            # window is closed to everyone else, whatever happens next.
            save_owner(winner)
            if winner_ctx:
                def _set_winner(contacts):
                    contacts[winner] = {"context_token": winner_ctx,
                                        "updated_at": int(time.time())}
                update_contacts(_set_winner)
            source_id = next(
                (str(m.get("message_id")) for m in fresh
                 if m.get("from_user_id") == winner
                 and m.get("message_id") is not None),
                str(int(time.time())))
            save_bind_state({
                "owner_id": winner,
                "context_token": winner_ctx,
                "claim_id": source_id,
                "claimed_at": int(time.time()),
                "welcome_sent": 0,
                "commit": {"cursor": new_buf, "ids": new_ids},
            })
            return _deliver_pending_welcome(token)
        print("BIND_TIMEOUT")
        return 3
    finally:
        _release_bind_lock(lock)


def headers(token=None):
    uin = str(random.randint(0, 0xFFFFFFFF))
    h = {
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "X-WECHAT-UIN": base64.b64encode(uin.encode()).decode(),
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def api(method, path, body=None, token=None, timeout=40):
    url = f"{BASE_URL}/{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers(token), method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def load_session():
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE) as f:
                data = json.load(f)
        except Exception as e:
            # Same rule as load_contacts: a corrupt session file degrades
            # to "not logged in" (the caller's NO_TOKEN path) instead of
            # crashing the command with a traceback.
            print(f"WARNING: corrupt session file {SESSION_FILE}: {e}; "
                  "treating as empty", file=sys.stderr, flush=True)
            return {}
        return data if isinstance(data, dict) else {}
    return {}


def save_session(s):
    os.makedirs(BRIDGE, exist_ok=True)
    _atomic_write(SESSION_FILE, lambda f: json.dump(s, f))


def _fetch_qrcode():
    """Fetch a fresh QR code. Returns the raw get_bot_qrcode response."""
    return api("GET", "ilink/bot/get_bot_qrcode?bot_type=3")


def _print_qrcode(data):
    """Print a fetched QR code in cmd_qrcode's format. Returns the
    qrcode string, for the login loop's auto-refresh."""
    qrcode = data["qrcode"]
    img = data.get("qrcode_img_content", "")
    if img.startswith("data:image/"):
        header, b64 = img.split(",", 1)
        m = re.search(r"data:image/(\w+)", header)
        ext = m.group(1) if m else "png"
        path = os.path.join(BRIDGE, f"qrcode.{ext}")
        os.makedirs(BRIDGE, exist_ok=True)
        with open(path, "wb") as f:
            f.write(base64.b64decode(b64))
        print(f"QR_IMAGE:{path}")
    elif img.startswith("http"):
        print(f"QR_URL:{img}")
    print(f"QR_CODE:{qrcode}")
    return qrcode


def cmd_qrcode():
    _print_qrcode(_fetch_qrcode())


def _save_login_session(st):
    s = load_session()
    s["bot_token"] = st["bot_token"]
    s["baseurl"] = st.get("baseurl", "")
    s["login_at"] = int(time.time())
    save_session(s)


def _follow_login_redirect(redirect):
    """Follow one server-driven login redirect. Returns the parsed JSON
    dict, or {} when the follow fails or yields nothing parseable.
    Absolute URLs go direct; relative paths go through api()."""
    try:
        if redirect.startswith("http"):
            req = urllib.request.Request(redirect, method="GET")
            with urllib.request.urlopen(req, timeout=35) as resp:
                return json.loads(resp.read().decode())
        return api("GET", redirect, timeout=35)
    except Exception:
        return {}


def cmd_login(qrcode, wait_secs=300):
    """Wait for a QR scan to confirm. Beyond the old confirmed /
    expired / canceled / timeout handling, this also covers the
    branches the old loop could only time out or fail on (none of
    them has ever triggered in production, so all of this is
    defensive — the confirmed path is unchanged):
    - expired: auto-refresh the QR code (max 3 refreshes) and keep
      waiting instead of failing outright;
    - binded / binded_redirect: the account is already bound; with a
      bot_token present this is a success, not a failure;
    - redirect / redirect_url: follow it once per round, and accept a
      confirmed result from the followed response;
    - need_verify / verify_url: print the verification URL for the
      operator to complete in WeChat and keep polling; when the server
      explicitly asks for a typed code, read one line from stdin and
      resubmit it with the next status poll (best-effort: a server
      that ignores the parameter just keeps polling as before)."""
    deadline = time.time() + wait_secs
    refreshes = 0
    verify_code = None
    while time.time() < deadline:
        try:
            url = f"ilink/bot/get_qrcode_status?qrcode={qrcode}"
            if verify_code:
                url += f"&verify_code={verify_code}"
            st = api("GET", url, timeout=35)
        except Exception as e:
            print(f"STATUS_ERROR:{e}", flush=True)
            time.sleep(3)
            continue
        status = st.get("status")
        print(f"SCAN_STATUS:{status}", flush=True)
        if status == "confirmed":
            _save_login_session(st)
            print("LOGIN_OK")
            return 0
        if status == "binded" or st.get("binded_redirect"):
            if st.get("bot_token"):
                _save_login_session(st)
                print("LOGIN_OK")
                return 0
            print("LOGIN_FAILED:binded_no_token")
            return 1
        redirect = st.get("redirect") or st.get("redirect_url")
        if redirect:
            print(f"LOGIN_REDIRECT:{redirect}", flush=True)
            followed = _follow_login_redirect(redirect)
            if (isinstance(followed, dict)
                    and followed.get("status") == "confirmed"
                    and followed.get("bot_token")):
                _save_login_session(followed)
                print("LOGIN_OK")
                return 0
            time.sleep(2)
            continue
        if (status in ("need_verify", "verify", "verify_code")
                or st.get("verify_url") or st.get("need_verify_code")):
            if st.get("verify_url"):
                print(f"VERIFY_URL:{st['verify_url']}", flush=True)
            if st.get("need_verify_code") or status == "verify_code":
                try:
                    print("VERIFY_CODE_REQUIRED: type the code and press Enter",
                          flush=True)
                    code = sys.stdin.readline().strip()
                except Exception:
                    code = ""
                verify_code = code or None
            time.sleep(2)
            continue
        if status == "expired":
            if refreshes >= 3:
                print("LOGIN_FAILED:expired")
                return 1
            refreshes += 1
            try:
                data = _fetch_qrcode()
            except Exception as e:
                print(f"QR_REFRESH_ERROR:{e}", flush=True)
                return 1
            qrcode = _print_qrcode(data)
            # L14: keep the ORIGINAL deadline — refreshing the QR
            # must not silently stretch the wait (3 refreshes used to
            # turn 300s into 1200s). The loop exits on the original
            # deadline with LOGIN_TIMEOUT.
            continue
        if status == "canceled":
            print(f"LOGIN_FAILED:{status}")
            return 1
        time.sleep(2)
    print("LOGIN_TIMEOUT")
    return 1


# --- Token stale backoff ------------------------------------------------------
# The official session-guard treats code -14 as "stale", not "expired":
# pause, retry with the same token, and only declare expiry if -14
# persists. Our previous behavior alerted "rescan now" on the first -14,
# so a transient server-side hiccup would have sent the owner to
# re-scan for nothing. Now a -14 inside the window reports TOKEN_STALE
# (the hook stays silent and keeps polling); only a -14 streak longer
# than the window reports TOKEN_EXPIRED. Any successful call clears the
# marker, so a self-healed token starts a fresh window next time.
TOKEN_STALE_FILE = os.path.join(BRIDGE, "token_stale.json")
TOKEN_STALE_WINDOW = 3600


def _note_token_stale():
    """Record a -14 sighting. Returns seconds since the first -14 in
    the current streak (0 on the first sighting)."""
    now = int(time.time())
    try:
        with open(TOKEN_STALE_FILE) as f:
            first = int(json.load(f).get("first_seen", now))
    except Exception:
        first = now
    try:
        _atomic_write(TOKEN_STALE_FILE,
                      lambda f: json.dump({"first_seen": first}, f))
    except Exception:
        pass
    return now - first


def _clear_token_stale():
    """Drop the stale marker: a successful call proves the token is
    healthy again."""
    try:
        os.remove(TOKEN_STALE_FILE)
    except OSError:
        pass


def _report_token_status():
    """Map a -14 to its report: TOKEN_STALE (exit 3) inside the backoff
    window, TOKEN_EXPIRED (exit 2) once the window elapsed. Prints the
    marker and returns the exit code."""
    if _note_token_stale() < TOKEN_STALE_WINDOW:
        print("TOKEN_STALE")
        return 3
    _clear_token_stale()
    print("TOKEN_EXPIRED")
    return 2


def cmd_poll(cursor=None):
    """Fetch one batch (two-phase: nothing is committed here).

    cursor overrides the committed cursor from session.json for this
    one fetch (read-only: it is never saved). A driver that collects
    several polls before committing (daemon.py's burst merge) passes
    the previous poll's commit cursor, so each fetch continues where
    the last one stopped instead of re-reading from the committed
    cursor — which made the server replay the same batch instantly,
    turning the merge window into a hot loop that re-downloaded the
    batch's media on every round."""
    s = load_session()
    token = s.get("bot_token")
    if not token:
        print("NO_TOKEN")
        return 1
    buf = cursor if cursor else s.get("cursor", "")
    try:
        data = api("POST", "ilink/bot/getupdates",
                   {"get_updates_buf": buf, "base_info": {"channel_version": "1.0.2"}},
                   token=token, timeout=40)
    except Exception as e:
        print(f"POLL_ERROR:{e}")
        return 1
    status = _api_error_code(data)
    if status == -14:
        return _report_token_status()
    if status != 0:
        # An API-level failure (ret / errcode / code != 0) is a poll
        # failure, not an empty batch: printing an empty batch with
        # exit 0 used to look like "no new messages" to both drivers,
        # so a persistent server-side error never reached the
        # poll_failing alert. Nothing is fetched, nothing committed.
        errmsg = (str(data.get("errmsg", ""))[:100]
                  if isinstance(data, dict) else "")
        print(f"POLL_ERROR:api status {status} {errmsg}".rstrip())
        return 1
    _clear_token_stale()
    # One media budget for the whole round, started once the long-poll
    # returned: downloads used to run up to ~260s per item, past the
    # hook's 120s poll timeout — the round was killed before commit,
    # the same batch came back next round and was killed again (a
    # poison batch that stalled every later message). Items that do not
    # fit the budget are delivered with media_error instead.
    media_deadline = time.time() + POLL_MEDIA_BUDGET
    new_buf = data.get("get_updates_buf") or buf
    fresh, new_ids = filter_seen(data.get("msgs") or [])
    owners = load_owner_ids()
    filtered_senders = []
    msgs = []
    # L1: only write contacts when this round changed them. Updates are
    # collected here and merged under the contacts lock at the end, so a
    # concurrent writer (typing) can neither lose nor revert them.
    ctx_updates = {}
    quote_cache = None  # L2: lazy — loaded on the first ref_msg, not every round
    pending_entries = {}
    pending_svr = {}
    for m in fresh:
        if m.get("message_type") == 2:
            # The bot's own words echoed back: cache only, never queue.
            # No media download here: a descriptor is enough for a
            # future quote lookup, and the send path already records
            # the full text of bot messages it sent.
            parts = []
            kinds = []
            for item in m.get("item_list") or []:
                t = item.get("type") or 0
                if t == 1:
                    tx = (item.get("text_item") or {}).get("text", "")
                    if tx:
                        parts.append(tx)
                        kinds.append("text")
                elif t in (2, 4, 5):
                    parts.append(_quote_media_descriptor(t, item))
                    kinds.append(ITEM_TYPE_NAMES.get(t, "text"))
            if parts and m.get("message_id"):
                pending_entries[str(m["message_id"])] = {
                    "text": "\n".join(parts), "kind": kinds[0],
                    "from": "bot", "at": int(time.time())}
            continue
        if m.get("message_type") != 1:  # 只要用户发来的消息
            continue
        from_id = m.get("from_user_id")
        if from_id is not None:
            from_id = str(from_id)
        if from_id not in owners:
            # Owner allowlist (code-enforced): a stranger's message is
            # never queued, never wakes the worker and never lands in
            # the contacts cache. Its id stays in new_ids, so the commit
            # step still advances past it — the server will not redeliver
            # it and the queue cannot jam on it. The sender is noted in
            # unknown_senders.json for manual approval (e.g. the owner
            # messaging from a second account).
            if from_id:
                note_unknown_sender(from_id)
                filtered_senders.append(from_id)
            continue
        ctx = m.get("context_token")
        if from_id and ctx:
            # Merge, don't replace: the entry may also carry a cached
            # typing_ticket, which a whole-entry overwrite would drop
            # on every inbound message (forcing a getconfig re-fetch
            # exactly when typing is needed most).
            ctx_updates[from_id] = {"context_token": ctx,
                                    "updated_at": int(time.time())}
        cache_parts = []
        cache_kinds = []
        cache_media_path = None
        for item in m.get("item_list") or []:
            t = item.get("type") or 0
            kind = ITEM_TYPE_NAMES.get(t, f"unknown_{t}")
            entry = {"from_user_id": from_id, "context_token": ctx, "kind": kind,
                     "message_id": m.get("message_id")}
            if t == 1:  # TEXT
                entry["text"] = (item.get("text_item") or {}).get("text", "")
                if entry["text"]:
                    cache_parts.append(entry["text"])
                    cache_kinds.append("text")
            elif t == 3:  # VOICE: 可能带语音转写文本
                entry["text"] = (item.get("voice_item") or {}).get("text", "")
                entry["voice_duration_ms"] = (item.get("voice_item") or {}).get("playtime", 0)
                if entry["text"]:
                    cache_parts.append(entry["text"])
                    cache_kinds.append("voice")
                try:
                    p = download_media_item(item, t, deadline=media_deadline)
                    if p:
                        entry["media_path"] = p
                        cache_media_path = p
                    elif time.time() >= media_deadline:
                        entry["media_error"] = MEDIA_BUDGET_ERROR
                except Exception as e:
                    entry["media_error"] = str(e)[:100]
            elif t in (2, 4, 5):  # IMAGE / FILE / VIDEO
                if t == 4:
                    entry["file_name"] = (item.get("file_item") or {}).get("file_name", "")
                cache_parts.append(_quote_media_descriptor(t, item))
                cache_kinds.append(ITEM_TYPE_NAMES.get(t, "text"))
                try:
                    p = download_media_item(item, t, deadline=media_deadline)
                    if p:
                        entry["media_path"] = p
                        cache_media_path = p
                    elif time.time() >= media_deadline:
                        entry["media_error"] = MEDIA_BUDGET_ERROR
                    else:
                        entry["media_error"] = "download failed"
                except Exception as e:
                    entry["media_error"] = str(e)[:100]
            else:
                # Unknown item type: never drop silently. The worker
                # must at least see that something arrived; the id
                # still flows into the commit block, so the cursor
                # advances exactly as before — only the loss is fixed.
                entry["text"] = f"[未知消息类型 {t}]"
            ref_msg = item.get("ref_msg")
            if isinstance(ref_msg, dict) and ref_msg:
                ref_item = ref_msg.get("message_item") or {}
                if ref_item.get("msg_id"):
                    entry["ref_msg_id"] = str(ref_item["msg_id"])
                if ref_msg.get("svr_id"):
                    entry["ref_svr_id"] = str(ref_msg["svr_id"])
                    if ref_item.get("msg_id"):
                        # Learn the svr -> msg mapping for later quotes
                        # that carry only svr_id.
                        pending_svr[str(ref_msg["svr_id"])] = str(ref_item["msg_id"])
                if ref_msg.get("title"):
                    entry["quoted_title"] = ref_msg["title"]
                if quote_cache is None:
                    quote_cache = _load_quote_cache()
                resolved, fragment = _quote_resolve(
                    quote_cache, pending_entries, pending_svr, ref_msg)
                if resolved:
                    if resolved.get("text"):
                        entry["quoted_text"] = resolved["text"]
                    entry["quoted_kind"] = resolved.get("kind", "text")
                    if resolved.get("media_path"):
                        entry["quoted_media_path"] = resolved["media_path"]
                if fragment:
                    entry["quoted_fragment"] = fragment
            msgs.append(entry)
        if (cache_parts or cache_media_path) and m.get("message_id"):
            cache_entry = {"text": "\n".join(cache_parts),
                           "kind": cache_kinds[0] if cache_kinds else "text",
                           "from": "user", "at": int(time.time())}
            if cache_media_path:
                cache_entry["media_path"] = cache_media_path
            pending_entries[str(m["message_id"])] = cache_entry
    if pending_entries or pending_svr:
        _quote_cache_upsert(pending_entries, pending_svr)
    if ctx_updates:
        def _merge(contacts):
            for uid, fields in ctx_updates.items():
                ent = contacts.get(uid)
                if not isinstance(ent, dict):
                    ent = contacts[uid] = {}
                ent.update(fields)
        update_contacts(_merge)
    # Two-phase commit: do NOT advance the cursor or record seen ids
    # here. The caller (hook script / daemon) queues the batch durably,
    # then submits the commit block below via `ilink.py commit`. A crash
    # before that simply re-fetches the same batch next poll — messages
    # may duplicate, but they are never lost.
    print(json.dumps({"msgs": msgs,
                      "filtered": {"dropped": len(filtered_senders),
                                   "senders": sorted(set(filtered_senders))},
                      "commit": {"cursor": new_buf, "ids": new_ids}},
                     ensure_ascii=False))
    return 0


def cmd_commit():
    """Commit one poll round: record delivered ids + advance the cursor.
    Reads the commit block from stdin: {"cursor": ..., "ids": [...]}.
    Call only after the batch is durably queued. Idempotent.

    Order matters: seen ids are persisted BEFORE the cursor. If the
    process dies between the two writes, the un-advanced cursor makes
    the server redeliver the batch, and the recorded seen ids filter
    the redelivery out — a duplicate poll, not a duplicate reply. The
    old order (cursor first) could lose the seen write and let a
    redelivery through as a fresh message."""
    try:
        payload = json.load(sys.stdin)
    except Exception as e:
        print(f"COMMIT_ERROR:bad stdin: {e}")
        return 1
    if not isinstance(payload, dict):
        print("COMMIT_ERROR:payload must be an object")
        return 1
    ids = payload.get("ids") or []
    if not isinstance(ids, list):
        # A string id list would otherwise be recorded char by char
        # into seen_ids, polluting the dedupe filter.
        print("COMMIT_ERROR:ids must be a list")
        return 1
    cursor = payload.get("cursor")
    if cursor is not None and not isinstance(cursor, str):
        print("COMMIT_ERROR:cursor must be a string")
        return 1
    mark_seen(ids)
    if cursor:
        s = load_session()
        s["cursor"] = cursor
        save_session(s)
    print("COMMIT_OK")
    return 0


# --- Worker queue claim (hook driver) ----------------------------------------
# The hook lands each batch in <state>/ilink-pending.json; a worker takes
# it over as <state>/ilink-processing.json. The original protocol was a
# bare `mv pending processing`, which has two holes:
#   * mv silently REPLACES an existing processing file, so a second
#     worker (woken while the first was still composing) destroyed the
#     first worker's claim — crash recovery could no longer find that
#     batch, and the first worker's cleanup could delete the second's.
#   * nothing tied a claim to its worker: a slow worker whose claim was
#     recycled could still delete (or reply to) the batch now owned by
#     someone else.
# These commands replace the bare mv. All three run under one flock
# (<state>/ilink-queue.lock), which the hook also takes for stale
# recycling, so a claim, a merge and a recycle never interleave:
#   claim              pending -> processing, only if NO processing file
#                      exists (exit 3 CLAIM_BUSY otherwise: the holder
#                      merges newer messages itself). Stamps a claim_id.
#   claim --merge ID   pre-send re-check: append pending's messages to
#                      the caller's processing file (exit 3
#                      MERGE_NOT_OWNER when the claim was recycled —
#                      the caller must then NOT send).
#   release ID         delete processing, only if it still carries ID.
# ID is optional for backward compatibility (no ownership check then).
QUEUE_LOCK_NAME = "ilink-queue.lock"


@contextlib.contextmanager
def _queue_lock(state_dir):
    """flock on <state>/ilink-queue.lock (shared with the hook script's
    `flock` on the same file). No-op where fcntl is unavailable."""
    fd = None
    if fcntl is not None:
        os.makedirs(state_dir, exist_ok=True)
        fd = os.open(os.path.join(state_dir, QUEUE_LOCK_NAME),
                     os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        yield
    finally:
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


def _read_queue_file(path):
    """The queue object at path, or None when the file is missing or
    empty. Raises ValueError on a corrupt / misshapen queue (same strict
    shape rule as the hook: object with a .msgs array)."""
    try:
        with open(path) as f:
            raw = f.read()
    except FileNotFoundError:
        return None
    if not raw.strip():
        return None
    data = json.loads(raw)
    if not isinstance(data, dict) or not isinstance(data.get("msgs"), list):
        raise ValueError(f"misshapen queue file {path}")
    return data


def cmd_claim(merge=False, claim_id=None):
    """Claim or merge the pending batch (see the block comment above).
    Exit 0 on CLAIMED / MERGED / MERGE_NOTHING, 3 when there is nothing
    for this caller (CLAIM_BUSY, CLAIM_EMPTY, MERGE_NOT_OWNER), 1 on a
    corrupt queue file (left in place for the hook to quarantine)."""
    state_dir = _hook_state_dir()
    pend = os.path.join(state_dir, "ilink-pending.json")
    proc = os.path.join(state_dir, "ilink-processing.json")
    try:
        with _queue_lock(state_dir):
            if merge:
                cur = _read_queue_file(proc)
                if cur is None or (claim_id and cur.get("claim_id") != claim_id):
                    # The claim was recycled (stale) or released: its
                    # batch belongs to someone else now. Do not send.
                    print("MERGE_NOT_OWNER")
                    return 3
                new = _read_queue_file(pend)
                if not new or not new["msgs"]:
                    os.utime(proc)  # doubles as a lease renewal
                    print("MERGE_NOTHING")
                    return 0
                cur["msgs"] = cur["msgs"] + new["msgs"]
                _atomic_write(proc, lambda f: json.dump(cur, f,
                                                        ensure_ascii=False))
                os.remove(pend)
                print(f"MERGED:{len(new['msgs'])}:{len(cur['msgs'])}")
                return 0
            if os.path.exists(proc):
                age = int(time.time() - os.stat(proc).st_ctime)
                print(f"CLAIM_BUSY:{age}")
                return 3
            new = _read_queue_file(pend)
            if not new or not new["msgs"]:
                print("CLAIM_EMPTY")
                return 3
            new["claim_id"] = (f"{os.getpid()}-"
                               f"{random.randint(0, 0xFFFFFFFF):08x}")
            # Write the stamped copy, then drop pending: both under
            # the queue lock, so no claim or recycle can interleave.
            _atomic_write(proc, lambda f: json.dump(new, f,
                                                    ensure_ascii=False))
            os.remove(pend)
            print(f"CLAIMED:{new['claim_id']}:{len(new['msgs'])}")
            print(f"PROCESSING_FILE:{proc}")
            return 0
    except ValueError as e:
        print(f"QUEUE_CORRUPT:{e}")
        return 1


def cmd_release(claim_id=None):
    """Delete the processing file after the reply went out — only when
    it still carries claim_id (a recycled-and-reclaimed batch belongs to
    another worker and must survive). Exit 0 RELEASED / RELEASE_NOTHING,
    3 RELEASE_NOT_OWNER."""
    state_dir = _hook_state_dir()
    proc = os.path.join(state_dir, "ilink-processing.json")
    with _queue_lock(state_dir):
        if not os.path.exists(proc):
            print("RELEASE_NOTHING")
            return 0
        if claim_id:
            try:
                cur = _read_queue_file(proc) or {}
            except ValueError:
                cur = {}
            if cur.get("claim_id") != claim_id:
                print("RELEASE_NOT_OWNER")
                return 3
        os.remove(proc)
        print("RELEASED")
        return 0


def _send_raw(to_user_id, context_token, text, token, client_id=None):
    if client_id is None:
        client_id = f"muse-wechat-{random.randint(0, 0xFFFFFFFF):08x}"
    body = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": client_id,
            "message_type": 2,
            "message_state": 2,
            "item_list": [{"type": 1, "text_item": {"text": text}}],
        },
        "base_info": {"channel_version": "1.0.2"},
    }
    if context_token:
        body["msg"]["context_token"] = context_token
    return api("POST", "ilink/bot/sendmessage", body, token=token, timeout=40)


# --- Send audit log + idempotency ledger ------------------------------------
# The 2026-10-23 duplicate-send diagnosis showed the failure mode is a
# second, independent send invocation (fresh client_id), which server-side
# client_id dedupe cannot stop. The audit log makes the next incident
# provable in minutes; the ledger makes a repeated invocation with the
# same key a no-op. Neither file ever carries message text or tokens —
# only ids, outcomes and error type names.

def _audit_send(direction, to_user_id, client_id, result, error=None,
                attempt=1, elapsed_ms=None):
    """Append one JSON line to the send audit log. Never raises: a logging
    failure must not change the outcome of the send itself."""
    try:
        entry = {"ts": int(time.time()), "direction": direction,
                 "to": to_user_id, "client_id": client_id,
                 "result": result, "attempt": attempt}
        if error:
            entry["error"] = error
        if elapsed_ms is not None:
            entry["elapsed_ms"] = elapsed_ms
        with open(SEND_AUDIT_FILE, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        try:
            os.chmod(SEND_AUDIT_FILE, 0o600)
        except Exception:
            pass
        try:
            with open(SEND_AUDIT_FILE) as f:
                lines = f.readlines()
        except Exception:
            return
        if len(lines) > SEND_AUDIT_CAP:
            _atomic_write(SEND_AUDIT_FILE,
                          lambda f: f.writelines(lines[-SEND_AUDIT_CAP:]))
    except Exception:
        pass


class LedgerCorrupt(Exception):
    """Send ledger file exists but is unusable: unparseable JSON, a
    non-object top level, or unreadable for a reason other than
    'not there yet'. Callers must fail closed on this — never fall
    back to an empty ledger, because an empty ledger forgets every
    recorded success and the next retry of an old key would send with
    a fresh client_id the server cannot dedupe against."""


def _load_send_ledger():
    """key -> {client_id, at, result}. A missing file reads as empty
    (fresh deploy). Anything else unusable raises LedgerCorrupt.

    The corrupt file is deliberately left in place for operator
    inspection — do NOT auto-quarantine-and-continue: quarantining and
    then proceeding would make the very next send see an empty ledger
    again, which is exactly the silent fallback this refuses. Manual
    recovery: inspect the file, then move it away; the ledger rebuilds
    empty, and the old keys' dedupe history is lost with it — the
    accepted cost of a human intervening on a damaged file."""
    try:
        with open(SEND_LEDGER_FILE) as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        raise LedgerCorrupt(f"send ledger unreadable: {e}")
    if not isinstance(data, dict):
        raise LedgerCorrupt("send ledger is not an object")
    return data


def _record_send_ledger(key, client_id):
    """Backwards-compatible alias: record a successful send under key.
    New code uses _mark_send_ledger_sent; both write the same entry,
    and both propagate a write failure instead of swallowing it."""
    _mark_send_ledger_sent(key, client_id)


def _ledger_entry_is_sent(ent):
    """A fresh 'sent' entry counts as delivered. Entries written before
    the intent upgrade carry no status field but result 'ok': those are
    legacy successes and count the same, so old ledger files stay valid."""
    if not ent:
        return False
    if ent.get("status") == "sent":
        return True
    return ent.get("status") is None and ent.get("result") == "ok"


def _ledger_entry_is_fresh(ent):
    try:
        return int(time.time()) - int(ent.get("at", 0)) < SEND_LEDGER_TTL
    except (TypeError, ValueError):
        return False


def _check_send_ledger(key):
    """Return the recorded entry for key if it is a fresh success, else None.
    Propagates LedgerCorrupt: no current caller uses this (sends go
    through _resolve_send_intent), but a corrupt ledger must never be
    reported as 'not sent' from any entry point."""
    if not key:
        return None
    ent = _load_send_ledger().get(key)
    if not _ledger_entry_is_sent(ent) or not _ledger_entry_is_fresh(ent):
        return None
    return ent


def _write_send_ledger(book):
    """Atomic ledger write (tmp + replace). Raises on failure, on
    purpose: a ledger write is a correctness gate, not a best-effort
    log — callers must not treat a failed write as success (a lost
    intent means a later retry gets a fresh client_id and the server
    dedupe can no longer cover it). tmp+replace already gives
    atomicity; no fsync by design (a torn write is impossible, a lost
    write is reported loudly instead)."""
    _atomic_write(SEND_LEDGER_FILE, lambda f: json.dump(book, f))


def _prune_send_ledger(book, now):
    # now is kept for call-site clarity; freshness itself is judged
    # against the current clock (a few seconds of skew is immaterial
    # next to a 24h TTL). Non-dict entries are dropped as malformed.
    return {k: v for k, v in book.items()
            if isinstance(v, dict) and _ledger_entry_is_fresh(v)}


_ledger_lock_state = threading.local()


@contextlib.contextmanager
def _send_ledger_lock():
    """Inter-process lock for a keyed send, held across check, intent
    write, API call and result marking — so two processes racing on the
    same key cannot both decide 'not sent yet'. Re-entrant within one
    thread (the decorator below and the helpers share it); across
    processes/threads fcntl serializes.

    Fail-closed, with one platform exception: where fcntl is
    unavailable (non-POSIX platforms) the lock is a no-op and only the
    intent/client_id reuse still applies — documented, not silent, and
    the only case that degrades. Where fcntl IS available but opening
    the lock file or flock fails, this raises instead of proceeding
    unlocked: an unlocked keyed send could race another process into
    the same 'not sent yet' decision the lock exists to prevent."""
    state = _ledger_lock_state
    if getattr(state, "depth", 0) > 0:
        state.depth += 1
        try:
            yield
        finally:
            state.depth -= 1
        return
    fd = None
    if fcntl is not None:
        try:
            fd = os.open(SEND_LEDGER_LOCK_FILE,
                         os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
        except Exception:
            if fd is not None:
                os.close(fd)
            raise
    state.depth = 1
    try:
        yield
    finally:
        state.depth = 0
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


def _keyed_send_locked(func):
    """Decorator: a call carrying a --key runs entirely inside the
    ledger lock; keyless sends are untouched (no lock, no behaviour
    change). The key is the function's `key` argument, positional or
    keyword. A lock-acquire failure aborts the send before any ledger
    read or API call: LOCK_ERROR, exit 1, audit row — never an
    unlocked send. (Only the acquire step is converted; exceptions
    from the send itself are the function's own to report, and
    release-step failures are best-effort — the lock was held for
    the whole critical section either way.)"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            bound = inspect.signature(func).bind_partial(*args, **kwargs)
            key = bound.arguments.get("key")
        except Exception:
            key = kwargs.get("key")
        if not key:
            return func(*args, **kwargs)
        lock = _send_ledger_lock()
        try:
            lock.__enter__()
        except Exception as e:
            direction = {"cmd_send": "send", "cmd_proactive": "proactive",
                         "cmd_send_media": "media",
                         "cmd_proactive_media": "media"}.get(
                             func.__name__, "send")
            to_user_id = args[0] if args else None
            _audit_send(direction, to_user_id, None, "lock_error",
                        error=type(e).__name__)
            print(f"LOCK_ERROR:{e}")
            return 1
        try:
            return func(*args, **kwargs)
        finally:
            try:
                lock.__exit__(None, None, None)
            except Exception:
                pass
    return wrapper


def _resolve_send_intent(key):
    """Under the ledger lock: return (client_id, already_sent).

    First call for a key persists an intent {client_id, at, status}
    BEFORE any API call; a later call with the same key reuses that
    client_id. Only a confirmed success flips the entry to 'sent'.
    Application failures and exceptions leave the intent in place, so a
    retry reuses the same client_id — and the server dedupes by
    client_id (verified live 2026-10-23), so a 'sent but unconfirmed'
    retry does not deliver twice. One accepted semantic: if a retry
    carries different text under the same key, the user keeps at most
    the first text, because the deduped retry never re-delivers.
    Raises if the intent itself cannot be persisted: the caller must
    abort BEFORE any API call, or the send would go out with a
    client_id no retry could ever reuse."""
    now = int(time.time())
    book = _load_send_ledger()
    ent = book.get(key)
    if ent and _ledger_entry_is_fresh(ent):
        if _ledger_entry_is_sent(ent):
            return ent.get("client_id"), True
        if ent.get("status") == "intent" and ent.get("client_id"):
            return ent["client_id"], False
    client_id = f"muse-wechat-{random.randint(0, 0xFFFFFFFF):08x}"
    book = _prune_send_ledger(book, now)
    book[key] = {"client_id": client_id, "at": now, "status": "intent"}
    _write_send_ledger(book)
    return client_id, False


def _mark_send_ledger_sent(key, client_id):
    """Flip a key's intent to 'sent' after a confirmed success.
    Raises on write failure, and also on a corrupt ledger load (the
    load happens here too, so a damaged file surfaces through the
    same caller path): either way the caller reports LEDGER_WRITE_FAILED,
    not success, per the unconfirmed-send semantics below.
    Raises on write failure: the caller must NOT report success then,
    because the ledger still says 'intent'. The caller reports the
    send as unconfirmed (nonzero exit, no SEND_OK), so its driver does
    not commit the batch and a later retry reuses the same client_id —
    and the server's client_id dedupe then covers the resend, so the
    user still receives exactly one message. A silent success here
    would be worse than a loud retry: the next invocation would send
    again believing nothing was recorded, and only the dedupe would
    (usually) save it."""
    now = int(time.time())
    book = _prune_send_ledger(_load_send_ledger(), now)
    book[key] = {"client_id": client_id, "at": now,
                 "status": "sent", "result": "ok"}
    _write_send_ledger(book)


def _api_error_code(data):
    """The status of an iLink API response, as one int: 0 on success.

    The server reports failure through any of three fields — `code`,
    `ret` and `errcode` — and a response can carry `code: 0` next to
    a non-zero `ret` (e.g. `{"code": 0, "ret": -2}`, the stale
    context_token signature). Reading `code` alone used to report such
    a response as a success: a keyed send flipped its ledger entry to
    'sent' for a message that never went out, so every retry got
    SEND_ALREADY_SENT and the reply was lost for good. Rule: -14 in
    any field wins (token expiry); otherwise the first non-zero field
    in code / ret / errcode order; a non-integer status value counts
    as a failure (-1); a non-dict response is a failure (-1)."""
    if not isinstance(data, dict):
        return -1
    codes = []
    for field in ("code", "ret", "errcode"):
        value = data.get(field)
        if value is None or value == "":
            continue
        try:
            codes.append(int(value))
        except (TypeError, ValueError):
            codes.append(-1)
    if -14 in codes:
        return -14
    for c in codes:
        if c != 0:
            return c
    return 0


def _classify_send_response(data, require_message_id=False):
    """Shared send/proactive outcome rule: 'ok' | 'token_expired' | 'app_error'.
    Status comes from _api_error_code (code / ret / errcode, not just
    code): -14 is always token expiry; any other non-zero status is an
    application failure.
    When require_message_id is set (media sends), a code-0 response with
    no message_id is also an application failure, not a success.
    Contract note (S5, deliberately NOT enforced): text sends accept
    code==0 without message_id, while media sends require it. The
    standing contract is that the server always returns message_id for
    a successful text send, so a code-0 reply without one would be a
    server-contract surprise (currently accepted, not failed); aligning
    text with media here would change success semantics and is left
    for a separate decision."""
    code = _api_error_code(data)
    if code == -14:
        return "token_expired"
    if code != 0:
        return "app_error"
    if require_message_id and not data.get("message_id"):
        return "app_error"
    return "ok"


def _proactive_needs_tokenless_retry(data, require_message_id=False):
    """proactive's retry-without-token trigger, on top of the shared rule:
    any non-ok outcome, or a ret == -2 / 'prepare failed' response even
    when code == 0 (the stale-context_token signature)."""
    if _classify_send_response(data, require_message_id) != "ok":
        return True
    if data.get("ret") == -2:
        return True
    return "prepare failed" in str(data.get("errmsg", ""))


def _validate_send_text(text):
    """发送正文守卫：空正文、或以 -- 开头的正文一律拒绝。
    2026-10-27 00:11 事故：worker 的 send 命令丢了正文占位符 '-'，
    '--key' 滑进正文位置被原样发出。桥此前对正文无任何校验，
    于是标志被当成消息发给了用户。本守卫让这类传错只报错、不发出。
    返回 None 表示通过，否则返回错误码。"""
    if text is None or text.strip() == "":
        return "SEND_TEXT_EMPTY"
    if text.strip().startswith("--"):
        return "SEND_TEXT_LOOKS_LIKE_FLAG"
    return None


@_keyed_send_locked
def cmd_proactive(to_user_id, text, key=None):
    """主动推送：先用缓存的 context_token，失败则去掉 token 重试一次。"""
    s = load_session()
    token = s.get("bot_token")
    if not token:
        print("NO_TOKEN")
        return 1
    if text == "-":
        text = sys.stdin.read()
    _text_err = _validate_send_text(text)
    if _text_err:
        # 拒绝在任何 API 调用之前：不落账本意图、不发消息，只记审计。
        _audit_send("proactive", to_user_id, None, "refused", error=_text_err)
        print(f"{_text_err}: refusing to send empty or flag-like text")
        return 1
    if key:
        try:
            client_id, already_sent = _resolve_send_intent(key)
        except LedgerCorrupt as e:
            # Ledger unusable: fail closed BEFORE any API call.
            # Falling back to an empty ledger here would forget
            # recorded successes and let a retry resend under a
            # fresh client_id the server cannot dedupe against.
            _audit_send("proactive", to_user_id, None, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_CORRUPT:{e}")
            return 1
        except Exception as e:
            # Intent not persisted: abort BEFORE any API call. Sending
            # now would use a client_id no retry could reuse.
            _audit_send("proactive", to_user_id, None, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_ERROR:{e}")
            return 1
        if already_sent:
            print("SEND_ALREADY_SENT")
            return 0
    else:
        client_id = f"muse-wechat-{random.randint(0, 0xFFFFFFFF):08x}"
    contacts = load_contacts()
    ctx = (contacts.get(to_user_id) or {}).get("context_token")
    if not ctx:
        print("NO_CONTEXT_TOKEN")
        return 1
    try:
        t0 = time.time()
        data = _send_raw(to_user_id, ctx, text, token, client_id=client_id)
    except Exception as e:
        _audit_send("proactive", to_user_id, client_id, "exception",
                    error=type(e).__name__, attempt=1)
        print(f"SEND_ERROR:{e}")
        return 1
    retry = _proactive_needs_tokenless_retry(data)
    _audit_send("proactive", to_user_id, client_id,
                "app_error" if retry else "ok",
                elapsed_ms=int((time.time() - t0) * 1000), attempt=1)
    if _classify_send_response(data) == "token_expired":
        return _report_token_status()
    if retry:
        # context_token 可能过期，去掉 token 重试一次
        print(f"RETRY_WITHOUT_TOKEN:{str(data.get('errmsg', ''))[:80]}", flush=True)
        try:
            t0 = time.time()
            data = _send_raw(to_user_id, None, text, token, client_id=client_id)
        except Exception as e:
            _audit_send("proactive", to_user_id, client_id, "exception",
                        error=type(e).__name__, attempt=2)
            print(f"SEND_ERROR:{e}")
            return 1
        # Same outcome rule as attempt 1: a second {"ret": -2} is a
        # failure, not PROACTIVE_OK (it used to be judged by `code`
        # alone and report success for an undelivered push).
        outcome = _classify_send_response(data)
        _audit_send("proactive", to_user_id, client_id,
                    "ok" if outcome == "ok" else "app_error",
                    elapsed_ms=int((time.time() - t0) * 1000), attempt=2)
        if outcome == "token_expired":
            return _report_token_status()
        if outcome != "ok":
            print(f"PROACTIVE_FAILED:{str(data)[:200]}")
            return 1
    if key:
        try:
            _mark_send_ledger_sent(key, client_id)
        except Exception as e:
            # Message may already be delivered, but the ledger cannot
            # confirm it: report unconfirmed, not success. The driver
            # will not commit the batch, and a retry reuses the same
            # client_id, which the server dedupes — one delivery.
            _audit_send("proactive", to_user_id, client_id, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_WRITE_FAILED:{e}")
            return 1
    if data.get("message_id"):
        # Confirmed success: the push is quotable from now on, even
        # before the next poll echoes it back.
        _quote_cache_upsert({str(data["message_id"]): {
            "text": text, "kind": "text", "from": "bot",
            "at": int(time.time())}})
    _clear_token_stale()
    print("PROACTIVE_OK")
    return 0


@_keyed_send_locked
def cmd_send(to_user_id, context_token, text, key=None):
    """发送回复。网络层失败时用相同 client_id 最多重试一次；应用层明确失败不重试。
    key 不为空时：账本已有该 key 的成功记录直接返回已发送，不再调用 API；
    否则先落意图（含固定 client_id）再发送，成功后标记已发送——重试复用
    同一 client_id，服务端去重兜底，崩溃窗口也不重复投递。"""
    s = load_session()
    token = s.get("bot_token")
    if not token:
        print("NO_TOKEN")
        return 1
    if text == "-":
        text = sys.stdin.read()
    _text_err = _validate_send_text(text)
    if _text_err:
        # 拒绝在任何 API 调用之前：不落账本意图、不发消息，只记审计。
        _audit_send("send", to_user_id, None, "refused", error=_text_err)
        print(f"{_text_err}: refusing to send empty or flag-like text")
        return 1
    if key:
        try:
            client_id, already_sent = _resolve_send_intent(key)
        except LedgerCorrupt as e:
            # Ledger unusable: fail closed BEFORE any API call.
            # Falling back to an empty ledger here would forget
            # recorded successes and let a retry resend under a
            # fresh client_id the server cannot dedupe against.
            _audit_send("send", to_user_id, None, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_CORRUPT:{e}")
            return 1
        except Exception as e:
            # Intent not persisted: abort BEFORE any API call. Sending
            # now would use a client_id no retry could reuse.
            _audit_send("send", to_user_id, None, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_ERROR:{e}")
            return 1
        if already_sent:
            print("SEND_ALREADY_SENT")
            return 0
    else:
        client_id = f"muse-wechat-{random.randint(0, 0xFFFFFFFF):08x}"
    body = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": client_id,
            "message_type": 2,
            "message_state": 2,
            "item_list": [{"type": 1, "text_item": {"text": text}}],
        },
        "base_info": {"channel_version": "1.0.2"},
    }
    if context_token:
        # L15: align with _send_raw, which only sends a truthy
        # context_token; a null key used to be sent unconditionally.
        body["msg"]["context_token"] = context_token
    for attempt in (1, 2):
        try:
            t0 = time.time()
            data = api("POST", "ilink/bot/sendmessage", body, token=token, timeout=40)
        except Exception as e:
            _audit_send("send", to_user_id, client_id, "exception",
                        error=type(e).__name__, attempt=attempt)
            if attempt == 1:
                time.sleep(5)
                continue
            print(f"SEND_ERROR:{e}")
            return 1
        outcome = _classify_send_response(data)
        _audit_send("send", to_user_id, client_id,
                    "ok" if outcome == "ok" else "app_error",
                    elapsed_ms=int((time.time() - t0) * 1000), attempt=attempt)
        if outcome == "token_expired":
            return _report_token_status()
        if outcome != "ok":
            print(f"SEND_FAILED:{str(data)[:200]}")
            return 1
        if key:
            try:
                _mark_send_ledger_sent(key, client_id)
            except Exception as e:
                # Message may already be delivered, but the ledger
                # cannot confirm it: report unconfirmed, not success.
                # The driver will not commit the batch, and a retry
                # reuses the same client_id, which the server dedupes.
                _audit_send("send", to_user_id, client_id, "ledger_error",
                            error=type(e).__name__, attempt=attempt)
                print(f"LEDGER_WRITE_FAILED:{e}")
                return 1
        if data.get("message_id"):
            # Confirmed success: the reply is quotable from now on, even
            # before the next poll echoes it back.
            _quote_cache_upsert({str(data["message_id"]): {
                "text": text, "kind": "text", "from": "bot",
                "at": int(time.time())}})
        _clear_token_stale()
        print("SEND_OK")
        return 0


def _typing_ticket(to_user_id, token, force=False):
    """取（或刷新）某用户的 typing_ticket，缓存在 contacts 里（TTL 9 分钟）。"""
    contacts = load_contacts()
    entry = contacts.get(to_user_id) or {}
    now = int(time.time())
    if (not force and entry.get("typing_ticket")
            and now - entry.get("typing_ticket_at", 0) < 540):
        return entry["typing_ticket"]
    ctx = entry.get("context_token")
    if not ctx:
        return None
    data = api("POST", "ilink/bot/getconfig",
               {"ilink_user_id": to_user_id, "context_token": ctx,
                "base_info": {"channel_version": "1.0.2"}},
               token=token, timeout=20)
    ticket = data.get("typing_ticket")
    if ticket:
        # Reload under the lock and touch only the ticket fields: the
        # snapshot above is stale by a network round-trip, and saving it
        # whole could revert a context_token poll wrote meanwhile.
        def _set_ticket(contacts):
            ent = contacts.get(to_user_id)
            if not isinstance(ent, dict):
                ent = contacts[to_user_id] = {}
            ent["typing_ticket"] = ticket
            ent["typing_ticket_at"] = now
        update_contacts(_set_ticket)
    return ticket


def cmd_typing(to_user_id, status_word):
    """显示/取消"正在输入"状态：status_word = start | stop。这是装饰性功能，
    失败绝不影响正常回复流程（调用方应忽略失败）。"""
    status = {"start": 1, "stop": 2}.get(status_word)
    if status is None:
        print("TYPING_BAD_STATUS")
        return 2
    s = load_session()
    token = s.get("bot_token")
    if not token:
        print("NO_TOKEN")
        return 1
    try:
        ticket = _typing_ticket(to_user_id, token)
        if not ticket:
            print("NO_TYPING_TICKET")
            return 1
        data = api("POST", "ilink/bot/sendtyping",
                   {"ilink_user_id": to_user_id, "typing_ticket": ticket,
                    "status": status,
                    "base_info": {"channel_version": "1.0.2"}},
                   token=token, timeout=20)
        if _api_error_code(data) != 0:
            # ticket 可能已失效，强制刷新后重试一次；刷新仍无票据时
            # 直接报错，不要发一次必然失败的请求。
            ticket = _typing_ticket(to_user_id, token, force=True)
            if not ticket:
                print("NO_TYPING_TICKET")
                return 1
            data = api("POST", "ilink/bot/sendtyping",
                       {"ilink_user_id": to_user_id, "typing_ticket": ticket,
                        "status": status,
                        "base_info": {"channel_version": "1.0.2"}},
                       token=token, timeout=20)
            if _api_error_code(data) != 0:
                print(f"TYPING_FAILED:{str(data)[:200]}")
                return 1
    except Exception as e:
        print(f"TYPING_ERROR:{e}")
        return 1
    print("TYPING_OK")
    return 0


def _aes_ecb_encrypt(data: bytes, key: bytes) -> bytes:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.padding import PKCS7
    padder = PKCS7(128).padder()
    padded = padder.update(data) + padder.finalize()
    enc = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
    return enc.update(padded) + enc.finalize()


def _send_item_raw(to_user_id, context_token, item, token, client_id=None):
    """sendmessage 的通用封装：item 是任意 MessageItem（文本/图片/文件）。"""
    if client_id is None:
        client_id = f"muse-wechat-{random.randint(0, 0xFFFFFFFF):08x}"
    body = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": client_id,
            "message_type": 2,
            "message_state": 2,
            "item_list": [item],
        },
        "base_info": {"channel_version": "1.0.2"},
    }
    if context_token:
        body["msg"]["context_token"] = context_token
    return api("POST", "ilink/bot/sendmessage", body, token=token, timeout=40)


def _upload_media(to_user_id, data, media_type, token):
    """把媒体字节加密上传到微信 CDN。返回构造消息 item 所需的字段 dict。
    协议要点（实测）：getuploadurl 的 aeskey 传 hex 字符串；消息里
    CDNMedia.aes_key 用 base64(hex 字符串)；CDN 上传响应头 x-encrypted-param
    即下载参数。"""
    import hashlib
    from urllib.parse import quote
    key = os.urandom(16)
    aeskey_hex = key.hex()
    filekey = "%032x" % random.getrandbits(128)
    ciphertext = _aes_ecb_encrypt(data, key)
    # 注意：getuploadurl 的 media_type 是另一套枚举（1=IMG, 2=VID, 3=FILE, 4=VOICE），
    # 与消息 item 类型（2=IMAGE, 4=FILE）不同，必须映射。
    upload_type = {2: 1, 3: 4, 4: 3, 5: 2}[media_type]
    body = {
        "filekey": filekey,
        "media_type": upload_type,
        "to_user_id": to_user_id,
        "rawsize": len(data),
        "rawfilemd5": hashlib.md5(data).hexdigest(),
        "filesize": len(ciphertext),
        "aeskey": aeskey_hex,
        "no_need_thumb": True,
        "base_info": {"channel_version": "1.0.2"},
    }
    r = api("POST", "ilink/bot/getuploadurl", body, token=token, timeout=20)
    upload_param = r.get("upload_param")
    full_url = r.get("upload_full_url")
    if not upload_param and not full_url:
        raise RuntimeError(f"getuploadurl no upload_param: {str(r)[:200]}")
    # Prefer a server-provided full upload URL when present; fall back
    # to the constructed CDN URL otherwise.
    url = full_url or (
        f"{CDN_BASE_URL}/upload?encrypted_query_param="
        f"{quote(upload_param, safe='')}&filekey={filekey}")
    req = urllib.request.Request(url, data=ciphertext, method="POST",
                                 headers={"Content-Type": "application/octet-stream"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        download_param = resp.headers.get("x-encrypted-param")
    if not download_param:
        raise RuntimeError("CDN upload: no x-encrypted-param header")
    return {
        "media": {"encrypt_query_param": download_param,
                  "aes_key": base64.b64encode(aeskey_hex.encode("ascii")).decode("ascii"),
                  "encrypt_type": 1},
        "md5": body["rawfilemd5"],
        "rawsize": len(data),
        "cipher_size": len(ciphertext),
    }


def _media_item(path, up, media_type, playtime_ms=0):
    if media_type == 2:  # IMAGE
        return {"type": 2, "image_item": {"media": up["media"],
                                          "mid_size": up["cipher_size"]}}
    if media_type == 3:  # VOICE (experimental): audio must be SILK@24kHz
        # NOTE: the iLink channel silently drops outbound voice items —
        # the API returns success but the client never shows them
        # (verified live 2026-10-22; other SDKs document the same).
        # Kept for completeness; send audio as FILE (wav) instead.
        return {"type": 3, "voice_item": {"media": up["media"],
                                          "encode_type": 6,
                                          "bits_per_sample": 16,
                                          "sample_rate": 24000,
                                          "playtime": int(playtime_ms)}}
    if media_type == 5:  # VIDEO (thumb optional: getuploadurl says no_need_thumb)
        return {"type": 5, "video_item": {"media": up["media"],
                                          "video_size": up["cipher_size"]}}
    # FILE
    return {"type": 4, "file_item": {"media": up["media"],
                                     "file_name": os.path.basename(path),
                                     "md5": up["md5"],
                                     "len": str(up["rawsize"])}}


@_keyed_send_locked
def cmd_send_media(to_user_id, context_token, path, media_type, playtime_ms=0, key=None):
    """发送图片/视频/文件（回复模式，需 context_token）。"""
    s = load_session()
    token = s.get("bot_token")
    if not token:
        print("NO_TOKEN")
        return 1
    if key:
        try:
            client_id, already_sent = _resolve_send_intent(key)
        except LedgerCorrupt as e:
            # Ledger unusable: fail closed BEFORE any API call.
            # Falling back to an empty ledger here would forget
            # recorded successes and let a retry resend under a
            # fresh client_id the server cannot dedupe against.
            _audit_send("media", to_user_id, None, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_CORRUPT:{e}")
            return 1
        except Exception as e:
            # Intent not persisted: abort BEFORE any read or API call.
            _audit_send("media", to_user_id, None, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_ERROR:{e}")
            return 1
        if already_sent:
            print("SEND_ALREADY_SENT")
            return 0
    try:
        if os.path.getsize(path) > MEDIA_MAX_BYTES:
            print(f"MEDIA_TOO_LARGE:{os.path.basename(path)} exceeds "
                  f"{MEDIA_MAX_BYTES} bytes, refusing to read it fully "
                  f"into memory")
            return 1
    except OSError as e:
        print(f"MEDIA_READ_ERROR:{e}")
        return 1
    try:
        with open(path, "rb") as f:
            data = f.read()
    except Exception as e:
        print(f"MEDIA_READ_ERROR:{e}")
        return 1
    if not key:
        client_id = f"muse-wechat-{random.randint(0, 0xFFFFFFFF):08x}"
    try:
        up = _upload_media(to_user_id, data, media_type, token)
        item = _media_item(path, up, media_type, playtime_ms)
        t0 = time.time()
        data_resp = _send_item_raw(to_user_id, context_token, item, token,
                                   client_id=client_id)
    except Exception as e:
        _audit_send("media", to_user_id, client_id, "exception",
                    error=type(e).__name__)
        print(f"SEND_ERROR:{e}")
        return 1
    outcome = _classify_send_response(data_resp, require_message_id=True)
    _audit_send("media", to_user_id, client_id,
                "ok" if outcome == "ok" else "app_error",
                elapsed_ms=int((time.time() - t0) * 1000))
    if outcome == "token_expired":
        return _report_token_status()
    if outcome != "ok":
        print(f"SEND_FAILED:{str(data_resp)[:200]}")
        return 1
    if key:
        try:
            _mark_send_ledger_sent(key, client_id)
        except Exception as e:
            # Message may already be delivered, but the ledger cannot
            # confirm it: report unconfirmed, not success. The driver
            # will not commit the batch, and a retry reuses the same
            # client_id, which the server dedupes.
            _audit_send("media", to_user_id, client_id, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_WRITE_FAILED:{e}")
            return 1
    if data_resp.get("message_id"):
        # Confirmed success: the media message is quotable from now on.
        _quote_cache_upsert({str(data_resp["message_id"]): {
            "text": os.path.basename(path),
            "kind": ITEM_TYPE_NAMES.get(media_type, "file"),
            "from": "bot", "at": int(time.time()),
            "media_path": path}})
    _clear_token_stale()
    print("SEND_OK")
    return 0


@_keyed_send_locked
def cmd_proactive_media(to_user_id, path, media_type, playtime_ms=0, key=None):
    """主动推送图片/视频/文件：用缓存的 context_token，失败则去掉 token 重试一次。"""
    s = load_session()
    token = s.get("bot_token")
    if not token:
        print("NO_TOKEN")
        return 1
    if key:
        try:
            client_id, already_sent = _resolve_send_intent(key)
        except LedgerCorrupt as e:
            # Ledger unusable: fail closed BEFORE any API call.
            # Falling back to an empty ledger here would forget
            # recorded successes and let a retry resend under a
            # fresh client_id the server cannot dedupe against.
            _audit_send("media", to_user_id, None, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_CORRUPT:{e}")
            return 1
        except Exception as e:
            # Intent not persisted: abort BEFORE any read or API call.
            _audit_send("media", to_user_id, None, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_ERROR:{e}")
            return 1
        if already_sent:
            print("SEND_ALREADY_SENT")
            return 0
    contacts = load_contacts()
    ctx = (contacts.get(to_user_id) or {}).get("context_token")
    if not ctx:
        print("NO_CONTEXT_TOKEN")
        return 1
    if not key:
        client_id = f"muse-wechat-{random.randint(0, 0xFFFFFFFF):08x}"
    try:
        if os.path.getsize(path) > MEDIA_MAX_BYTES:
            print(f"MEDIA_TOO_LARGE:{os.path.basename(path)} exceeds "
                  f"{MEDIA_MAX_BYTES} bytes, refusing to read it fully "
                  f"into memory")
            return 1
    except OSError as e:
        print(f"MEDIA_READ_ERROR:{e}")
        return 1
    try:
        with open(path, "rb") as f:
            data = f.read()
        up = _upload_media(to_user_id, data, media_type, token)
        item = _media_item(path, up, media_type, playtime_ms)
        t0 = time.time()
        data_resp = _send_item_raw(to_user_id, ctx, item, token, client_id=client_id)
    except Exception as e:
        _audit_send("media", to_user_id, client_id, "exception",
                    error=type(e).__name__, attempt=1)
        print(f"SEND_ERROR:{e}")
        return 1
    outcome = _classify_send_response(data_resp, require_message_id=True)
    retry = _proactive_needs_tokenless_retry(data_resp,
                                             require_message_id=True)
    _audit_send("media", to_user_id, client_id,
                "app_error" if retry else "ok",
                elapsed_ms=int((time.time() - t0) * 1000), attempt=1)
    if outcome == "token_expired":
        return _report_token_status()
    if retry:
        # Same tokenless-retry trigger as text proactive (including
        # the 'prepare failed' signature), so the two push paths agree.
        try:
            t0 = time.time()
            data_resp = _send_item_raw(to_user_id, None, item, token,
                                       client_id=client_id)
        except Exception as e:
            _audit_send("media", to_user_id, client_id, "exception",
                        error=type(e).__name__, attempt=2)
            print(f"SEND_ERROR:{e}")
            return 1
        outcome = _classify_send_response(data_resp, require_message_id=True)
        _audit_send("media", to_user_id, client_id,
                    "ok" if outcome == "ok" else "app_error",
                    elapsed_ms=int((time.time() - t0) * 1000), attempt=2)
        if outcome == "token_expired":
            return _report_token_status()
        if outcome != "ok":
            print(f"PROACTIVE_FAILED:{str(data_resp)[:200]}")
            return 1
    if key:
        try:
            _mark_send_ledger_sent(key, client_id)
        except Exception as e:
            # Message may already be delivered, but the ledger cannot
            # confirm it: report unconfirmed, not success. The driver
            # will not commit the batch, and a retry reuses the same
            # client_id, which the server dedupes.
            _audit_send("media", to_user_id, client_id, "ledger_error",
                        error=type(e).__name__)
            print(f"LEDGER_WRITE_FAILED:{e}")
            return 1
    if data_resp.get("message_id"):
        # Confirmed success: the media message is quotable from now on.
        _quote_cache_upsert({str(data_resp["message_id"]): {
            "text": os.path.basename(path),
            "kind": ITEM_TYPE_NAMES.get(media_type, "file"),
            "from": "bot", "at": int(time.time()),
            "media_path": path}})
    _clear_token_stale()
    print("PROACTIVE_OK")
    return 0


def _fmt_age(seconds):
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}s"
    if seconds < 5400:
        return f"{seconds // 60}m"
    if seconds < 172800:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def _queue_count(path):
    """(count, corrupt): a queue file that exists but does not
    parse is corrupt — never report it as empty, or a stuck batch
    looks like a drained queue. Same strict shape rule as the hook
    script's queue_count: top level must be an object and .msgs
    must be an array. A parseable-but-misshapen file (e.g.
    {"msgs": "invalid"}) is corrupt too — counting a string's
    length as a message count used to report phantom backlog.
    Shared by health and stats."""
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        return 0, False
    except Exception:
        return 0, True
    if not isinstance(data, dict) or not isinstance(data.get("msgs"), list):
        return 0, True
    return len(data["msgs"]), False


def cmd_health(offline=False):
    """Self-check: auth, live token probe, message queue, last poll error.
    Exit code 0 = healthy, 1 = warnings, 2 = broken."""
    now = int(time.time())
    lines, warnings, problems = [], [], []
    s = load_session()
    token = s.get("bot_token")
    if not token:
        problems.append("not logged in: no bot_token in session, QR login needed")
    elif s.get("login_at"):
        lines.append(f"auth: logged in (QR scanned {_fmt_age(now - s['login_at'])} ago)")
    else:
        lines.append("auth: logged in (login time unknown)")

    owner = None
    try:
        with open(os.path.join(BRIDGE, "owner.json")) as f:
            owner = json.load(f).get("owner_id")
    except Exception:
        pass

    # Live probe: getconfig for a typing ticket — no side effects, does not
    # touch the message cursor.
    if token and not offline:
        ctx = (load_contacts().get(owner) or {}).get("context_token") if owner else None
        if not ctx:
            lines.append("probe: skipped (no cached owner context_token)")
        else:
            try:
                data = api("POST", "ilink/bot/getconfig",
                           {"ilink_user_id": owner, "context_token": ctx,
                            "base_info": {"channel_version": "1.0.2"}},
                           token=token, timeout=12)
                if data.get("typing_ticket"):
                    lines.append("probe: token valid (getconfig OK)")
                elif _api_error_code(data) == -14:
                    problems.append("token expired (getconfig code -14), re-scan needed")
                else:
                    warnings.append(f"probe result unclear: {str(data)[:80]}")
            except Exception as e:
                warnings.append(f"probe failed (network?): {type(e).__name__}")
    elif token:
        lines.append("probe: skipped (--offline)")

    # Message queue (hook state dir — same canonical variable and default
    # as the example hook script; ILINK_HOOK_STATE is a legacy alias)
    state_dir = (os.environ.get("ILINK_HOOK_STATE_DIR")
                 or os.environ.get("ILINK_HOOK_STATE")
                 or os.path.expanduser("~/hooks/state"))
    pend = os.path.join(state_dir, "ilink-pending.json")
    proc = os.path.join(state_dir, "ilink-processing.json")

    pend_n, pend_corrupt = _queue_count(pend)
    if pend_corrupt:
        warnings.append(
            f"pending queue is corrupt (unparseable JSON): {pend} — "
            "the hook quarantines it on its next run; inspect it there")
    elif pend_n > 0:
        lines.append(f"queue: {pend_n} message(s) pending")
    else:
        lines.append("queue: nothing pending")
    if os.path.exists(proc):
        claimed_age = now - int(os.stat(proc).st_ctime)  # ctime = claim time
        if claimed_age > 600:
            warnings.append(
                f"processing claimed {_fmt_age(claimed_age)} ago, still not done "
                "(worker may be stuck)")
        else:
            lines.append(f"queue: processing in flight ({_fmt_age(claimed_age)})")
    else:
        lines.append("queue: nothing processing")

    try:
        lines.append(f"dedupe: {len(load_seen())} message ids recorded")
    except Exception:
        pass

    lines.append(f"owners: {len(load_owner_ids())} allowlisted")
    try:
        with open(os.path.join(BRIDGE, "unknown_senders.json")) as f:
            unknown = json.load(f)
        if unknown:
            lines.append(f"unknown senders: {len(unknown)} awaiting approval "
                         "(unknown_senders.json)")
    except Exception:
        pass
    try:
        _load_send_ledger()
    except LedgerCorrupt:
        # Keyed sends fail closed while the ledger is damaged, so this
        # is a warning, not a snapshot detail: the file is left in
        # place for inspection, and sends resume only once an operator
        # has inspected or moved it away.
        warnings.append(
            "send ledger is corrupt (unparseable or not an object): "
            f"{SEND_LEDGER_FILE} — keyed sends abort until it is "
            "inspected or moved away")

    poll_log = os.path.join(BRIDGE, "poll.log")
    if os.path.exists(poll_log):
        try:
            # L4: read only the tail — poll.log is append-only and
            # unbounded, and health only needs its last line.
            with open(poll_log, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size - 8192))
                tail = [ln.strip() for ln in
                        f.read().decode("utf-8", "replace").splitlines()
                        if ln.strip()]
            if tail:
                lines.append(f"poll: last error {tail[-1][:90]}")
        except Exception:
            pass

    for w in warnings:
        lines.append(f"WARN: {w}")
    for p in problems:
        lines.append(f"BAD: {p}")
    if problems:
        lines.append("verdict: BAD")
        print("\n".join(lines))
        return 2
    if warnings:
        lines.append("verdict: WARNINGS")
        print("\n".join(lines))
        return 1
    lines.append("verdict: OK")
    print("\n".join(lines))
    return 0


# 1x1 transparent PNG (base64) for the smoke-test media leg: a real
# image upload through the encrypted CDN path, nearly invisible to the
# owner, and self-describing via the text leg sent just before it.
_SMOKE_PNG_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk"
    "+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)
_SMOKE_TEXT = ("🔥 冒烟测试：这条是 ilink.py smoke-test 发出的测试消息，"
               "下一条是测试图片，收到请忽略。")


def cmd_smoke_test(no_send=False, to_user_id=None):
    """End-to-end chain check, beyond health's local-state snapshot:
    login probe, local receive readiness, a real proactive text +
    image to the owner (unless --no-send), and send-ledger
    readability. Prints one SMOKE line per check and SMOKE_OK /
    SMOKE_FAILED; exit 0 only when nothing FAILED (SKIP does not
    fail). The send legs deliberately reuse cmd_proactive /
    cmd_proactive_media — the exact production paths, including the
    tokenless retry — so a PASS means the real chain works, not a
    rehearsal of it. No --key: every run must really send, or a
    repeat run would print SEND_ALREADY_SENT and prove nothing. The
    test image lives in a temp dir (not MEDIA_DIR) so it never enters
    media retention; the quote-cache entry it leaves behind points at
    a deleted path, same as any pruned media."""
    results = []  # (name, status, detail)

    def _check(name, status, detail=""):
        results.append((name, status, detail))

    # -- target: explicit --to wins, else the primary owner.
    target = to_user_id
    if not target:
        try:
            with open(os.path.join(BRIDGE, "owner.json")) as f:
                target = json.load(f).get("owner_id")
        except Exception:
            target = None
        if not target:
            owners = load_owner_ids()
            target = sorted(owners)[0] if owners else None

    # -- login: session token + the same live getconfig probe health
    # uses (no side effects, does not touch the message cursor).
    # Deliberately NOT getupdates: that is a ~35s long-poll hold, and
    # a fetch that is never committed would just be redelivered.
    # A diagnostic reports only: on -14 it must NOT call
    # _report_token_status, which would flip the token-stale state
    # machine as a side effect.
    s = load_session()
    token = s.get("bot_token")
    if not token:
        _check("login", "FAIL", "not logged in: no bot_token in session")
    else:
        ctx = (load_contacts().get(target) or {}).get("context_token") if target else None
        if not ctx:
            _check("login", "SKIP", "no cached owner context_token for probe")
        else:
            try:
                data = api("POST", "ilink/bot/getconfig",
                           {"ilink_user_id": target, "context_token": ctx,
                            "base_info": {"channel_version": "1.0.2"}},
                           token=token, timeout=12)
            except Exception as e:
                _check("login", "FAIL", f"probe failed: {type(e).__name__}")
            else:
                if data.get("typing_ticket"):
                    _check("login", "PASS", "getconfig OK")
                elif _api_error_code(data) == -14:
                    _check("login", "FAIL", "token expired (getconfig code -14)")
                else:
                    _check("login", "FAIL", f"probe unclear: {str(data)[:80]}")

    # -- receive: local readiness of the inbound path (dirs writable,
    # session parses). The live poll loop is exercised by the hook
    # every few seconds; poll.log carries its errors.
    recv_problems = []
    if not (os.path.isdir(BRIDGE) and os.access(BRIDGE, os.W_OK)):
        recv_problems.append("data dir not writable")
    if not (os.path.isdir(MEDIA_DIR) and os.access(MEDIA_DIR, os.W_OK)):
        recv_problems.append("media dir not writable")
    try:
        with open(SESSION_FILE) as f:
            sdata = json.load(f)
        if not isinstance(sdata, dict):
            recv_problems.append("session is not an object")
    except FileNotFoundError:
        recv_problems.append("no session file")
    except Exception as e:
        recv_problems.append(f"session unreadable: {type(e).__name__}")
    if recv_problems:
        _check("receive", "FAIL", "; ".join(recv_problems))
    else:
        _check("receive", "PASS", "dirs writable, session parses")

    # -- send / media: real proactive legs, their stdout swallowed so
    # the report stays one line per check; return codes decide, with
    # the swallowed last line as the failure detail when present.
    def _run_leg(func, *args):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = func(*args)
        out = buf.getvalue().strip().splitlines()
        return code, (out[-1][:80] if out else f"exited {code}")

    if no_send:
        _check("send", "SKIP", "--no-send")
        _check("media", "SKIP", "--no-send")
    elif not target:
        _check("send", "FAIL", "no owner configured (bind first)")
        _check("media", "SKIP", "no target to send to")
    else:
        code, detail = _run_leg(cmd_proactive, target, _SMOKE_TEXT)
        if code == 0:
            _check("send", "PASS", f"PROACTIVE_OK to {target}")
        else:
            _check("send", "FAIL", detail)
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(suffix=".png", prefix="ilink-smoke-")
            with os.fdopen(fd, "wb") as f:
                f.write(base64.b64decode(_SMOKE_PNG_B64))
            code, detail = _run_leg(cmd_proactive_media, target, tmp_path, 2)
            if code == 0:
                _check("media", "PASS", "proactive image OK")
            else:
                _check("media", "FAIL", detail)
        except Exception as e:
            _check("media", "FAIL", f"media leg error: {type(e).__name__}")
        finally:
            if tmp_path:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_path)

    # -- ledger: a corrupt ledger fail-closes every keyed send, so it
    # is a chain failure here, not health's warning.
    try:
        book = _load_send_ledger()
    except LedgerCorrupt as e:
        _check("ledger", "FAIL", f"ledger corrupt: {e}")
    except Exception as e:
        _check("ledger", "FAIL", f"ledger unreadable: {type(e).__name__}")
    else:
        _check("ledger", "PASS", f"{len(book)} entries")

    for name, status, detail in results:
        print(f"SMOKE {name}: {status}" + (f" ({detail})" if detail else ""))
    failed = [n for n, st, _ in results if st == "FAIL"]
    if failed:
        print(f"SMOKE_FAILED: {', '.join(failed)}")
        return 1
    print("SMOKE_OK")
    return 0


def _hook_state_dir():
    """Canonical hook state dir — same expression and default as
    cmd_health and the example hook script (ILINK_HOOK_STATE is the
    legacy alias)."""
    return (os.environ.get("ILINK_HOOK_STATE_DIR")
            or os.environ.get("ILINK_HOOK_STATE")
            or os.path.expanduser("~/hooks/state"))


def cmd_watchdog(max_age_mins=WATCHDOG_MAX_AGE_MINS):
    """Dead-man's switch for the hook driver. The hook touches
    ilink-heartbeat on every run (quiet runs included — they write
    nothing to poll.log, so the heartbeat is the only proof the hook
    is alive). If the heartbeat is older than max_age_mins, or has
    never existed, the hook is presumed dead. Alert transitions are
    tracked in ilink-watchdog-state.json so a cron driver stays
    silent on healthy runs, reports the first stale sighting once
    (WATCHDOG_ALERT), repeats only as WATCHDOG_STALE, and reports
    recovery once (WATCHDOG_RECOVERED). A maintenance flag file
    ilink-watchdog-off silences everything — touch it while the
    bridge is intentionally stopped, remove it after. Exit 0 when
    fresh, recovered or disabled; 1 when stale."""
    state_dir = _hook_state_dir()
    if os.path.exists(os.path.join(state_dir, "ilink-watchdog-off")):
        print("WATCHDOG_DISABLED")
        return 0
    hb = os.path.join(state_dir, "ilink-heartbeat")
    now = int(time.time())
    try:
        age = now - int(os.stat(hb).st_mtime)
    except OSError:
        age = None
    state_path = os.path.join(state_dir, "ilink-watchdog-state.json")
    try:
        with open(state_path) as f:
            alerted = bool(json.load(f).get("alerted"))
    except Exception:
        alerted = False

    def _set_alerted(value):
        try:
            _atomic_write(state_path,
                          lambda f: json.dump({"alerted": value}, f),
                          mode=None)
        except Exception:
            pass

    if age is None or age > max_age_mins * 60:
        detail = ("no heartbeat yet" if age is None
                  else f"heartbeat {_fmt_age(age)} old")
        if not alerted:
            _set_alerted(True)
            print(f"WATCHDOG_ALERT:{detail}")
        else:
            print(f"WATCHDOG_STALE:{detail}")
        return 1
    if alerted:
        _set_alerted(False)
        print("WATCHDOG_RECOVERED")
        return 0
    print(f"WATCHDOG_OK:heartbeat {_fmt_age(age)} old")
    return 0


def cmd_rotate_chatlog():
    """Archive chatlog.md lines from past months into per-month files
    (chatlog-YYYY-MM.md), keeping current-month and undated lines in
    place. Idempotent: a second run finds nothing old and prints
    ROTATE_NOTHING. The worker reads the last 30 lines of chatlog.md,
    which rotation never breaks — current-month lines stay, and
    archives are named by month for manual lookup. A missing chatlog
    is ROTATE_NOTHING, not an error. Exit 0 on success or nothing to
    do, 1 only on a write failure."""
    path = os.path.join(BRIDGE, "chatlog.md")
    try:
        with open(path) as f:
            lines = f.readlines()
    except FileNotFoundError:
        print("ROTATE_NOTHING:no chatlog")
        return 0
    cur_month = time.strftime("%Y-%m", time.localtime())
    keep = []
    archived = {}
    for line in lines:
        m = re.match(r"\[(\d{4}-\d{2})-\d{2}", line)
        month = m.group(1) if m else None
        if month and month != cur_month:
            archived.setdefault(month, []).append(line)
        else:
            keep.append(line)
    if not archived:
        print("ROTATE_NOTHING:nothing from past months")
        return 0
    try:
        for month, mls in sorted(archived.items()):
            with open(os.path.join(BRIDGE, f"chatlog-{month}.md"),
                      "a") as f:
                f.writelines(mls)
        tmp = path + ".tmp-rotate"
        with open(tmp, "w") as f:
            f.writelines(keep)
        os.replace(tmp, path)
    except OSError as e:
        print(f"ROTATE_FAILED:{type(e).__name__}")
        return 1
    total = sum(len(v) for v in archived.values())
    print(f"ROTATE_OK:archived {total} line(s) into "
          f"{len(archived)} month file(s)")
    return 0


def _newest_backup(target):
    """Newest '<target>.bak-*' file by mtime, or None. Covers both
    backup schemes: update-to's '.bak-<tag>' and the port script's
    '.bak-<date>-<label>'. Pre-rollback copies ('.bak-pre-rollback-*')
    are valid sources too — rolling back twice returns to the state
    before the first rollback."""
    d = os.path.dirname(target) or "."
    base = os.path.basename(target) + ".bak-"
    try:
        names = os.listdir(d)
    except OSError:
        return None
    cands = [os.path.join(d, n) for n in names
             if n.startswith(base) and os.path.isfile(os.path.join(d, n))]
    if not cands:
        return None
    return max(cands, key=lambda p: os.stat(p).st_mtime)


def cmd_rollback(hook_dir=None, script_path=None):
    """Restore ilink.py (and, with --hook-dir, the hook scripts) from
    the newest .bak-* backup next to each file — the backups update-to
    and the port script leave behind. All-or-nothing: every target
    must have a backup and every backup must verify (compile check
    for ilink.py, bash -n for hook scripts) before anything is
    swapped. Current files are first copied to
    '.bak-pre-rollback-<epoch>' so a rollback is itself reversible.
    State files are never touched (same rule as update-to), and the
    worker contract (AGENTS.md) is not rolled back — it is manual
    anyway. After swapping, the restored script's health --offline
    runs best-effort and its verdict is reported (HEALTH:<verdict>)
    without changing the exit code. Prints ROLLBACK_OK:<backup> plus
    RESTORED lines, or ROLLBACK_FAILED:<reason>. Exit 0/1."""
    if script_path is None:
        script_path = os.path.abspath(__file__)
    targets = [("ilink.py", script_path)]
    if hook_dir:
        for name in HOOK_SCRIPT_FILES:
            targets.append((name, os.path.join(hook_dir, name)))
    plan = []  # (kind, target, backup, content)
    for kind, target in targets:
        backup = _newest_backup(target)
        if backup is None:
            print(f"ROLLBACK_FAILED:no-backup:{kind}")
            return 1
        try:
            with open(backup) as f:
                content = f.read()
        except OSError as e:
            print(f"ROLLBACK_FAILED:read:{kind}:{type(e).__name__}")
            return 1
        err = (_verify_python_source(content) if kind == "ilink.py"
               else _verify_shell_source(content))
        if err:
            print(f"ROLLBACK_FAILED:verify:{kind}:{err[:200]}")
            return 1
        plan.append((kind, target, backup, content))
    for kind, target, backup, content in plan:
        if os.path.exists(target):
            _copy_file(target,
                       target + f".bak-pre-rollback-{int(time.time())}")
        tmp = target + f".tmp-rollback-{os.getpid()}"
        try:
            mode = os.stat(target).st_mode & 0o777
        except OSError:
            mode = 0o644
        with open(tmp, "w") as f:
            f.write(content)
        os.chmod(tmp, mode)
        os.replace(tmp, target)
    # Best-effort health check of the restored script: it proves the
    # restored file not only compiles but runs. Informational only —
    # a stale token makes health return 2, which must not fail a
    # restore that itself succeeded, so the verdict never changes
    # this command's exit code.
    health_verdict = None
    try:
        import subprocess
        proc = subprocess.run(
            [sys.executable, script_path, "health", "--offline"],
            capture_output=True, text=True, timeout=30)
        for line in proc.stdout.splitlines():
            if line.startswith("verdict: "):
                health_verdict = line.split("verdict: ", 1)[1]
                break
    except Exception:
        pass
    print(f"ROLLBACK_OK:{os.path.basename(plan[0][2])}")
    for kind, target, backup, content in plan:
        print(f"RESTORED:{kind} from {os.path.basename(backup)}")
    if health_verdict:
        print(f"HEALTH:{health_verdict}")
    return 0


def cmd_stats():
    """Offline snapshot from local files only (no server calls, so it
    works while the bridge is down). Today's chatlog exchanges and
    send-audit outcomes (local-midnight boundary), send-ledger size,
    quote-cache size, media dir usage, session age, and queue backlog
    (same strict queue rule as health). A missing file is a zero,
    never a failure — stats is a diagnostic, so exit code is always
    0."""
    today = time.strftime("%Y-%m-%d", time.localtime())
    try:
        today_start = int(time.mktime(time.strptime(today, "%Y-%m-%d")))
    except Exception:
        today_start = 0
    out = ["STATS"]
    user_n = bot_n = 0
    try:
        with open(os.path.join(BRIDGE, "chatlog.md")) as f:
            for line in f:
                if line.startswith(f"[{today}"):
                    if "] 用户：" in line:
                        user_n += 1
                    elif "] 我：" in line:
                        bot_n += 1
    except OSError:
        pass
    out.append(f"chatlog today: {user_n} user / {bot_n} bot lines")
    ok_n = fail_n = 0
    try:
        with open(SEND_AUDIT_FILE) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if int(entry.get("ts", 0) or 0) < today_start:
                        continue
                    if entry.get("result") == "ok":
                        ok_n += 1
                    else:
                        fail_n += 1
                except (ValueError, AttributeError, TypeError):
                    # A corrupt audit line (non-numeric ts, a JSON
                    # array instead of an object, ...) must never
                    # crash stats: skip it and keep counting.
                    continue
    except OSError:
        pass
    out.append(f"sends today: {ok_n} ok / {fail_n} failed")
    try:
        out.append(f"ledger: {len(_load_send_ledger())} entries")
    except LedgerCorrupt:
        out.append("ledger: CORRUPT (fail-closed, see health)")
    except Exception:
        out.append("ledger: unreadable")
    try:
        cache = _load_quote_cache()
        out.append(f"quote cache: {len(cache['by_id'])} entries, "
                   f"{len(cache['by_svr'])} svr mappings")
    except Exception:
        out.append("quote cache: unreadable")
    try:
        names = os.listdir(MEDIA_DIR)
        total = 0
        for name in names:
            try:
                total += os.path.getsize(os.path.join(MEDIA_DIR, name))
            except OSError:
                pass
        out.append(f"media: {len(names)} file(s), "
                   f"{total / (1024 * 1024):.1f} MB")
    except OSError:
        out.append("media: no dir yet")
    try:
        sess = load_session()
        if sess and sess.get("login_at"):
            out.append("session: logged in "
                       f"({_fmt_age(int(time.time()) - int(sess['login_at']))} ago)")
        elif sess:
            out.append("session: present (no login_at)")
        else:
            out.append("session: none")
    except Exception:
        out.append("session: unreadable")
    pend_n, pend_corrupt = _queue_count(
        os.path.join(_hook_state_dir(), "ilink-pending.json"))
    if pend_corrupt:
        out.append("queue: pending CORRUPT (hook quarantines it)")
    else:
        out.append(f"queue: {pend_n} pending")
    for line in out:
        print(line if line == "STATS" else f"STATS {line}")
    return 0


def cmd_preflight():
    """Pre-login environment check for first-time deployers: Python
    version (>= 3.10), cryptography importable (media decryption
    depends on it — a missing module once broke decryption silently),
    network reachability to the iLink API host (any HTTP response at
    all, even an error status, proves network + TLS work), and data /
    media dir writability. No login needed, no state touched.
    Prints PREFLIGHT lines + PREFLIGHT_OK / PREFLIGHT_FAILED.
    Exit 0/1."""
    results = []

    def _check(name, ok, detail):
        results.append((name, ok, detail))

    if sys.version_info >= (3, 10):
        _check("python", True, ".".join(map(str, sys.version_info[:3])))
    else:
        _check("python", False,
               f"{'.'.join(map(str, sys.version_info[:3]))} < 3.10")
    try:
        import cryptography  # noqa: F401  (presence is the check)
        _check("cryptography", True, "importable")
    except ImportError:
        _check("cryptography", False,
               "not installed (pip install cryptography)")
    try:
        urllib.request.urlopen(BASE_URL, timeout=8)
        _check("network", True, "ilinkai reachable")
    except urllib.error.HTTPError:
        _check("network", True, "ilinkai answered (HTTP error status)")
    except Exception as e:
        _check("network", False, f"unreachable: {type(e).__name__}")
    for name, path in (("data dir", BRIDGE), ("media dir", MEDIA_DIR)):
        if os.path.isdir(path) and os.access(path, os.W_OK):
            _check(name, True, path)
        else:
            _check(name, False, f"{path} not writable")
    for name, ok, detail in results:
        print(f"PREFLIGHT {name}: {'PASS' if ok else 'FAIL'} ({detail})")
    failed = [n for n, ok, _ in results if not ok]
    if failed:
        print(f"PREFLIGHT_FAILED: {', '.join(failed)}")
        return 1
    print("PREFLIGHT_OK")
    return 0


def _pop_key(args):
    """Extract a trailing '--key <id>' pair from an argv tail.
    Returns (key_or_None, remaining_args). A '--key' with no following
    value is ignored rather than crashing."""
    rest = list(args)
    key = None
    if "--key" in rest:
        i = rest.index("--key")
        if i + 1 < len(rest):
            key = rest[i + 1]
            del rest[i:i + 2]
    return key, rest


def _int_arg(value, usage):
    """Parse an int CLI arg; on failure print usage and return None
    (callers return exit 2) instead of an uncaught ValueError."""
    try:
        return int(value)
    except (TypeError, ValueError):
        print(f"usage: {usage}", file=sys.stderr)
        return None


def _need(argv, n, usage):
    """Arg-count guard: missing args print usage to stderr and exit 2
    instead of raising a bare IndexError traceback."""
    if len(argv) < n:
        print(f"usage: {usage}", file=sys.stderr)
        return False
    return True


def main(argv):
    """CLI dispatch. Returns an exit code (does not call sys.exit itself,
    so tests can call it directly)."""
    cmd = argv[1] if len(argv) > 1 else ""
    if cmd == "qrcode":
        cmd_qrcode()
        return 0
    if cmd == "login":
        if not _need(argv, 3, "ilink.py login <qrcode> [wait_secs]"):
            return 2
        if len(argv) > 3:
            wait = _int_arg(argv[3], "ilink.py login <qrcode> [wait_secs]")
            if wait is None:
                return 2
        else:
            wait = 300
        return cmd_login(argv[2], wait)
    if cmd == "bind":
        if len(argv) > 2:
            secs = _int_arg(argv[2], "ilink.py bind [seconds]")
            if secs is None:
                return 2
        else:
            secs = BIND_DEFAULT_SECONDS
        return cmd_bind(secs)
    if cmd == "poll":
        args = argv[2:]
        cursor = None
        if "--cursor" in args:
            i = args.index("--cursor")
            if i + 1 >= len(args):
                print("usage: ilink.py poll [--cursor <buf>]", file=sys.stderr)
                return 2
            cursor = args[i + 1]
        return cmd_poll(cursor)
    if cmd == "commit":
        return cmd_commit()
    if cmd == "claim":
        args = argv[2:]
        if args and args[0] == "--merge":
            return cmd_claim(merge=True,
                             claim_id=args[1] if len(args) > 1 else None)
        return cmd_claim()
    if cmd == "release":
        return cmd_release(argv[2] if len(argv) > 2 else None)
    if cmd == "health":
        return cmd_health(offline="--offline" in argv)
    if cmd == "smoke-test":
        no_send = "--no-send" in argv
        to = None
        args = [a for a in argv[2:] if a != "--no-send"]
        for i, a in enumerate(args):
            if a == "--to" and i + 1 < len(args):
                to = args[i + 1]
        return cmd_smoke_test(no_send=no_send, to_user_id=to)
    if cmd == "watchdog":
        age = WATCHDOG_MAX_AGE_MINS
        args = argv[2:]
        for i, a in enumerate(args):
            if a == "--max-age-mins" and i + 1 < len(args):
                parsed = _int_arg(args[i + 1],
                                  "ilink.py watchdog [--max-age-mins N]")
                if parsed is None:
                    return 2
                age = parsed
        return cmd_watchdog(age)
    if cmd == "rotate-chatlog":
        return cmd_rotate_chatlog()
    if cmd == "rollback":
        hook_dir = None
        args = argv[2:]
        for i, a in enumerate(args):
            if a == "--hook-dir" and i + 1 < len(args):
                hook_dir = args[i + 1]
        return cmd_rollback(hook_dir=hook_dir)
    if cmd == "stats":
        return cmd_stats()
    if cmd == "preflight":
        return cmd_preflight()
    if cmd == "prune-media":
        dry_run = "--dry-run" in argv
        age = MEDIA_PRUNE_MAX_AGE_DAYS
        total_mb = MEDIA_PRUNE_MAX_TOTAL_BYTES // (1024 * 1024)
        args = [a for a in argv[2:] if a != "--dry-run"]
        for i, a in enumerate(args):
            if a == "--max-age-days" and i + 1 < len(args):
                parsed = _int_arg(args[i + 1],
                                  "ilink.py prune-media [--max-age-days N]")
                if parsed is None:
                    return 2
                age = parsed
            if a == "--max-total-mb" and i + 1 < len(args):
                parsed = _int_arg(args[i + 1],
                                  "ilink.py prune-media [--max-total-mb N]")
                if parsed is None:
                    return 2
                total_mb = parsed
        return cmd_prune_media(age, total_mb * 1024 * 1024, dry_run)
    if cmd == "version-check":
        return cmd_version_check(notify="--notify" in argv)
    if cmd == "update-to":
        if not _need(argv, 3, "ilink.py update-to <tag> [--hook-dir <dir>]"):
            return 2
        hook_dir = None
        args = argv[3:]
        for i, a in enumerate(args):
            if a == "--hook-dir" and i + 1 < len(args):
                hook_dir = args[i + 1]
        return cmd_update_to(argv[2], hook_dir=hook_dir)
    if cmd == "subscribe":
        if not _need(argv, 3, "ilink.py subscribe <owner_id>"):
            return 2
        return cmd_subscribe(argv[2])
    if cmd == "unsubscribe":
        if not _need(argv, 3, "ilink.py unsubscribe <owner_id>"):
            return 2
        return cmd_unsubscribe(argv[2])
    if cmd == "send":
        if not _need(argv, 5, "ilink.py send <to> <ctx> <text> [--key <id>]"):
            return 2
        key, _ = _pop_key(argv[5:])
        return cmd_send(argv[2], argv[3], argv[4], key=key)
    if cmd == "proactive":
        if not _need(argv, 4, "ilink.py proactive <to> <text> [--key <id>]"):
            return 2
        key, _ = _pop_key(argv[4:])
        return cmd_proactive(argv[2], argv[3], key=key)
    if cmd == "typing":
        if not _need(argv, 4, "ilink.py typing <to> <start|stop>"):
            return 2
        return cmd_typing(argv[2], argv[3])
    if cmd in ("send-image", "send-file", "send-video"):
        if not _need(argv, 5, f"ilink.py {cmd} <to> <ctx> <path> [--key <id>]"):
            return 2
        key, _ = _pop_key(argv[5:])
        return cmd_send_media(argv[2], argv[3], argv[4],
                              {"send-image": 2, "send-file": 4,
                               "send-video": 5}[cmd], key=key)
    if cmd == "send-voice":
        if not _need(argv, 5, "ilink.py send-voice <to> <ctx> <path> [playtime_ms] [--key <id>]"):
            return 2
        key, rest = _pop_key(argv[5:])
        if rest:
            playtime = _int_arg(rest[0], "ilink.py send-voice <to> <ctx> <path> [playtime_ms]")
            if playtime is None:
                return 2
        else:
            playtime = 0
        return cmd_send_media(argv[2], argv[3], argv[4], 3, playtime, key=key)
    if cmd in ("proactive-image", "proactive-file", "proactive-video"):
        if not _need(argv, 4, f"ilink.py {cmd} <to> <path> [--key <id>]"):
            return 2
        key, _ = _pop_key(argv[4:])
        return cmd_proactive_media(argv[2], argv[3],
                                   {"proactive-image": 2, "proactive-file": 4,
                                    "proactive-video": 5}[cmd], key=key)
    if cmd == "proactive-voice":
        if not _need(argv, 4, "ilink.py proactive-voice <to> <path> [playtime_ms] [--key <id>]"):
            return 2
        key, rest = _pop_key(argv[4:])
        if rest:
            playtime = _int_arg(rest[0], "ilink.py proactive-voice <to> <path> [playtime_ms]")
            if playtime is None:
                return 2
        else:
            playtime = 0
        return cmd_proactive_media(argv[2], argv[3], 3, playtime, key=key)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
