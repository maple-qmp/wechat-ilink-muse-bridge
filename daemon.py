#!/usr/bin/env python3
"""Standalone long-poll daemon for the iLink WeChat bot.

Reference loop used by wechat-ilink-muse-bridge: keep calling
`ilink.py poll` back-to-back. Each poll is a *long* poll — the server
holds the connection for up to ~35s and returns the instant a message
arrives — so this loop is effectively push, with second-level latency.

It also implements burst merging: when a user sends several messages
seconds apart ("吃完了" / "吃干净了"), the iLink server often returns
them in separate poll responses. Instead of replying to each one, the
daemon keeps collecting while messages keep arriving and dispatches the
whole burst as ONE user turn. (None of the popular iLink SDKs do this;
bots built on raw per-message dispatch feel robotic.)

MERGE_WINDOW is an IDLE window, not a total cap: every new message
pushes the deadline out another MERGE_WINDOW seconds, so a chatty burst
can collect for much longer than MERGE_WINDOW in total. It is also
approximate rather than strict: the loop only re-checks the deadline
between polls, and a single long-poll can run past the remaining
window (poll_once's own timeout is 180s).

Positioning: this is the EXAMPLE driver. It exists to show the wire
protocol and the smallest runnable loop — not to be a production-grade
receiver. Its delivery semantics are deliberately simple:

  - handle-then-commit: a batch is committed only after the handler
    returns normally. A crash or a handler exception mid-burst leaves
    the cursor un-advanced, so the batch is re-fetched next round —
    at-least-once delivery: duplicates are possible, but the daemon
    itself never silently drops a batch it has not handled.
  - a failed commit is logged (COMMIT_FAILED) and recovered the same
    way, by re-fetch on the next round.

For production-grade delivery (durable on-disk pending queue, crash
recovery, closed-loop alerting), use the Muse hook driver in
examples/muse-hook/ instead.

Usage:
  python3 ilink.py qrcode && python3 ilink.py login <qrcode>   # once
  python3 ilink.py bind          # once: 10-min window, first sender = owner
  python3 daemon.py              # log incoming bursts, reply nothing
  python3 daemon.py --echo       # reply once per burst (demo handler)
  python3 daemon.py --llm        # reply with an LLM (example handler)

Environment:
  MERGE_WINDOW   idle-gap seconds for burst collecting (default 25;
                 approximate, not a strict cap — see collect_burst)
  ILINK_DATA_DIR data dir forwarded to ilink.py (default ./data)
  ILINK_HOOK_STATE_DIR
                 hook state dir holding ilink-driver.lock and
                 ilink-heartbeat (legacy ILINK_HOOK_STATE also honored;
                 default ~/hooks/state). The hook driver must use the
                 same dir, or the shared driver lock does not apply.
  ILINK_DRIVER_HEARTBEAT_STALE_SECS
                 hook-heartbeat freshness window for the daemon
                 startup gate (default 120)
  LLM_API_KEY    API key for --llm mode (required there)
  LLM_BASE_URL   OpenAI-compatible base URL (default https://api.openai.com/v1)
  LLM_MODEL      model name (default gpt-4o-mini)
  LLM_SYSTEM_PROMPT  system prompt for --llm mode (default: a brief
                 Chinese assistant persona)
"""
import json
import os
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request

try:
    import fcntl
except ImportError:  # non-Unix platforms have no flock; see below
    fcntl = None

HERE = os.path.dirname(os.path.abspath(__file__))
MERGE_WINDOW = float(os.environ.get("MERGE_WINDOW", "25"))

# Shared driver lock (daemon vs. the Muse hook driver). Previously the
# "hook and daemon must not run together" rule lived only in AGENTS.md
# prose, and nothing stopped an accidental second driver from polling
# the same account: wasted compute, double wakes, and two drivers
# fighting over the same batches (the send ledger usually, but not
# always, caught the duplicate replies). Now the rule is a lock:
#
#   * The lock file is $STATE_DIR/ilink-driver.lock, where STATE_DIR is
#     the hook state dir (ILINK_HOOK_STATE_DIR, legacy ILINK_HOOK_STATE,
#     or ~/hooks/state — the same canonical dir `ilink.py health`
#     inspects). Both drivers MUST point at the same state dir, or
#     they are not sharing a lock at all.
#   * The daemon holds the lock for its whole lifetime (non-blocking
#     acquire at startup; a held lock means another driver — daemon or
#     hook — is already running, so this daemon exits with code 2).
#   * The hook holds the same lock only for the duration of one run
#     (it is a short-lived script, not a resident process). A hook run
#     that cannot acquire it exits quietly WITHOUT touching the
#     heartbeat, polling, or waking — the daemon keeps the driver role
#     until it stops.
#   * The daemon ALSO refuses to start when the hook heartbeat
#     (ilink-heartbeat, touched by every hook run) is fresh, because
#     a hook run holds the driver lock only intermittently: checking
#     the lock alone at daemon startup would let the daemon steal the
#     driver role in a gap between hook runs. First driver wins;
#     latecomers stay quiet. The daemon never touches the heartbeat
#     itself — that file is the hook's liveness signal, and a daemon
#     refreshing it would fool the next daemon startup into thinking
#     the hook is alive.
DRIVER_LOCK_NAME = "ilink-driver.lock"
HOOK_HEARTBEAT_NAME = "ilink-heartbeat"
HOOK_HEARTBEAT_STALE_SECS = float(
    os.environ.get("ILINK_DRIVER_HEARTBEAT_STALE_SECS", "120"))


def hook_state_dir():
    """Hook state dir, with the same precedence as the hook script."""
    return (
        os.environ.get("ILINK_HOOK_STATE_DIR")
        or os.environ.get("ILINK_HOOK_STATE")
        or os.path.expanduser("~/hooks/state")
    )


def hook_heartbeat_fresh(state_dir=None, stale_secs=HOOK_HEARTBEAT_STALE_SECS,
                         now=None):
    """True iff the hook driver touched its heartbeat recently.

    Missing, unreadable, or stale heartbeat files mean "hook not
    running" — never an error. `now` is injectable for tests.
    """
    if state_dir is None:
        state_dir = hook_state_dir()
    try:
        mtime = os.path.getmtime(os.path.join(state_dir, HOOK_HEARTBEAT_NAME))
    except OSError:
        return False
    if now is None:
        now = time.time()
    return (now - mtime) <= stale_secs


def try_acquire_driver_lock(lock_path):
    """Non-blocking exclusive flock on lock_path.

    Returns the open file object (the caller must keep it alive for as
    long as the lock must be held — closing it releases the lock), or
    None when another process already holds the lock. Raises OSError
    for anything else (missing directory, permission denied, no flock
    support), so the caller can decide on a fallback instead of
    silently running unlocked.
    """
    if fcntl is None:
        raise OSError("fcntl unavailable on this platform")
    f = open(lock_path, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.close()
        return None
    except OSError:
        f.close()
        raise
    return f


def acquire_driver_lock_or_exit():
    """Daemon startup gate: refuse to run if another driver is active.

    Returns the held lock file object on success (kept alive by the
    caller for the daemon's lifetime). Exits with code 2 when another
    driver wins, and continues WITHOUT a shared lock (with a warning)
    only when no lock file can be created anywhere — the example
    daemon must still work for users with no hook runtime at all.
    """
    state_dir = hook_state_dir()
    if hook_heartbeat_fresh(state_dir):
        print("DRIVER_LOCK: hook driver is active (fresh ilink-heartbeat "
              f"in {state_dir}); refusing to start daemon. Stop the hook "
              "driver first if you really want the daemon.", flush=True)
        sys.exit(2)
    lock_path = os.path.join(state_dir, DRIVER_LOCK_NAME)
    try:
        os.makedirs(state_dir, exist_ok=True)
        lock = try_acquire_driver_lock(lock_path)
        if lock is None:
            print("DRIVER_LOCK: another driver already holds "
                  f"{lock_path}; refusing to start.", flush=True)
            sys.exit(2)
        return lock
    except OSError as e:
        # State dir unusable: fall back to a data-dir lock. It still
        # blocks a second daemon, but the hook (which only checks the
        # state dir) will NOT see it — documented limitation, and the
        # warning says so instead of pretending otherwise.
        fallback = os.path.join(DATA_DIR, DRIVER_LOCK_NAME)
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            lock = try_acquire_driver_lock(fallback)
            if lock is None:
                print("DRIVER_LOCK: another daemon already holds "
                      f"{fallback}; refusing to start.", flush=True)
                sys.exit(2)
            print("DRIVER_LOCK: state dir unusable "
                  f"({e}); using fallback lock {fallback} "
                  "(the hook driver will not see it).", flush=True)
            return lock
        except OSError as e2:
            print("DRIVER_LOCK: cannot acquire any driver lock "
                  f"({e2}); continuing without a shared lock.",
                  flush=True)
            return None


def poll_once(cursor=None):
    """One long-poll. Returns (status, msgs, commit).

    The commit block is NOT submitted here. The caller hands the batch
    to the handler first and commits only after handling succeeds
    (handle-then-commit — see the module docstring for the semantics).
    A poll that fails or expires carries commit=None.

    cursor (optional) continues from an uncommitted earlier poll — see
    collect_burst; None reads the committed cursor from session.json.
    """
    args = [sys.executable, os.path.join(HERE, "ilink.py"), "poll"]
    if cursor:
        args += ["--cursor", cursor]
    r = subprocess.run(args, capture_output=True, text=True, timeout=180)
    out = (r.stdout or "").strip()
    if out.startswith("NO_TOKEN") or out.startswith("TOKEN_EXPIRED"):
        return "expired", [], None
    if out.startswith("POLL_ERROR"):
        return "error", [], None
    try:
        data = json.loads(out)
    except ValueError:
        return "error", [], None
    return "ok", data.get("msgs") or [], data.get("commit")


def commit_batch(commit):
    """Submit one poll commit block via `ilink.py commit`.

    Returns True only when the subprocess exits 0 and prints COMMIT_OK.
    Any failure (nonzero exit, missing COMMIT_OK, subprocess error) is
    logged as COMMIT_FAILED and returns False — never raises. The
    cursor has not advanced, so the batch is re-fetched next round:
    that re-delivery is the recovery mechanism, not an error path.
    """
    if not commit:
        return True
    try:
        r = subprocess.run(
            [sys.executable, os.path.join(HERE, "ilink.py"), "commit"],
            input=json.dumps(commit), capture_output=True, text=True,
            timeout=30,
        )
    except Exception as e:
        print(f"COMMIT_FAILED: commit subprocess error: {e}; "
              "batch will be re-fetched next round", flush=True)
        return False
    if r.returncode != 0 or "COMMIT_OK" not in (r.stdout or ""):
        detail = (r.stdout or "").strip()[:200] or (r.stderr or "").strip()[:200]
        print(f"COMMIT_FAILED: ilink.py commit exited {r.returncode}: "
              f"{detail}; batch will be re-fetched next round", flush=True)
        return False
    return True


def collect_burst(first_msgs, first_commits):
    """Keep polling until MERGE_WINDOW elapses with no new messages.

    MERGE_WINDOW is an idle window, not a total budget: each poll that
    returns new messages pushes the deadline out another MERGE_WINDOW.
    It is also approximate: the deadline is only re-checked between
    polls, and one long-poll (timeout 180s) can overshoot it.

    Returns (msgs, commits): the merged burst and the commit block of
    every poll that fed it, in poll order, all still unsubmitted.

    In-burst dedupe by message_id: every poll in this loop runs before
    any commit, so each poll subprocess re-reads the un-advanced cursor
    from session.json, and filter_seen only filters ids already
    persisted at commit time. If the server replays from the old
    cursor, a later poll returns the same batch again — without this
    set it would land in the burst twice and inflate the reply
    context. Entries without a message_id are kept as-is (never
    dropped on a missing key); duplicate commit blocks are harmless
    (commit is idempotent) and kept in order.
    """
    seen_ids = set()

    def _fresh(batch):
        out = []
        for m in batch:
            mid = m.get("message_id") if isinstance(m, dict) else None
            if mid is None:
                out.append(m)
                continue
            try:
                dup = mid in seen_ids
            except TypeError:
                out.append(m)  # unhashable id: keep, never drop
                continue
            if dup:
                continue
            seen_ids.add(mid)
            out.append(m)
        return out

    msgs = _fresh(first_msgs)
    commits = list(first_commits)

    def _cursor():
        # Continue from the newest uncommitted poll: re-polling from the
        # committed cursor made the server replay the same batch at once
        # (no long-poll hold), so the merge window spun hot and every
        # round re-downloaded the batch's media.
        for c in reversed(commits):
            if isinstance(c, dict) and c.get("cursor"):
                return c["cursor"]
        return None

    deadline = time.time() + MERGE_WINDOW
    while time.time() < deadline:
        status, more, commit = poll_once(_cursor())
        if status == "ok" and more:
            fresh = _fresh(more)
            msgs.extend(fresh)
            if commit:
                commits.append(commit)
            # Only genuinely new messages extend the idle window: a
            # replay-only poll (all duplicates) must not push the
            # deadline out forever under a replaying server.
            if fresh:
                deadline = time.time() + MERGE_WINDOW  # burst still going
        elif status == "ok":
            if commit:
                commits.append(commit)
            break  # a full long-poll came back empty: burst is over
        else:
            time.sleep(2)
    return msgs, commits


def _typing(user, word):
    """Best-effort 'typing...' indicator; failures are ignored by design."""
    try:
        subprocess.run(
            [sys.executable, os.path.join(HERE, "ilink.py"), "typing", user, word],
            capture_output=True, timeout=30,
        )
    except Exception:
        pass


def handle_burst(msgs, echo=False, llm=False):
    """One user turn. Replace this with your own agent/LLM call."""
    by_user = {}
    for m in msgs:
        by_user.setdefault(m["from_user_id"], []).append(m)
    for user, items in by_user.items():
        texts = [i.get("text") or f"[{i.get('kind')}]" for i in items]
        print(f"[{time.strftime('%H:%M:%S')}] {user}: " + " / ".join(texts), flush=True)
        if not (echo or llm):
            continue
        ctx = items[-1]["context_token"]  # always reply with the latest ctx
        if llm:
            reply = llm_reply(user, " ".join(texts))
        else:
            reply = "收到：" + " ".join(texts)
        _typing(user, "start")  # show "typing..." while the reply goes out
        try:
            # The reply is keyed by this user's first message of the
            # burst, so a re-delivery after a skipped commit reuses the
            # ledger intent instead of sending twice. A nonzero exit
            # (SEND_FAILED / SEND_ERROR / TOKEN_EXPIRED) must raise:
            # silently ignoring it used to commit the batch anyway and
            # lose the reply. Timeout/OSError still raise on their own.
            send_args = [sys.executable, os.path.join(HERE, "ilink.py"),
                         "send", user, ctx, "-"]
            first_id = items[0].get("message_id")
            if first_id is not None:
                send_args += ["--key", str(first_id)]
            r = subprocess.run(
                send_args, input=reply, text=True, timeout=120,
                capture_output=True,
            )
            if r.returncode != 0:
                detail = ((r.stdout or "") + (r.stderr or "")).strip()[:200]
                raise RuntimeError(
                    f"send to {user} exited {r.returncode}: {detail}")
        finally:
            _typing(user, "stop")


# ---------------------------------------------------------------------------
# Example LLM handler (--llm): any OpenAI-compatible chat-completions API.
# Works with OpenAI itself, DeepSeek, Qwen (DashScope compatible mode),
# Moonshot, a local Ollama (LLM_BASE_URL=http://localhost:11434/v1,
# LLM_API_KEY can be any string), etc. Stdlib only — no extra dependency.
# Per-user history (last LLM_HISTORY messages) is persisted to
# llm_history.json in the data dir, so a daemon restart continues the
# conversation instead of starting fresh; delete that file to reset.
# Swap this function out for your own agent call if you need real
# tools/memory — that is what the Muse hook setup in
# examples/muse-hook/ does.
# ---------------------------------------------------------------------------

LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
LLM_SYSTEM_PROMPT = os.environ.get(
    "LLM_SYSTEM_PROMPT", "你是一个微信聊天助手，回复简短、口语化，用中文。"
)
LLM_HISTORY = 10  # messages per user kept as context
DATA_DIR = os.environ.get("ILINK_DATA_DIR", "./data")
HISTORY_FILE = os.path.join(DATA_DIR, "llm_history.json")
_history = None  # loaded lazily from HISTORY_FILE on first use


def _load_history():
    global _history
    if _history is None:
        try:
            with open(HISTORY_FILE) as f:
                data = json.load(f)
            _history = data if isinstance(data, dict) else {}
        except Exception:
            _history = {}  # missing or corrupt: start fresh, never crash
    return _history


def _save_history():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = f"{HISTORY_FILE}.tmp-{os.getpid()}"
        with open(tmp, "w") as f:
            json.dump(_history, f, ensure_ascii=False)
        os.chmod(tmp, 0o600)
        os.replace(tmp, HISTORY_FILE)
    except Exception as e:
        print(f"[llm] could not persist history: {e}", flush=True)


def llm_reply(user, text):
    """Send one user turn to the LLM and return its reply text."""
    history = _load_history()
    hist = history.setdefault(user, [])
    messages = (
        [{"role": "system", "content": LLM_SYSTEM_PROMPT}]
        + hist
        + [{"role": "user", "content": text}]
    )
    req = urllib.request.Request(
        LLM_BASE_URL + "/chat/completions",
        data=json.dumps({"model": LLM_MODEL, "messages": messages}).encode(),
        headers={
            "Authorization": f"Bearer {LLM_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
        reply = data["choices"][0]["message"]["content"].strip()
    except Exception as e:  # network error, bad key, unexpected payload...
        print(f"[llm] request failed: {e}", flush=True)
        return "（LLM 调用失败，看一下 daemon 的日志）"
    hist += [
        {"role": "user", "content": text},
        {"role": "assistant", "content": reply},
    ]
    history[user] = hist[-LLM_HISTORY:]
    _save_history()
    return reply


def main():
    llm = "--llm" in sys.argv
    echo = "--echo" in sys.argv and not llm
    if llm and not LLM_API_KEY:
        print(
            "LLM_API_KEY is not set. Example:\n"
            "  LLM_API_KEY=sk-... python3 daemon.py --llm\n"
            "Optional: LLM_BASE_URL, LLM_MODEL, LLM_SYSTEM_PROMPT (see docstring).",
            flush=True,
        )
        sys.exit(1)
    # Shared driver lock: refuse to become a second driver. The held
    # lock object must stay alive for the daemon's lifetime (closing
    # it releases the flock), so it is kept in a module-global.
    global _driver_lock
    _driver_lock = acquire_driver_lock_or_exit()
    backoff = 1
    handle_failures = 0
    noticed_expired = False
    print("long-poll daemon started (Ctrl+C to stop)", flush=True)
    while True:
        try:
            status, msgs, commit = poll_once()
        except Exception:
            # poll_once itself can raise (e.g. the subprocess times out
            # after 180s). That used to kill the whole daemon; now it is
            # just a failed round — log, back off, keep looping. The
            # cursor has not advanced, so nothing is lost.
            print("POLL_EXCEPTION: poll_once raised; sleeping and continuing",
                  flush=True)
            traceback.print_exc()
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if status == "expired":
            if not noticed_expired:
                print("TOKEN EXPIRED: run `ilink.py qrcode` + `login` again.", flush=True)
                noticed_expired = True
            time.sleep(60)
            continue
        if status == "error":
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        backoff = 1
        commits = [commit] if commit else []
        if msgs:
            # Handle first, commit after. A handler (or burst-collect)
            # exception must not kill the daemon and must not commit:
            # the un-advanced cursor makes the server re-deliver the
            # whole burst next round (at-least-once; duplicates are
            # possible — the documented demo-driver trade-off).
            try:
                burst, commits = collect_burst(msgs, commits)
                handle_burst(burst, echo=echo, llm=llm)
                handle_failures = 0
            except Exception:
                handle_failures += 1
                delay = min(2 ** handle_failures, 60)
                print("HANDLE_FAILED: burst handling raised; skipping "
                      "commit, batch will be re-fetched next round",
                      flush=True)
                traceback.print_exc()
                if handle_failures == 5:
                    # A permanently failing handler ("poison batch")
                    # used to spin every ~35s and flood the log
                    # forever; surface it once, then keep backing
                    # off. The batch is NEVER auto-committed: losing
                    # it would violate at-least-once delivery.
                    print("HANDLE_FAILING: 5 consecutive handle "
                          "failures; backing off", flush=True)
                time.sleep(delay)
                continue
        # Commits are cursor-ordered: stop at the first failure, or a
        # later commit would jump the cursor past an uncommitted batch.
        # A failure is already logged as COMMIT_FAILED by commit_batch;
        # the un-advanced cursor re-delivers the rest next round.
        for c in commits:
            if not commit_batch(c):
                break


if __name__ == "__main__":
    main()
