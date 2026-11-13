#!/usr/bin/env bash
# iLink 微信分身：长轮询收消息（hook 版）
# 每一轮脚本运行 = 一次 getupdates 长轮询（服务端 hold 最多 35s，有消息立刻返回）。
# 有新消息 -> 写入 pending 队列并 wake，由 worker agent 生成回复；
# 无消息/出错 -> silent。
# poll 是两段式的：ilink.py poll 只拉取、不推进游标不记去重 ID；本脚本把
# 批次原子落盘 pending 之后才调 ilink.py commit 提交。任何一步崩溃，
# 消息都会在下一轮被重新拉到——宁可重复，绝不丢失。
# 告警闭环（alert-state.json 状态机）：
#   - token 过期：首次立即 wake 报一次，之后每小时复报一次（payload 带 repeat/hours）
#   - 轮询连续失败：满 10 分钟才报（防抖动误报），之后每小时复报一次
#   - 恢复：曾告警过的异常恢复正常后 wake 一次 recovered（报好不只报坏）；
#     短暂抖动未告警的静默清除状态
#   - worker 卡住：processing 超 10 分钟被回收时记 stuck 起点，积压超 10 分钟
#     随 pending 的 wake 带 stuck_alert（每小时至多一次）
# 可靠性补强：
#   - 单实例锁（flock）：重叠触发时后到的实例安静退出，防两个实例在
#     查队列→poll→落盘→commit→唤醒全程互相覆盖
#   - pending 损坏隔离：队列文件不可解析时改名 .corrupt-<epoch> 保留
#     现场并在日志打 PENDING_CORRUPT 标记，绝不当成空队列覆盖或静默卡死
#   - commit 待提交账本：commit 前先把 payload 原子落盘到
#     ilink-commit-pending.json，成功才删；失败时下一轮开头先补交，
#     补交成功前不 poll——commit 失败只是延迟，不与重拉交叠出重复处理
# 存活心跳（2026-10-25）：
#   - 每轮拿锁成功后 touch ilink-heartbeat；安静轮次不写 poll.log，
#     唯独心跳能证明 hook 活着，供 ilink.py watchdog 看门
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

# Paths are parameterized for this public example. In production these point
# at the bridge's data dir and the hook state dir.
BRIDGE=${ILINK_DATA_DIR:-$HOME/wechat-ilink-data}
# Canonical name/default match `ilink.py health` (ILINK_HOOK_STATE_DIR,
# ~/hooks/state) — the two MUST agree or health inspects an empty dir.
# ILINK_HOOK_STATE is the legacy name, still honored as an alias.
STATE_DIR=${ILINK_HOOK_STATE_DIR:-${ILINK_HOOK_STATE:-$HOME/hooks/state}}
mkdir -p "$STATE_DIR"
# 单实例锁：用 fd 持有 flock 到脚本结束（不要用 flock 命令包裹子命令，
# 那样会漏掉段落）。拿不到锁 = 已有实例在跑，安静退出即可，不是错误。
exec 9>"$STATE_DIR/ilink-watch.lock"
if ! flock -n 9; then
  exit 0
fi
# 共享驱动锁（daemon/hook 二选一落到锁上，不只靠文档）：
# daemon 常驻时会一直持有 $STATE_DIR/ilink-driver.lock；本脚本每轮
# 尝试非阻塞拿同一把锁，拿不到 = daemon 在跑，本轮安静退出，不
# touch 心跳、不 poll、不 wake。反过来，daemon 启动时会先看
# ilink-heartbeat 是否新鲜（本脚本每轮都 touch），新鲜就拒绝启动，
# 所以先跑起来的驱动赢，后到的安静让路。两把锁分工不同：
# ilink-watch.lock 防 hook 自己重叠，ilink-driver.lock 防 hook 与
# daemon 互抢。注意顺序：驱动锁检查必须在 touch 心跳之前，否则
# 一个被 daemon 挡住的 hook 轮次也会刷新心跳，误导 daemon 以为
# hook 还活着。
exec 8>"$STATE_DIR/ilink-driver.lock"
if ! flock -n 8; then
  exit 0
fi
# 存活心跳：每轮都 touch，看门狗凭它判断 hook 是不是还在跑。
# 拿不到锁的实例安静退出，不 touch。
touch "$STATE_DIR/ilink-heartbeat"
PENDING=$STATE_DIR/ilink-pending.json
PROC=$STATE_DIR/ilink-processing.json
WARM=$STATE_DIR/ilink-warm.json
COMMIT_PENDING=$STATE_DIR/ilink-commit-pending.json
RESCAN=$BRIDGE/rescan_notified
POLLLOG=$BRIDGE/poll.log
ALERT=$BRIDGE/alert-state.json
DRY=${HATCH_HOOK_DRY_RUN:-0}
NOW=$(date +%s)

now() { date '+%Y-%m-%d %H:%M:%S'; }

# 带时间戳的一行日志进 poll.log（脚本平时落痕的地方；dry-run 不写）
log_line() {
  [ "$DRY" = "1" ] && return 0
  echo "[$(now)] $1" >> "$POLLLOG"
  trim_poll_log
}

# poll.log 上限：health 只读最后一行，但脚本只追加从不截断，文件会
# 无限增长。每次写日志后只保留最近 500 行（安静轮次不写日志，所以
# 截断本身很廉价；单实例锁保证截断时没有别的实例在写）。
trim_poll_log() {
  [ "$DRY" = "1" ] && return 0
  [ -f "$POLLLOG" ] || return 0
  local n
  n=$(wc -l < "$POLLLOG" 2>/dev/null || echo 0)
  if [ "$n" -gt 500 ] 2>/dev/null; then
    tail -n 500 "$POLLLOG" > "$POLLLOG.trim.$$" && mv "$POLLLOG.trim.$$" "$POLLLOG"
  fi
}

# 队列文件三态计数：空/不存在 -> 0；合法队列（对象 + .msgs 数组）-> 条数；
# 损坏或结构不对 -> 立即隔离改名保留现场、日志打 PENDING_CORRUPT
# 标记、本轮按 0 处理。绝不把损坏文件当成空队列：当 0 条会被上层
# 覆盖（丢消息）或误判成"队列已空"而静默卡死。
queue_count() {
  local path=$1 n iso
  [ -s "$path" ] || { echo 0; return; }
  # Strict shape check, not just "parses as JSON": a file like
  # {"msgs": "invalid"} parses fine but is not a queue. Counting it
  # as 0 used to send the script down the silent "pending file empty"
  # branch forever (the pending branch never reaches poll), a quiet
  # total stall. Anything misshapen quarantines like corrupt input.
  if ! jq -e 'type == "object" and (.msgs | type == "array")' "$path" >/dev/null 2>&1; then
    # $$ in the name: two quarantines in the same second must not
    # clobber each other (epoch seconds alone collide).
    iso="$path.corrupt-$(date +%s)-$$"
    if mv "$path" "$iso" 2>/dev/null; then
      log_line "PENDING_CORRUPT: $path unparseable, quarantined as $iso"
      log "PENDING_CORRUPT: quarantined $path as $iso"
    fi
    echo 0
    return
  fi
  n=$(jq '[.msgs[]?] | length' "$path" 2>/dev/null || echo 0)
  echo "${n:-0}"
}

# commit 待提交账本补交：上一轮 commit 失败时 payload 留在
# ilink-commit-pending.json。本轮 poll 之前先补交：成功删账本、继续；
# 失败打 COMMIT_RETRY_FAILED 并返回非 0，本轮跳过 poll（旧批次未提交
# 前拉新批次会交叠）。账本本身损坏则隔离丢弃并明确打标记——payload
# 丢了游标就停在原地，下轮 poll 会把这批消息原样重拉，不丢消息。
flush_commit_pending() {
  [ -f "$COMMIT_PENDING" ] || return 0
  [ "$DRY" = "1" ] && return 0
  local payload iso
  if ! jq -e 'type == "object"' "$COMMIT_PENDING" >/dev/null 2>&1; then
    iso="$COMMIT_PENDING.corrupt-$(date +%s)"
    mv "$COMMIT_PENDING" "$iso" 2>/dev/null || rm -f "$COMMIT_PENDING"
    log_line "COMMIT_PENDING_CORRUPT: $COMMIT_PENDING unparseable, quarantined as $iso; its batch will be re-fetched"
    log "COMMIT_PENDING_CORRUPT: quarantined $COMMIT_PENDING as $iso"
    return 0
  fi
  payload=$(jq -c '.' "$COMMIT_PENDING")
  if printf '%s' "$payload" | python3 "${ILINK_CLIENT:-$BRIDGE/ilink.py}" commit >/dev/null 2>&1; then
    rm -f "$COMMIT_PENDING"
    log "retried pending commit OK"
    return 0
  fi
  log_line "COMMIT_RETRY_FAILED: $COMMIT_PENDING still uncommitted, poll skipped this round"
  log "commit retry failed, poll skipped this round"
  return 1
}

# 热态守候：worker 回复后不退出、由 ilink-warm-wait.sh 维护 ilink-warm.json
# 标记（心跳 + 总时长上限）。心跳新鲜（<=45s）且未过总上限时，说明已有一个
# 上下文就绪的 worker 在盯着队列——本脚本只管落盘/点亮输入，不再重复 wake；
# 标记过期即视为 worker 已死，自动恢复正常 wake（自愈）。
warm_alive() {
  [ -f "$WARM" ] || return 1
  local hb until_n
  hb=$(jq -r '.heartbeat // 0' "$WARM" 2>/dev/null || echo 0)
  until_n=$(jq -r '.until // 0' "$WARM" 2>/dev/null || echo 0)
  case "$hb" in ''|*[!0-9]*) return 1 ;; esac
  case "$until_n" in ''|*[!0-9]*) return 1 ;; esac
  [ $((NOW - hb)) -le 45 ] && [ "$until_n" -gt "$NOW" ]
}

# 队列锁：与 `ilink.py claim / claim --merge / release` 共用
# $STATE_DIR/ilink-queue.lock（同一把 flock），保证"认领 / 合并 / 超时回收 /
# 落盘新批次"互不交叠。fd 7 只在需要时短暂持有。
QUEUE_LOCK=$STATE_DIR/ilink-queue.lock
queue_lock() { exec 7>"$QUEUE_LOCK"; flock -w 30 7; }
queue_unlock() { flock -u 7 2>/dev/null || true; exec 7>&-; }

# processing 认领是否已超时（ctime 超 10 分钟 = worker 停止续租，视为已死）
proc_stale() {
  [ -f "$PROC" ] && [ -n "$(find "$PROC" -cmin +10 2>/dev/null || true)" ]
}
# 有一个仍在续租的 worker 正在处理：新批次留给它在发送前复查时合并
# （或回复后由 warm-wait 接手），不再另起 worker。以前这里照样 wake，
# 第二个 worker 的 mv 会直接覆盖第一个的 processing 文件——双回复，
# 且第一个批次从崩溃恢复里消失。
claim_fresh() {
  [ -f "$PROC" ] && ! proc_stale
}

# 告警状态读写（jq，原子写；dry-run 不写状态，保持纯净）
astate() { jq -r ".$1 // 0" "$ALERT" 2>/dev/null || echo 0; }
aset() {
  [ "$DRY" = "1" ] && return 0
  local tmp="$ALERT.tmp.$$"
  if [ -f "$ALERT" ]; then
    jq --argjson v "$2" ".$1 = \$v" "$ALERT" > "$tmp" 2>/dev/null || echo '{}' > "$tmp"
  else
    jq -n --argjson v "$2" "{\"$1\": \$v}" > "$tmp" 2>/dev/null || echo '{}' > "$tmp"
  fi
  mv "$tmp" "$ALERT" 2>/dev/null || true
}

# 更新提醒：每小时最多跑一次 version-check --notify（后台跑，不阻塞
# 消息处理；未订阅时它只打印 NO_SUBSCRIBERS，不发任何消息）
vc_last=$(astate vc_last)
case "$vc_last" in ''|*[!0-9]*) vc_last=0 ;; esac
if [ "$vc_last" -eq 0 ] || [ $((NOW - vc_last)) -ge 3600 ]; then
  aset vc_last "$NOW"
  if [ "$DRY" != "1" ]; then
    (python3 "${ILINK_CLIENT:-$BRIDGE/ilink.py}" version-check --notify >/dev/null 2>&1 &) || true
  else
    silent "dry-run: would run version-check --notify"
  fi
fi

# 崩溃恢复：上一个 worker 超过 10 分钟没处理完，把消息退回 pending
# 注意：必须用 -cmin（inode 变更时间）而非 -mmin（内容修改时间）：
# worker 用 mv 认领队列时 mtime 保留为消息落盘时间，若消息在 pending 里
# 积压超过 10 分钟，认领瞬间就会被误判过期、打回 pending 形成乒乓循环。
# mv 会刷新 ctime，ctime 才是真正的“被认领时间”。
# pending 里还有新批次时，把卡住的批次合并到它前面（旧消息在前），
# 绝不覆盖新批次。以前遇到这种情况只是推迟回收，而 worker 认领新批次
# 的 mv 会覆盖卡住的 processing 文件，卡住的批次就永久丢了。
# 全程持有队列锁，与 ilink.py claim/merge/release 互斥。
pending_msgs=$(queue_count "$PENDING")
if proc_stale && queue_lock; then
  if proc_stale; then
    if [ "${pending_msgs:-0}" -gt 0 ] 2>/dev/null; then
      mtmp="$PENDING.tmp.$$"
      if jq -c -s '{msgs: ((.[0].msgs // []) + (.[1].msgs // []))}' "$PROC" "$PENDING" > "$mtmp" 2>/dev/null; then
        mv "$mtmp" "$PENDING"
        rm -f "$PROC"
        log "recovered stale processing file, merged ahead of pending"
      else
        rm -f "$mtmp"
        log_line "RECOVER_MERGE_FAILED: $PROC could not be merged into $PENDING; left in place"
      fi
    else
      # claim_id 只属于原认领者，退回 pending 时去掉
      mtmp="$PENDING.tmp.$$"
      if jq -c '{msgs: (.msgs // [])}' "$PROC" > "$mtmp" 2>/dev/null; then
        mv "$mtmp" "$PENDING"
        rm -f "$PROC"
      else
        rm -f "$mtmp"
        mv "$PROC" "$PENDING"
      fi
      log "recovered stale processing file to pending"
    fi
    # 卡住计时：能被回收说明这一轮处理至少已卡了 10 分钟，起点回拨 600 秒
    if [ "$(astate stuck_since)" -eq 0 ]; then
      aset stuck_since $((NOW - 600))
    fi
  fi
  queue_unlock
fi

# 有未处理的消息（上一轮拉到、agent 还没吃完）：直接 wake，不再拉新的
if [ -s "$PENDING" ]; then
  cnt=$(queue_count "$PENDING")
  if [ "$cnt" -gt 0 ] 2>/dev/null; then
    log "pending $cnt messages waiting"
    # Leftover batches also light the typing indicator immediately (owner only)
    owner=$(jq -r '.owner_id // empty' "$BRIDGE/owner.json" 2>/dev/null || true)
    sender=$(jq -r '.msgs[0].from_user_id // empty' "$PENDING" 2>/dev/null || true)
    if [ -n "$owner" ] && [ "$sender" = "$owner" ]; then
      python3 "${ILINK_CLIENT:-$BRIDGE/ilink.py}" typing "$sender" start >/dev/null 2>&1 || true
    fi
    # 卡住告警：积压起点距今 >=10 分钟且距上次告警 >=1 小时，随 wake 带出
    extra=""
    stuck_since=$(astate stuck_since)
    stuck_last=$(astate stuck_last)
    if [ "$stuck_since" -gt 0 ] && [ $((NOW - stuck_since)) -ge 600 ] \
       && [ $((NOW - stuck_last)) -ge 3600 ]; then
      aset stuck_last "$NOW"
      extra=",\"stuck_alert\":true,\"stuck_minutes\":$(( (NOW - stuck_since) / 60 ))"
    fi
    if warm_alive; then
      log "warm worker on duty, $cnt pending left to it"
      silent "warm worker on duty"
    elif claim_fresh; then
      log "active claim in processing, $cnt pending left to its pre-send merge"
      silent "worker on duty (fresh claim)"
    else
      wake "pending_wechat_messages" "{\"count\":$cnt,\"pending_file\":\"$PENDING\"$extra}"
    fi
  else
    silent "pending file empty"
  fi
  # 分支已结束（wake/silent 为最后调用）
elif ! flush_commit_pending; then
  # 上一轮的 commit 还没补交成功：本轮不 poll，避免新批次与未提交的
  # 旧批次交叠（补交成功后旧批次的游标才推进，届时正常拉新消息）
  silent "commit retry pending, poll skipped"
else
  # 长轮询一轮（服务端 hold 住最多 35s）。timeout 120 封顶：poll 本身
  # 是纯拉取（不推进游标、不记去重），超时只是本轮无结果、下轮重拉，
  # 但能防止病态媒体批次把队列拖住数十分钟（单实例锁会让后续轮次
  # 安静退出，队列实际停滞）。
  out=$(timeout 120 python3 "${ILINK_CLIENT:-$BRIDGE/ilink.py}" poll 2>/dev/null || true)

  if [[ "$out" == TOKEN_STALE* ]]; then
    # -14 退避窗口内：token 可能自愈，不告警、不记 poll 故障，下轮再看
    silent "token stale, backoff in progress"
  elif [[ "$out" == NO_TOKEN* ]] || [[ "$out" == TOKEN_EXPIRED* ]]; then
    exp_since=$(astate exp_since)
    exp_last=$(astate exp_last)
    if [ "$exp_since" -eq 0 ]; then
      # 首次过期：立即报一次（rescan_notified 标记保留作兼容，成功轮询时清除）
      aset exp_since "$NOW"
      aset exp_last "$NOW"
      if [ "$DRY" != "1" ]; then
        touch "$RESCAN"
        wake "token_expired" "{}"
      else
        silent "dry-run: would set expiry state and wake token_expired"
      fi
    elif [ $((NOW - exp_last)) -ge 3600 ]; then
      # 长时间未恢复：每小时复报一次
      aset exp_last "$NOW"
      if [ "$DRY" != "1" ]; then
        wake "token_expired" "{\"repeat\":true,\"hours\":$(( (NOW - exp_since) / 3600 ))}"
      else
        silent "dry-run: would repeat token_expired alert"
      fi
    else
      silent "token expired, repeat alert throttled"
    fi
  elif [[ "$out" == POLL_ERROR* ]] || ! printf '%s' "$out" | jq -e 'type == "object" and has("msgs")' >/dev/null 2>&1; then
    # POLL_ERROR，或 poll 进程硬崩溃（空输出/非法 JSON/缺 msgs 字段）。
    # 崩溃若不进本分支，会被误判成"没有新消息"、故障永远不告警。
    if [ "$DRY" != "1" ]; then
      echo "[$(now)] $(printf '%s' "${out:-POLL_CRASH:empty-or-invalid-output}" | head -c 200)" >> "$POLLLOG"
      trim_poll_log
    fi
    err_since=$(astate err_since)
    if [ "$err_since" -eq 0 ]; then
      aset err_since "$NOW"
      err_since=$NOW
    fi
    err_last=$(astate err_last)
    err_dur=$((NOW - err_since))
    if [ "$err_dur" -ge 600 ] && { [ "$err_last" -eq 0 ] || [ $((NOW - err_last)) -ge 3600 ]; }; then
      # 连续失败满 10 分钟才报（抖动通常几分钟内自愈），之后每小时复报
      aset err_last "$NOW"
      if [ "$DRY" != "1" ]; then
        wake "poll_failing" "{\"minutes\":$((err_dur / 60))}"
      else
        silent "dry-run: would wake poll_failing"
      fi
    else
      silent "poll error"
    fi
  else
    # 轮询成功：先做恢复记账（曾告警过的异常恢复了要报平安）
    recovered=""
    exp_since=$(astate exp_since)
    err_since=$(astate err_since)
    # 卡住事件收尾：队列已排空（无 pending 才进本分支）且无人处理中，清状态
    if [ ! -f "$PROC" ] && [ "$(astate stuck_since)" -gt 0 ]; then
      aset stuck_since 0
      aset stuck_last 0
    fi
    if [ "$exp_since" -gt 0 ]; then
      recovered="{\"kind\":\"token\",\"minutes\":$(( (NOW - exp_since) / 60 ))}"
      aset exp_since 0
      aset exp_last 0
      if [ "$DRY" != "1" ]; then rm -f "$RESCAN"; fi
    elif [ "$err_since" -gt 0 ]; then
      if [ "$(astate err_last)" -gt 0 ]; then
        recovered="{\"kind\":\"poll_error\",\"minutes\":$(( (NOW - err_since) / 60 ))}"
      fi
      aset err_since 0
      aset err_last 0
    fi
    cnt=$(printf '%s' "$out" | jq '[.msgs[]?] | length' 2>/dev/null || echo 0)
    commit_json=$(printf '%s' "$out" | jq -c '.commit // empty' 2>/dev/null || true)
    # 提交本轮 poll（推进游标 + 记 seen）：只能在批次可靠落盘之后调用。
    # payload 先原子落盘成待提交账本再调 commit、成功才删账本：进程在
    # 前后任一步崩溃，账本都还在，下轮开头先补交（见 flush_commit_pending）。
    # 失败不中断本轮（批次已在队列里、worker 照常处理），COMMIT_FAILED
    # 记入 poll.log 并注明账本位置——失败只是延迟，不与重拉交叠。
    do_commit() {
      if [ "$DRY" != "1" ] && [ -n "$commit_json" ]; then
        local ctmp="$COMMIT_PENDING.tmp.$$"
        printf '%s' "$commit_json" > "$ctmp" && mv "$ctmp" "$COMMIT_PENDING"
        if printf '%s' "$commit_json" | python3 "${ILINK_CLIENT:-$BRIDGE/ilink.py}" commit >/dev/null 2>&1; then
          rm -f "$COMMIT_PENDING"
        else
          echo "[$(now)] COMMIT_FAILED: payload kept in $COMMIT_PENDING, retried next round" >> "$POLLLOG"
          trim_poll_log
        fi
      fi
    }
    if [ "${cnt:-0}" -gt 0 ] 2>/dev/null; then
      if [ "$DRY" = "1" ]; then
        # dry 不落盘、不提交：消息留给正式轮询去拉，避免重复入队
        silent "dry-run: $cnt messages fetched, left uncommitted for live poll"
      else
        # 原子写入 pending（只写 msgs，保持队列文件既有格式）
        tmp="$PENDING.tmp.$$"
        printf '%s' "$out" | jq -c '{msgs}' > "$tmp"
        # 落盘在队列锁内进行；万一 pending 已存在（理论上本分支只在
        # pending 为空时进入），合并追加而不是覆盖。
        if queue_lock; then
          if [ -s "$PENDING" ] && jq -e '.msgs | type == "array"' "$PENDING" >/dev/null 2>&1; then
            jq -c -s '{msgs: (.[0].msgs + .[1].msgs)}' "$PENDING" "$tmp" > "$tmp.m" && mv "$tmp.m" "$tmp"
          fi
          mv "$tmp" "$PENDING"
          queue_unlock
        else
          mv "$tmp" "$PENDING"
        fi
        log "received $cnt messages"
        # Light the "typing..." indicator the moment the batch lands (owner
        # only) instead of waiting for the worker agent to boot — the worker's
        # own typing start then just acts as a keep-alive refresh.
        owner=$(jq -r '.owner_id // empty' "$BRIDGE/owner.json" 2>/dev/null || true)
        sender=$(printf '%s' "$out" | jq -r '.msgs[0].from_user_id // empty' 2>/dev/null || true)
        if [ -n "$owner" ] && [ "$sender" = "$owner" ]; then
          python3 "${ILINK_CLIENT:-$BRIDGE/ilink.py}" typing "$sender" start >/dev/null 2>&1 || true
        fi
        do_commit
        rec_extra=""
        if [ -n "$recovered" ]; then rec_extra=",\"recovered\":$recovered"; fi
        if [ -z "$recovered" ] && warm_alive; then
          # 守候中的 worker 会在数秒内自己认领；带 recovered 时照常 wake，
          # 免得恢复通知被吞掉（双 worker 靠 mv 原子认领仲裁，不重复处理）
          log "warm worker on duty, batch left to it"
          silent "warm worker on duty"
        elif [ -z "$recovered" ] && claim_fresh; then
          # 已有 worker 在处理：它发送前的复查（claim --merge）会把这批
          # 合并进同一条回复；它回复后若没合并到，warm-wait 会接手，
          # 或它 release 后下一轮再 wake。
          log "active claim in processing, batch left to its pre-send merge"
          silent "worker on duty (fresh claim)"
        else
          wake "new_wechat_messages" "{\"count\":$cnt,\"pending_file\":\"$PENDING\"$rec_extra}"
        fi
      fi
    elif [ -n "$recovered" ]; then
      do_commit
      if [ "$DRY" != "1" ]; then
        wake "recovered" "$recovered"
      else
        silent "dry-run: would wake recovered"
      fi
    else
      do_commit
      silent "no new messages"
    fi
  fi
fi
