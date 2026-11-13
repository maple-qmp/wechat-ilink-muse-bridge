# Muse hook 变体：把“长轮询”塞进 Muse 的 hook 机制里

[中文](README.md) | [English](README.en.md)

`daemon.py` 是给普通环境用的常驻长轮询循环。但 Muse 运行时里已经有
现成的 agent 调度，不需要再养一个调 LLM 的进程——本目录是生产环境
实际在用的另一种接法：

## 结构

```
微信用户 ──消息──> iLink 服务器
                      │  getupdates（长轮询，服务端 hold ≤35s）
                      ▼
              ilink-watch.sh        ← Muse hook 脚本，每 5s 跑一轮，
                      │               每轮就是一次长轮询
             有新消息 │  先落盘，再 wake
                      ▼
        ilink-pending.json          ← 待处理队列（原子写入，断电不丢）
                      │  worker 用 ilink.py claim 认领 → ilink-processing.json
                      ▼
              Muse worker agent     ← 发送前复查队列、合并连发，只回一条；
                                      超过 10 分钟没处理完的批次会被
                                      脚本退回 pending 重新唤醒
```

## 为什么这么绕

- **不丢消息**：hook 脚本先把消息落盘再唤醒 agent。agent 崩溃/超时，
  消息还在队列里，下一轮脚本会再次唤醒。
- **不重复回复**：认领用 `ilink.py claim`（队列锁内完成；已有
  processing 时拒绝认领，绝不像裸 `mv` 那样覆盖别人的批次），多个重复
  唤醒里只有一个能拿到队列，其余安静退出。已有 worker 持有新鲜认领时，
  脚本不再为新消息另起 worker，新消息由它发送前的
  `ilink.py claim --merge <claim_id>` 合并进同一条回复；回复后用
  `ilink.py release <claim_id>` 释放（认领已被回收时拒绝删除别人的批次）。认领后立即 `touch` processing 文件、
  处理期间约每 60 秒再 touch 一次续租：脚本按文件 ctime 超 10 分钟
  回收，活着但慢的 worker 停 touch 也会被误判死亡、批次被重新唤醒
  导致双 worker 各回一遍。
- **发送幂等键**：回复某批次时，把该批次首条消息的 `message_id`
  作 `--key` 传给 send（例：`ilink.py send <to> <ctx> - --key <message_id>`）；
  同一 key 的重复调用直接回 `SEND_ALREADY_SENT` 而不重发——这拦的是
  "两次独立调用各带新 client_id"这类服务端去重拦不住的重复。拆成多
  气泡时每个气泡的 key 要加气泡序号后缀（如 `<message_id>-1`、
  `<message_id>-2`），否则后几个气泡会被账本当成重复拦住。
- **连发合并**：用户连发几条时 iLink 会拆成多个响应。worker 不做固定
  等待，而是**发送前复查**：回复组织好、真正发出之前再看一眼队列，
  有新消息就合并进来、按最新上下文重新组织回复，最后只回一条。
- **"正在输入"秒亮**：消息落盘那一刻，脚本核对发信人是主人后立即点亮
  typing 指示，不等 worker 冷启动；worker 侧的点亮只作续期。
- **回复后守候（连聊免冷启动）**：worker 回复完不立刻退出，而是运行
  `ilink-warm-wait.sh` 守候（默认空闲 180 秒、连聊总上限 1800 秒）：它
  维护热态标记 `ilink-warm.json`（心跳），守候期内新消息由它第一时间
  发现、worker 跳过上下文重读直接回复；脚本侧看到心跳新鲜的标记就不
  再重复 wake。标记心跳过期（worker 已死）脚本自动恢复正常 wake，
  自愈。空闲满 180 秒无人说话，worker 才安静退出。
- **token 过期只通知一次**：脚本发现 `TOKEN_EXPIRED` 时创建
  `rescan_notified` 标记并唤醒 agent 通知用户重扫；标记在就不再打扰。
- **告警闭环**（`alert-state.json` 状态机）：过期超过 1 小时每小时复报
  一次；轮询连续失败满 10 分钟才告警（防网络抖动误报）、之后每小时
  复报；曾告警的异常恢复后唤醒 agent 报一句平安（短暂抖动未告警的
  静默清除）；worker 处理超时被回收后，积压的 wake 会带 `stuck_alert`
  字段。恢复（重新扫码/网络恢复）时自动清除过期标记——旧版标记永不
  清除会导致下一次过期不再通知，已修复。
- **交付语义：至少一次（at-least-once）**。poll 是两段式的：`ilink.py
  poll` 只拉取、不推进游标不记去重 ID；脚本把批次原子落盘后才调
  `ilink.py commit` 提交。任何一步崩溃，消息都会在下一轮被重新拉到
  ——可能重复回复，但绝不静默丢失。配套的防线：超时回收会把卡住的
  批次合并到新批次前面（两边都不覆盖）；单轮媒体下载有总预算
  （`ILINK_POLL_MEDIA_BUDGET`，默认 60 秒），超出的附件带 `media_error`
  照常投递，不会拖过 poll 超时变成反复重拉的毒批次；poll 输出为空/非法 JSON 一律按
  轮询故障处理（进程硬崩溃也会进告警状态机，而不是被当成"没有新
  消息"）。

- **共享驱动锁**：本脚本每轮在单实例锁之外，还会非阻塞尝试状态目录下的 `ilink-driver.lock`；拿不到说明 `daemon.py` 在跑，本轮安静退出（不 touch 心跳、不 poll、不 wake）。反过来，`daemon.py` 启动时若本脚本的心跳新鲜也会拒绝启动。先跑起来的驱动赢，后到的让路——“hook 与 daemon 二选一”不再只是一句文档。
- **两把锁分工不同**：`ilink-watch.lock` 防 hook 自己重叠，`ilink-driver.lock` 防 hook 与 daemon 互抢；两个驱动必须指向同一个状态目录，锁才共享。

脚本里的路径已参数化（`ILINK_DATA_DIR`、`ILINK_HOOK_STATE_DIR`、`ILINK_CLIENT`；
状态目录旧变量名 `ILINK_HOOK_STATE` 仍兼容，默认值与 `ilink.py health` 一致），
`$HATCH_HOOK_RUNTIME` 是 Muse hook 运行时提供的环境，非 Muse 环境请用
仓库根目录的 `daemon.py`。
