# 巡游直播的多语字幕与回看修订

节目锚点按实际解说位置写入。字幕版次按语言和节目区间区分。播放端分别反馈领到内容、入队与画面实际显示。

`fixtures/registers.json` 提供演练台账，`fixtures/events.ndjson` 提供各端送达的独立消息，`fixtures/receipts.json` 提供合作方的模拟回执。事件游标为送达顺序。业务发生时点出现在消息内容中时，以其来源所注明的时点为准。整套资料均为合成数据。

## 本地联调

使用 Python 3.11.2 标准库。联调入口只监听本地地址，不需要账号或外部连接。

```sh
python3 -m unittest -v
python3 fixture_gateway.py --port 8765
```

`GET /v1/registers` 返回台账。用 `GET /v1/events?after=0&limit=50` 读取一页消息，后续页将上一页的 `next_cursor` 作为 `after`。每条消息有 `cursor`、`source`、`minute` 及原业务记录 `payload`。

对端发送协议为 `POST /v1/receipts`，正文提供 `reference`、`operation`、`recipient`，三个字段均为字符串。需要配对的操作与接收方见回执示例文件。返回对象含 `reference` 和对端 `receipt`。若条目设置 `lost_reply`，首次调用被记录后连接断开。可用 `GET /v1/receipts/<reference>` 查看已记录的回执。相同编号和内容可重试，编号被另一个操作使用时返回 409。

联调进程运行期间保存回执记录。需要试读其他台账、消息与回执时，可通过 `--fixtures` 指向另一组同名资料文件。合作方只返回模拟事实，业务办理功能由各方应用负责。

---

# 巡游字幕后端（导播 / 审校 / 播放点）

`backend.py` 是只依赖标准库的业务后端：领域核心 `ControlRoom` 与 HTTP 服务在同一文件，可直接 import 做进程内测试，也可独立起服务。`app.py`、`server.py`、`main.py`、`control_room.py` 只是指向它的兼容别名。

## 运行与自检

```sh
python3 -m unittest -v          # 原有联调桩测试 + 39 项后端行为测试
python3 demo.py                 # 起真实 FixtureGateway，跑完整场景并自检
python3 backend.py --port 8787  # 默认加载 fixtures 台账并回放 events.ndjson
```

另开终端启动合作方桩并让后端改从 HTTP 同步事件、办理回执：

```sh
python3 fixture_gateway.py --port 8765
python3 backend.py --port 8787 --gateway http://127.0.0.1:8765 --sync
curl -s -X POST localhost:8787/v1/publish -H 'Content-Type: application/json' \
  -d '{"channel":"museum-delay"}'
```

端口也可用 `PORT` 环境变量指定；默认只监听 `127.0.0.1`。

## 三类查询

| 角色 | 接口 | 回答的问题 |
| --- | --- | --- |
| 导播 | `GET /v1/board` | 现在实际进行到哪个 cue、各语言屏/渠道现在能放哪个版次或走何种保障 |
| 审校 | `GET /v1/replay?flat=1`（或 `/v1/editions`） | 每个 cue/lang 哪个 approved rev 用于回看，哪些只是跳段备选 |
| 选版 | `GET /v1/replay/select?cue=&lang=&mode=corrected\|aired` | 回看取最新校订版（默认）或按播出事实取 as-aired 版 |
| 播放点 | `GET /v1/channels/<screen>/position` | 本渠道上次确认位置、已领 rev、实际显示位置、可接续/可替换内容 |
| 换班 | `GET /v1/handover` | 各渠道「已领 / 实际显示 / 仍可修改 / 回执不确定」区间与接续项 |

常用写入接口：

```
POST /v1/director/anchor     {"cue":"courtyard-talk","anchor_ms":420000,"minute":7}
POST /v1/director/skip        {"cue":"interlude","minute":5}
POST /v1/captions             字幕版次（cue/lang/rev/state/text/segment/applies_to/kind…）
POST /v1/reviews/caption      一次确认译文、适用段落 applies_to 与术语 terms
POST /v1/reviews/term         确认术语
POST /v1/playback             {"screen","cue","rev","state":"fetched|queued|shown",…}
POST /v1/events               单条 fixture 风格事件或 {"events":[…]} 批量接入（cursor 幂等）
POST /v1/sync                 {"gateway_url":"http://127.0.0.1:8765"} 分页拉取事件
POST /v1/publish              {"channel":"museum-delay"[,"cue":…,"rev":…]} 办回执
POST /v1/receipts             直接登记回执事实
```

## 合成台账对应的确定结论

回放 `fixtures/` 后（`demo.py` 会连真实回执桩验证）：

1. `interlude` 在 minute 5 被导播跳过；minute 6 到达的 `interlude/local rev2 queued` 永远只属于 `interlude`，状态为 **standby（备选）**。即使它后来被批准，也不会因为分钟接近而补进实际在 minute 7 锚定的 `courtyard-talk`。匹配只认显式 `cue`（和 `segment`），代码中不存在按时间相近回填的路径。
2. `courtyard-talk/local rev2「西院开唱」` 在 minute 8 approved，是 local 的当前 head；zh head 仍是独立核定的 rev1「演出将在西院开始」。校订按 `(cue, lang, rev, segment)` 隔离；共享 `line_id` 的另一语言、或引用其他段落的节目版本都不会被连带替换。
3. `museum-delay` minute 9 只 **fetched rev1、未 shown** → 接续项要求安全替换为 rev2（`replace:true, modifiable:true`）。向回执桩 `publish-cue/museum-delay` 用同一 reference 重试（首次 `lost_reply` 断连）后，回执 `museum-48` 确认 **rev2 fetched、shown:false** → 可排队播出但仍可修改。
4. `plaza-live` minute 11 **shown zh rev1**，生成不可变播出事实；回执 `plaza-85 shown:true` 幂等补全 sequence，不产生第二条事实。已显示内容不可被校订/迟报推回屏幕，该渠道接续队列到段尾；回看仍采用最新 approved 校订版（若存在），本台账 zh 无新 rev，故 `replay_as_aired`。
5. `village-live` 没有任何反馈，回执桩也没有对应样例（`422 no_counterparty_example` → 发布状态 `no_counterparty`），其 `feedback_state=no_feedback`，**不算播出**；`/v1/handover` 中 `global_block=false`，zh（plaza）与 delay local（museum）照常确认，不被它卡住。

## 保障约定（译文不齐时）

渠道对象可在 registers 中用 `guarantee.on_missing`（或顶层 `on_missing`/`fallback`）配置有序策略，取值 `brief`（简要预告，需要 `kind:"brief"` 的 approved 版）、`delay`（延播，要求 `can_hold:true`，可带 `minimum_delay`）、`notice`（明确显示「字幕暂缺」，文案可用 `guarantee.unavailable_text` 覆盖）。缺省策略：

- `can_hold:false` 的现场屏：`["brief","notice"]`；
- `can_hold:true` 的延播馆：`["delay","notice"]`。

`GET /v1/channels/<id>/position` 的 `continuation.items[].resolution` 与 `/v1/board` 都按此约定给出 `caption | brief | delay | notice | standby` 决策。

## 迟报与并发口径

- 事件游标即送达顺序；业务 `minute`/`anchor_ms` 只作为业务时间保存。带 `cursor` 的事件重复接入是幂等的。
- 播放位置按脚本顺序单调推进：迟到的旧 cue `shown` 报告会如实补一条播出事实，但不会降低 `last_shown_cue`，也不会重新进入接续队列。
- 所有状态在一把 `RLock` 后变更，跳段与校订并发到达时按接入顺序收敛；HTTP 服务为 `ThreadingHTTPServer`。
- 回执 reference 由渠道/cue/rev 确定性生成（`pub-<screen>-<cue>-r<rev>`，超长自动摘要），满足桩的 `[A-Za-z0-9_-]{1,96}` 规则；断连后用同一 reference 重试，重试耗尽再 `GET /v1/receipts/<ref>` 恢复。异操作复用 reference 记为 `conflict/409`。
