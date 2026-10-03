# 公共交通票价结算 API

基于 FastAPI + SQLite 的公交/地铁票价结算与换乘网络校验服务。客户端提交刷卡事件，服务按事件时间配对行程，依据可配置的分区票价、换乘优惠以及 UTC 自然日 / 自然周（ISO 周，周一起算）封顶规则计算应收金额；迟到或更正的记录会触发受影响乘客全量重算并返回每段行程的计费依据与调账差额。此外提供 `POST /reconciliations` 扣款对账接口，按出站时间快照内部行程费用，与外部扣款明细进行批次化一对一核对。`POST /network/validate` 还可对带生效时间的换乘网络（线路段 + 步行换乘边）按时间边界切片构图，校验枢纽可达性、双向换乘对称性、重复 / 自环边与重叠生效区间；通过的版本可用 `POST /network/snapshots` 固化并随时查询。票价争议通过 `POST /disputes` 提起（创建时冻结行程收费快照、申请额不得超过快照应收）、`POST /disputes/{id}/resolve` 凭乐观版本一次性结案并写入不可变退款流水；结案后若行程因事件更正而重算，`GET /disputes/{id}` 会实时报告待补退 / 待追回差额而不覆盖已结退款。此外提供运营方票款清分接口 `POST /clearings`：首次关账按出站时间冻结 UTC 半开结算区间与按起终点站配置的运营方万分比（10000=100%）规则，区间内行程缺匹配规则或份额合计不为 10000 时整批拒绝；每笔实收按规则拆分，除不尽的分依次按小数余数降序、运营方 ID 升序分配，并写入带触发器保护的不可变流水。刷卡更正导致行程金额、站点或配对变化后，已关账流水绝不改写；后续批次用新的 `clearing_id` 经 `POST /clearings/adjustments` 引用首次关账，沿用冻结区间与规则，按当前应分金额减去历次累计只追加正负差额（行程消失全额冲回、站点对变化时旧运营方冲回新运营方补入），重复调整不会重复入账。同 `clearing_id` 同载荷幂等、异载荷返回 `409`，可用 `GET /clearings/{id}` 查询冻结结果。此外提供 `POST /event-quality/scans` 刷卡事件质量扫描：在 UTC 半开区间内按 `(occurred_at, seq)` 扫描事件，识别窗口内重复同类刷卡与出站后未达到站点最短行程秒数便再次进站的时序冲突，结果只写入独立的只追加表，可随时用 `GET /event-quality/scans/{id}` 查询，绝不改动事件、行程、账单或审计。最后提供 `POST /privacy/exports` 匿名出行轨迹导出：在 UTC 半开区间（按进站时间）内读取行程，用服务器配置的 `PRIVACY_SALT` 对 `passenger_id` 做稳定 HMAC 假名化（同一乘客跨多段行程、跨多次导出保持同一假名），按（纪元对齐的 UTC 时间桶、起点站组、终点站组）统计去重乘客数，等价类人数不足最小群体 `k` 时**整组抑制**（不逐行泄露），只返回保留行程的假名 / 时间桶 / 站点组；结果只追加保存，相同 `export_id` 同载荷幂等、不同载荷 `409`，可用 `GET /privacy/exports/{id}` 查询。突发中断时可调用 `POST /disruption/replans` 疏运重排：提交最小换乘秒数、按站序排列（含 UTC 到发时刻与核定容量）的替代班次和必须整体成行的乘客组（origin / destination / ready_at / 人数），服务按 `(ready_at, group_id)` 依次在各相邻区段剩余容量约束下为每组选出**最早到达**行程（同到达再取换乘更少、班次 ID 序列字典序更小者），分配后逐段扣减容量，无法成行则给出滞留原因；班次时刻校验失败不落库，结果冻结保存，相同 `replan_id` 同载荷幂等、不同载荷 `409`，可用 `GET /disruption/replans/{id}` 查询。最后提供刷卡事件**复核暂存与释放**：`POST /events/review-holds` 提交 `hold_id`、乘客、事件 ID 列表与复核原因，服务在一个事务内确认事件归属并冻结每条事件的完整载荷快照、将其标记为 `held`（暂不参与配对、计费与待处理查询）；任一事件已被对账批次、清分批次或已结案争议引用则整批 `409` 且不改变状态。同 `hold_id` 同载荷幂等、异载荷 `409`；暂存期间用相同 `event_id` 提交更正一律拒绝。`POST /events/review-holds/{hold_id}/release` 凭期望版本释放，按原 `seq` 与事件时间重新结算并返回受影响行程的增减额；版本冲突或事件快照不一致时拒绝释放并保持 `held`。暂存记录与事件快照不可修改，释放结果另行只追加，`GET /events/review-holds/{hold_id}` 查询冻结状态。最后提供**更正影响预览** `POST /events/correction-preview`：运营人员在真正修改一条已收到的刷卡记录前，提交已有 `event_id` 及替换后的乘客、站点、进出站类型和 UTC 时间，服务在**不写入**事件、行程、账单、审计链或任何冻结表的前提下，按正式结算规则在当前快照上模拟相关乘客（乘客变更时同时模拟原、新乘客），返回与正式更正完全一致的新旧行程调账，并逐项指出因行程消失、站点或金额变化而受影响的对账批次、清分批次和已结案争议及原因；事件处于复核暂存、`event_id` 不存在、站点未知或时间非法时直接拒绝，完全相同载荷返回无变化。

## 启动

单条命令（依赖安装与数据库初始化随容器启动自动完成）：

```bash
docker compose up --build
```

访问地址：

- API 根地址： http://localhost:8000
- 交互式文档（Swagger UI）： http://localhost:8000/docs
- 健康检查： http://localhost:8000/health

SQLite 数据文件持久化在 `transit-data` 卷中（容器内路径 `/data/transit.db`）。匿名轨迹导出所需的 `PRIVACY_SALT` 由 Compose 注入（内置开发默认值，生产请覆盖，见下文[配置](#配置)）；启动方式与命令均无其他变化。

## 业务规则

- **事件配对**：同一乘客的刷卡事件按 `(occurred_at, 到达序号)` 排序扫描；`in` 开启行程，下一条 `out` 关闭行程。无法配对的记录进入待处理列表，原因包括：
  - `tap_out_without_tap_in`：出站时无开启的进站记录
  - `unclosed_tap_in`：连续两次进站，前一次进站被作废
  - `missing_tap_out`：进站后始终没有对应出站
- **分区票价**：车站归属分区，票价按起讫分区对查表（对称），见 `config/fares.json` 的 `zone_fares`。
- **换乘优惠**：上一段行程出站后 `transfer_window_minutes` 分钟内再次进站，本段减免 `transfer_discount_cents`（不超过本段基础票价）。
- **封顶**：按行程时间顺序累计，先受 UTC 自然日封顶（`daily_cap_cents`）限制，再受 ISO 自然周封顶（`weekly_cap_cents`）限制；达到封顶后后续行程应收为 0。
- **幂等与更正**：
  - `event_id` 相同且内容完全一致的重复提交 → 幂等返回 `duplicate`，不触发重算；
  - `event_id` 相同但内容不同 → 视为更正（`corrected`），更新记录并触发重算；
  - 任何新记录（包括迟到记录）都会触发该乘客全量确定性重算，响应中的 `adjustments` 列出每段行程的 `old/new/delta` 金额与完整计费依据 `basis`；配对关系变化导致行程消失时会返回金额为负的冲正调账。
- **更正影响预览**：`POST /events/correction-preview` 是 `POST /events` 更正路径的只读模拟——按当前快照用正式结算规则在内存中试算相关乘客（被更正事件保留原始到达 `seq`，与正式更正完全一致），不写入任何业务表或冻结表、不追加审计记录；完全相同载荷（含同一时刻的其它时区写法）返回 `unchanged`，暂存中事件 `409`、未知事件 `404`、未知站点或非法时间 `422`。
- **金额单位**：所有金额均为整数分（cents），货币见配置 `currency`。
- **刷卡事件复核暂存与释放**：
  - **暂存**（`POST /events/review-holds`）：同一事务内确认每个 `event_id` 存在且属于该乘客，并为每条事件冻结**完整载荷快照**（乘客、站点、刷卡类型、UTC 事件时间、原始到达 `seq` 与逐事件载荷哈希），随后把事件标记为 `held` 并立即对该乘客重算——暂存事件**不参与**配对、计费，也不出现在 `GET /pending` 中（其余事件照常结算）；响应给出因抽出暂存事件而产生的各行程正负调账。
  - **整批拒绝**：任一事件已被**对账批次**（`recon_results`）、**清分批次**（`clearing_entries`）或**已结案争议**（`refunds`，按行程 `trip_id` 的进站/出站事件匹配）引用，或已处于另一暂存中，整批返回 `409`，**不写入任何暂存或快照、不改变状态**。仅 open 的争议不算 booked，引用其行程不阻止暂存。事件不存在或属于其他乘客返回 `422`，事件 ID 列表为空或含重复也返回 `422`。
  - **幂等与冲突**：相同 `hold_id` + 相同载荷（`event_ids` 数组顺序不影响判定）重复提交返回已存冻结结果（`status="duplicate"`）；相同 `hold_id` + 不同载荷返回 `409`。暂存期间对暂存事件以相同 `event_id` 提交**不同载荷的更正一律 `409` 拒绝**（载荷完全相同的重复提交仍是幂等 `duplicate`）。
  - **乐观版本**：暂存为 `state="held"`、`version=1`；`POST /events/review-holds/{hold_id}/release` 必须提交 `expected_version=1`，版本不符返回 `409` 且事件保持 held。
  - **快照一致性**：释放前逐条比对冻结的逐事件载荷哈希；任何事件当前载荷与快照不一致（或已不在该暂存下）都拒绝释放（`409`）并保持 held——复核始终以冻结快照为准。
  - **释放重算**：校验通过后清除 held 标记，事件按**原始 `seq` 与事件时间**重新进入配对与计费，响应在 `release.adjustments` 中给出受影响行程的增减额；释放结果（含调账）**另行追加**到只追加表，`state="released"`、`version=2`。
  - **不可修改**：`event_holds`、`event_hold_snapshots` 与 `event_hold_releases` 均带禁止 `UPDATE`/`DELETE` 的触发器；一个暂存至多释放一次，相同释放请求幂等返回 `duplicate`，其它释放载荷 `409`。
- **运营方票款清分**：
  - **首次关账**（`POST /clearings`）：冻结按**出站时间**（`ended_at`）界定的 UTC 半开区间 `[window_start, window_end)` 与按有向起讫站对配置的运营方万分比规则；同一站对不得重复配置、同规则内运营方不得重复、每条规则份额合计必须恰为 **10000**，且区间内**每笔**行程都必须能匹配到规则，否则**整批拒绝**（不写任何批次或流水）。
  - **尾分分配**：每个运营方先取 `floor(金额 × 万分比 / 10000)`，剩余整数分按**小数余数降序**逐分发放，余数相同按**运营方 ID 升序**兜底；拆分结果合计严格等于行程金额（封顶导致 0 元行程各运营方均为 0）。
  - **不可变流水**：首次关账逐行程、逐运营方写入一条流水（含当时站点、出站时间、万分比、毛额、分摊额、累计额），批次表与流水表均带禁止 `UPDATE`/`DELETE` 的触发器。
  - **更正追补**（`POST /clearings/adjustments`）：用新的 `clearing_id` 引用首次关账（`base_clearing_id`，必须是 `seq=0` 的首账），沿用其**冻结区间与规则**；对区间内当前行程按当前应收重新拆分，减去该行程/运营方在本链历次批次的**累计已分**，只追加非零的**正负差额**；行程消失（更正后重新配对或移出区间）按末次记载全额冲回到 0；站点对变为另一个已覆盖规则对时，旧规则独有运营方冲回、新规则独有运营方补入（毛额不变时整批净额为 0）；当前行程出现冻结规则未覆盖的新站对时整批 `422`（规则不可变，需要新规则应另开首次关账）。
  - **不重复入账**：每个批次都从"当前目标 − 链上累计"重新计算，因此同载荷重放幂等、以新批次号在状态未变时再次追补只会写入空批次（零差额），已冲回的行程再次消失不会二次冲回。
  - **幂等与原子性**：同 `clearing_id` 同载荷返回已存结果（`status="duplicate"`，规则/份额数组顺序不影响载荷判定），异载荷 `409`；批次行与其全部流水行、审计记录同事务原子提交。
- **票价争议与退款**：`POST /disputes` 创建时冻结行程当时的收费快照，申请退款额不得超过快照应收；`POST /disputes/{id}/resolve` 凭乐观版本（open 为 1、结案后为 2）一次性结案，同事务写入一条不可变退款流水（存储层触发器禁止更新/删除）。结案后行程若因事件更正而重算，`GET /disputes/{id}` 按"快照应收 − 已退金额 = 乘客净付"与当前应收对比，报告待补退（`top_up_due`）或待追回（`clawback_due`）差额；差额仅报告，已结退款绝不被覆盖。相同请求幂等，版本冲突返回 `409`。

## 接口

### `POST /events` — 提交刷卡事件

```json
{
  "event_id": "evt-001",
  "passenger_id": "p-1001",
  "station_id": "S01",
  "tap_type": "in",
  "occurred_at": "2026-09-21T08:30:00Z"
}
```

响应：

```json
{
  "event_id": "evt-001",
  "status": "processed | duplicate | corrected",
  "event_pairing": "paired | pending",
  "pending_reason": null,
  "adjustments": [
    {
      "trip_id": "evt-001:evt-002",
      "day_key": "2026-09-21",
      "week_key": "2026-W39",
      "old_amount_cents": 0,
      "new_amount_cents": 500,
      "delta_cents": 500,
      "basis": { "zone_fare_rule": "zone 1 -> zone 2", "...": "..." }
    }
  ]
}
```

未知车站返回 `422`。

### `GET /passengers/{passenger_id}/bill` — 乘客账单

可选查询参数：`day=YYYY-MM-DD`、`week=YYYY-Www`。返回行程明细（含每段计费依据）、按日 / 按周合计与封顶额度、总金额。

### `GET /pending` — 待处理记录

可选查询参数：`passenger_id`。返回所有无法配对的事件及原因。处于复核暂存（`held`）中的事件不会出现在结果中。

### `POST /events/correction-preview` — 更正影响预览（只读模拟）

运营人员在真正修改一条已收到的刷卡记录前，先用该接口预览这次更正会不会让已经生成的业务凭证失效。请求体与 `POST /events` 相同（已有 `event_id` + 替换后的乘客、站点、进出站类型与 UTC 时间）。服务按正式结算规则在内存中模拟：乘客变更时同时模拟原乘客与新乘客（被更正事件保留原始到达 `seq`，模拟结果与正式更正完全一致），但**不写入**事件、行程、账单、审计链或任何冻结表，也不追加审计记录。

```bash
curl -X POST http://localhost:8000/events/correction-preview \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt-002",
    "passenger_id": "p-1001",
    "station_id": "S07",
    "tap_type": "out",
    "occurred_at": "2026-09-21T08:30:00Z"
  }'
```

- `simulation` 按乘客逐项给出与正式更正完全一致的行程调账（`old/new/delta` 与完整计费依据 `basis`，行程消失时表现为负向冲正），并给出模拟前后的行程数；`event_pairing` / `pending_reason` 是被更正事件在模拟后的配对状态；
- `impacts` 逐项列出会变"过时"的已生成凭证——引用该行程的**对账批次**（`reconciliation`）、**清分链**（`clearing`，按根批次号）与**已结案争议**（`dispute`）；`cause` 为 `trip_vanished`（行程消失）/ `amount_changed`（金额变化）/ `station_changed`（站点对变化），`reason` 说明具体影响（例如清分新站对未被冻结规则覆盖时下一追补批次会被 `422` 拒绝、争议结案差额将变为待补退 / 待追回多少分）。仍处于 open 状态的争议不是已结案凭证，不会列出；
- 完全相同载荷（含同一时刻的其它时区写法）返回 `status="unchanged"`、空模拟与空影响；
- `event_id` 不存在 → `404`；事件处于复核暂存 → `409`（与正式更正的拒绝一致）；站点未知或时间非法 → `422`；
- 预览使用当前快照计算，同一请求在数据不变时结果相同；预览绝不改变 `POST /events` 的幂等与更正语义。

响应示例（`evt-001:evt-002` 由 500 分改价为 800 分，且该行程已被对账批次 `recon-2026-09-21-a` 按 500 分冻结）：

```json
{
  "event_id": "evt-002",
  "status": "preview",
  "current": {
    "event_id": "evt-002", "passenger_id": "p-1001", "station_id": "S04",
    "tap_type": "out", "occurred_at": "2026-09-21T08:30:00Z"
  },
  "replacement": {
    "event_id": "evt-002", "passenger_id": "p-1001", "station_id": "S07",
    "tap_type": "out", "occurred_at": "2026-09-21T08:30:00Z"
  },
  "event_pairing": "paired",
  "pending_reason": null,
  "simulation": [
    {
      "passenger_id": "p-1001",
      "trip_count_before": 1,
      "trip_count_after": 1,
      "adjustments": [
        {
          "trip_id": "evt-001:evt-002",
          "day_key": "2026-09-21",
          "week_key": "2026-W39",
          "old_amount_cents": 500,
          "new_amount_cents": 800,
          "delta_cents": 300,
          "basis": { "zone_fare_rule": "zone 1 -> zone 3", "...": "..." }
        }
      ]
    }
  ],
  "impacts": [
    {
      "artefact": "reconciliation",
      "artefact_id": "recon-2026-09-21-a",
      "passenger_id": "p-1001",
      "trip_id": "evt-001:evt-002",
      "cause": "amount_changed",
      "old_amount_cents": 500,
      "new_amount_cents": 800,
      "reason": "trip amount would change 500 -> 800 cents; batch 'recon-2026-09-21-a' froze internal amount 500 cents for it"
    },
    {
      "artefact": "reconciliation",
      "artefact_id": "recon-2026-09-21-a",
      "passenger_id": "p-1001",
      "trip_id": "evt-001:evt-002",
      "cause": "station_changed",
      "old_amount_cents": 500,
      "new_amount_cents": 800,
      "reason": "trip stations would change S01->S04 to S01->S07; batch 'recon-2026-09-21-a' froze its result against the old stations"
    }
  ],
  "impact_count": 2
}
```

### `POST /events/review-holds` — 刷卡事件复核暂存

调用方提交暂存号 `hold_id`、乘客、要冻结的事件 ID 列表（至少一个）与复核原因。服务在**一个事务内**确认每条事件都存在且属于该乘客、未被任何已关账工件引用、尚未被其它暂存单占用，然后为每条事件冻结一份不可修改的完整载荷快照，把事件标记为 `held`，并立即对该乘客重算：被冻结事件暂时退出配对、计费与待处理查询。

请求示例：

```bash
curl -X POST http://localhost:8000/events/review-holds \
  -H "Content-Type: application/json" \
  -d '{
    "hold_id": "hold-2026-10-22-a",
    "passenger_id": "p-1001",
    "event_ids": ["evt-003", "evt-004"],
    "review_reason": "中段行程分区计费复核"
  }'
```

- `event_ids` 顺序不影响载荷判定（服务按字典序规范化、去重），但同一请求内出现重复 ID 返回 `422`。
- 任一 `event_id` 不存在或不属于 `passenger_id` 返回 `422`；任一事件已被对账批次 / 清分批次 / 已结案争议（按行程 `trip_id = "<in>:<out>"` 的任一事件匹配）引用，或已处于另一暂存中，整批 `409`，**不写任何数据、不改变状态**。
- 暂存成功返回 `state="held"`、乐观版本 `version=1`，`snapshots` 是按规范化顺序排列的完整事件载荷快照（含原始到达 `event_seq`、逐事件 `payload_hash` 与 `frozen_at`）；`adjustments` 列出抽出这些事件后受影响行程的增减额（被冻结行程表现为负向冲正）。

响应示例（暂存一段 500 分行程，抽出后该行程冲回）：

```json
{
  "hold_id": "hold-2026-10-22-a",
  "status": "created",
  "state": "held",
  "version": 1,
  "passenger_id": "p-1001",
  "event_ids": ["evt-003", "evt-004"],
  "review_reason": "中段行程分区计费复核",
  "created_at": "2026-10-22T10:00:00Z",
  "released_at": null,
  "snapshots": [
    {"seq": 0, "event_id": "evt-003", "passenger_id": "p-1001",
     "station_id": "S01", "tap_type": "in",
     "occurred_at": "2026-10-22T12:00:00Z", "event_seq": 3,
     "payload_hash": "ab12…", "frozen_at": "2026-10-22T10:00:00Z"},
    {"seq": 1, "event_id": "evt-004", "passenger_id": "p-1001",
     "station_id": "S04", "tap_type": "out",
     "occurred_at": "2026-10-22T12:30:00Z", "event_seq": 4,
     "payload_hash": "cd34…", "frozen_at": "2026-10-22T10:00:00Z"}
  ],
  "release": null,
  "adjustments": [
    {"trip_id": "evt-003:evt-004", "day_key": "2026-10-22",
     "week_key": "2026-W43", "old_amount_cents": 500,
     "new_amount_cents": 0, "delta_cents": -500,
     "basis": {"voided": true, "reason": "trip no longer exists after re-pairing"}}
  ],
  "trip_change_count": 1
}
```

**幂等与冲突**：相同 `hold_id` + 相同载荷重放返回同一冻结结果（`status="duplicate"`，`event_ids` 顺序不影响判定）；同号异载荷返回 `409`。暂存期间对被冻结事件提交同 `event_id` 不同载荷的更正返回 `409`（完全相同的重复提交仍是幂等 `duplicate`）。

### `POST /events/review-holds/{hold_id}/release` — 复核完成后释放

复核方提交其视图上的**期望版本**。服务校验暂存处于 held（`version=1`），并逐条重算事件载荷哈希与冻结快照比对；版本不符或任一快照不一致都返回 `409`、**保持 held**。校验通过后清除 held 标记，事件按原始到达 `seq` 与事件时间重新进入配对 / 计费，调账结果冻结到只追加的释放记录中。

```bash
curl -X POST http://localhost:8000/events/review-holds/hold-2026-10-22-a/release \
  -H "Content-Type: application/json" \
  -d '{"expected_version": 1}'
```

- 暂存不存在 → `404`；`expected_version != 1` → `409`（版本冲突，事件保持 held）。
- 事件当前载荷与冻结快照不一致 → `409`，响应列出分歧事件，事件保持 held。
- 成功后 `state="released"`、`version=2`，`release.release_id` 固定为 `"RL-<hold_id>"`，`release.adjustments` 给出释放重算后各受影响行程的增减额（上例中冲回的行程恢复为 `delta +500`）。
- 释放记录只追加：相同释放请求幂等返回 `status="duplicate"`（同一 `release_id` / 调账 / 释放时间），任何不同释放载荷返回 `409`，不可二次释放。

响应节选：

```json
{
  "hold_id": "hold-2026-10-22-a",
  "status": "created",
  "state": "released",
  "version": 2,
  "released_at": "2026-10-22T11:00:00Z",
  "release": {
    "release_id": "RL-hold-2026-10-22-a",
    "expected_version": 1,
    "released_at": "2026-10-22T11:00:00Z",
    "adjustments": [
      {"trip_id": "evt-003:evt-004", "old_amount_cents": 0,
       "new_amount_cents": 500, "delta_cents": 500, "basis": {"...": "..."}}
    ]
  }
}
```

### `GET /events/review-holds/{hold_id}` — 查询暂存冻结状态

返回暂存号对应的冻结记录：规范化后的 `event_ids` 与逐事件快照、复核原因、`state`（`held` / `released`）、乐观版本 `version`，以及（释放后）另行追加的不可变释放结果 `release`（含调账）。`GET` 中 `status` 固定为 `"created"`（重放标记只出现在 `POST` 响应里）；暂存与快照在任何时候都按冻结原文返回。不存在返回 `404`。

```bash
curl http://localhost:8000/events/review-holds/hold-2026-10-22-a
```

### `POST /reconciliations` — 扣款对账批次

调用方提交批次号、UTC 对账区间（半开）、时间容差（秒）与外部扣款明细，服务将**出站时间**（`ended_at`）落在 `[window_start, window_end)` 内的内部行程费用做成快照并完成一次性对账。

请求示例（完整地址 `http://localhost:8000`）：

```bash
curl -X POST http://localhost:8000/reconciliations \
  -H "Content-Type: application/json" \
  -d '{
    "batch_id": "recon-2026-09-25-a",
    "window_start": "2026-09-25T00:00:00Z",
    "window_end": "2026-09-26T00:00:00Z",
    "tolerance_seconds": 300,
    "records": [
      {"serial_no": "x1", "passenger_id": "RA",
       "charged_at": "2026-09-25T08:30:02Z", "amount_cents": 500,
       "trip_id": "ra1:ra2"},
      {"serial_no": "x2", "passenger_id": "RA",
       "charged_at": "2026-09-25T09:19:00Z", "amount_cents": 150}
    ]
  }'
```

- `serial_no`：批次内唯一流水号；`trip_id` 可选；金额为整数分；时间接受任意时区偏移并归一化为 UTC。
- **第一阶段精确锁定**：外部记录的 `trip_id` 与快照行程精确对应且乘客一致时优先锁定（不参与容差匹配）；`trip_id` 在快照中不存在或乘客不符时，该记录回落至容差匹配，并在 `reason` 中说明。
- **第二阶段容差匹配**：其余记录按乘客分组，仅当 `|charged_at - ended_at| <= tolerance_seconds` 时构成候选；每名乘客内做一对一匹配，先最大化配对数，再最小化总时间差。若存在多个同样最优但配对关系不同的解，涉及的记录全部标记为 `ambiguous`（歧义），绝不按提交顺序强行配对；判定依据 `reason` 会列出可能的配对对象。
- **结果类别**（每条结果一行）：
  - `matched` 金额一致；`overcharged` 外部多扣；`undercharged` 外部少扣（`difference_cents = 外部 - 内部`）；
  - `internal_only` 仅内部有；`external_only` 仅外部有；`ambiguous` 歧义；
  - `match_method` 标明配对方式：`trip_id` 或 `tolerance`。
- **幂等与冲突**：相同 `batch_id` + 相同载荷重复提交返回已固化结果（`status="duplicate"`，明细记录顺序不影响载荷判定）；相同 `batch_id` + 不同载荷返回 `409`。
- **结果不可变**：结果在批次创建时随快照持久化；后续刷卡事件更正（包括迟到记录）不会改变任何已生成批次，只有新批次能看到更正后的费用。
- 非法区间（`window_end <= window_start`）或批次内流水号重复返回 `422`。

响应示例：

```json
{
  "batch_id": "recon-2026-09-25-a",
  "status": "created",
  "window_start": "2026-09-25T00:00:00Z",
  "window_end": "2026-09-26T00:00:00Z",
  "tolerance_seconds": 300,
  "currency": "CNY",
  "summary": {
    "matched": 1,
    "overcharged": 0,
    "undercharged": 1,
    "internal_only": 0,
    "external_only": 0,
    "ambiguous": 0,
    "snapshot_trip_count": 2,
    "external_record_count": 2
  },
  "results": [
    {
      "category": "matched",
      "passenger_id": "RA",
      "trip_id": "ra1:ra2",
      "serial_no": "x1",
      "internal_amount_cents": 500,
      "external_amount_cents": 500,
      "difference_cents": 0,
      "internal_ended_at": "2026-09-25T08:30:00Z",
      "external_charged_at": "2026-09-25T08:30:02Z",
      "match_method": "trip_id",
      "reason": "locked by exact trip_id; amounts equal"
    },
    {
      "category": "undercharged",
      "passenger_id": "RA",
      "trip_id": "ra3:ra4",
      "serial_no": "x2",
      "internal_amount_cents": 200,
      "external_amount_cents": 150,
      "difference_cents": -50,
      "internal_ended_at": "2026-09-25T09:20:00Z",
      "external_charged_at": "2026-09-25T09:19:00Z",
      "match_method": "tolerance",
      "reason": "paired by time tolerance (|time diff| 60s <= tolerance 300s); external undercharged by 50 cents"
    }
  ]
}
```

### `POST /network/validate` — 换乘网络校验

调用方提交校验号、UTC 生效区间（半开）、指定枢纽 `hubs`、全部服务站点 `stations`、带起止时间的**线路段** `segments`（有向，属于某条 `line_id`）与**步行换乘边** `transfers`（有向，两端站直接相连）。每条边都有自己的半开生效区间 `[start_at, end_at)`。

服务以生效区间边界和所有边的（裁剪后）起止时间为切分点，把时间轴切成若干**时间片**；每个时间片内有向图是静态的，边的区间与生效窗口取交（完全落在窗口外的边不参与，端点相接合法）。随后按固定优先级逐片检查并返回**首个冲突**、所在时间片与（可能存在的）最短证据路径：

1. `self_loop_edge`：端点相同的自环边（按其最早生效时间片报告）；
2. `overlapping_interval`：同一声明标识的两条边生效区间有正时长重叠——线路段标识为 `(line_id, 起点, 终点)`，步行边标识为 `(起点, 终点)`；首尾相接的区间合法；
3. `duplicate_edge`：同一时间片内两个不同声明提供相同有向站对（平行边，例如同一对站同时有线路段和步行边）；
4. `transfer_asymmetry`：某时间片内步行边 `A->B` 存在但反向 `B->A` 不存在；响应附 `evidence_path`：图中 `B -> A` 的**最短绕行路径**（跳数最少、并列时按站点字典序），完全无法返回时为 `null`；
5. `unreachable_station`：某时间片内某枢纽无法到达某服务站（无证据路径，`evidence_path` 为 `null`）。

请求示例：

```bash
curl -X POST http://localhost:8000/network/validate \
  -H "Content-Type: application/json" \
  -d '{
    "validation_id": "net-2026-10-01-a",
    "effective_start": "2026-10-01T00:00:00Z",
    "effective_end": "2026-10-02T00:00:00Z",
    "hubs": ["H1"],
    "stations": ["H1", "S1", "S2", "S3"],
    "segments": [
      {"line_id": "L1", "segment_id": "L1-h1-s1",
       "from_station": "H1", "to_station": "S1",
       "start_at": "2026-10-01T05:30:00Z", "end_at": "2026-10-01T23:30:00Z"},
      {"line_id": "L1", "segment_id": "L1-s1-h1",
       "from_station": "S1", "to_station": "H1",
       "start_at": "2026-10-01T05:30:00Z", "end_at": "2026-10-01T23:30:00Z"}
    ],
    "transfers": [
      {"from_station": "S1", "to_station": "S2",
       "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
      {"from_station": "S2", "to_station": "S1",
       "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"}
    ]
  }'
```

- `hubs` 必须是 `stations` 的子集；所有边引用的站点必须在 `stations` 中；时间接受任意时区偏移并归一化为 UTC。
- 非法请求返回 `422`：生效区间为空、站点 / 枢纽重复、枢纽不在站点集合中、边引用未知站点、边自身区间为空。
- **幂等与冲突**：相同 `validation_id` + 相同载荷（站点 / 枢纽 / 边的数组顺序、时间偏移写法不影响判定）重复提交返回已存结果（`status="duplicate"`）；相同 `validation_id` + 不同载荷返回 `409`。
- 无论通过与否，校验结果在校验时一次性持久化；重放只返回存储结果，绝不重新计算。

通过的响应：

```json
{
  "validation_id": "net-2026-10-01-a",
  "status": "created",
  "passed": true,
  "effective_start": "2026-10-01T00:00:00Z",
  "effective_end": "2026-10-02T00:00:00Z",
  "slice_count": 3,
  "slices": [
    {"index": 0, "start": "2026-10-01T00:00:00Z", "end": "2026-10-01T05:30:00Z"},
    {"index": 1, "start": "2026-10-01T05:30:00Z", "end": "2026-10-01T23:30:00Z"},
    {"index": 2, "start": "2026-10-01T23:30:00Z", "end": "2026-10-02T00:00:00Z"}
  ],
  "conflict": null
}
```

存在冲突时（下例中夜间首班车未开，时间片 0 内枢纽无法到达任何远端站点）：

```json
{
  "validation_id": "net-2026-10-01-a",
  "status": "created",
  "passed": false,
  "effective_start": "2026-10-01T00:00:00Z",
  "effective_end": "2026-10-02T00:00:00Z",
  "slice_count": 3,
  "slices": [ "...同上..." ],
  "conflict": {
    "code": "unreachable_station",
    "slice": {"index": 0, "start": "2026-10-01T00:00:00Z", "end": "2026-10-01T05:30:00Z"},
    "message": "station S1 is not reachable from hub H1 in this slice",
    "hub": "H1",
    "station": "S1",
    "evidence_path": null
  }
}
```

步行边不对称但可绕行时，`evidence_path` 给出最短返回路径（路径中的边标注 `edge_kind` 与所属线路）：

```json
{
  "code": "transfer_asymmetry",
  "slice": {"index": 1, "start": "2026-10-01T05:30:00Z", "end": "2026-10-01T23:30:00Z"},
  "message": "walking transfer S1 -> S2 has no reverse walking transfer S2 -> S1 active in this slice; shortest alternative return path S2 -> S1 takes 1 hop(s)",
  "edge": {"edge_kind": "transfer", "from_station": "S1", "to_station": "S2",
           "start_at": "2026-10-01T05:30:00Z", "end_at": "2026-10-01T23:30:00Z"},
  "evidence_path": [
    {"from_station": "S2", "to_station": "S1", "edge_kind": "segment",
     "line_id": "L2", "segment_id": "L2-s2-s1"}
  ]
}
```

### `POST /network/snapshots` — 固化通过的网络版本

只有 `passed=true` 的校验可以固化；请求体只含校验号。快照在固化时复制当时的规范化载荷与校验结果：

```bash
curl -X POST http://localhost:8000/network/snapshots \
  -H "Content-Type: application/json" \
  -d '{"validation_id": "net-2026-10-01-a"}'
```

- 首次固化返回 `status="created"`；对同一校验号重复固化幂等返回 `status="duplicate"`（`frozen_at` 不变）。
- 校验号不存在 → `404`；校验未通过（有冲突）→ `409`，且不留快照。

### `GET /network/snapshots/{snapshot_id}` — 查询固化快照

`snapshot_id` 即校验号。返回固化时的完整校验结果与固化时间 `frozen_at`；快照在存储层带禁止 `UPDATE`/`DELETE` 的触发器，**后续任何同号校验（幂等重放或冲突拒绝）都不会改写已固化内容**。不存在返回 `404`。

### `POST /disputes` — 提起票价争议

乘客对某段已配对行程提起争议，服务在创建时**冻结该行程当时的收费快照**（应收金额、基础票价、换乘减免与完整计费依据），审核始终针对快照应收进行。申请退款金额不得超过快照应收。

```bash
curl -X POST http://localhost:8000/disputes \
  -H "Content-Type: application/json" \
  -d '{
    "dispute_id": "disp-2026-09-27-0001",
    "passenger_id": "p-1001",
    "trip_id": "evt-001:evt-002",
    "claimed_amount_cents": 400,
    "reason": "分区计费错误，实际为同区行程"
  }'
```

- 金额为整数分，`claimed_amount_cents >= 0` 且**不得超过**快照应收（`snapshot.amount_cents`），否则 `422`。
- `trip_id` 不存在（行程尚未配对或已消失）→ `404`；行程属于其他乘客 → `422`。
- 争议创建后 `state="open"`、乐观版本 `version=1`；响应内嵌冻结快照 `snapshot`（含 `frozen_at`），此时 `refund=null`、`resolved_at=null`。
- **幂等与冲突**：相同 `dispute_id` + 相同载荷重复提交返回已存结果（`status="duplicate"`，快照不重新冻结）；相同 `dispute_id` + 不同载荷返回 `409`。

响应示例：

```json
{
  "dispute_id": "disp-2026-09-27-0001",
  "status": "created",
  "state": "open",
  "version": 1,
  "passenger_id": "p-1001",
  "trip_id": "evt-001:evt-002",
  "claimed_amount_cents": 400,
  "reason": "分区计费错误，实际为同区行程",
  "created_at": "2026-09-27T10:00:00Z",
  "resolved_at": null,
  "snapshot": {
    "trip_id": "evt-001:evt-002",
    "passenger_id": "p-1001",
    "amount_cents": 500,
    "base_fare_cents": 500,
    "transfer_discount_cents": 0,
    "basis": { "zone_fare_rule": "zone 1 -> zone 2", "...": "..." },
    "frozen_at": "2026-09-27T10:00:00Z"
  },
  "refund": null,
  "recomputed": {
    "current_amount_cents": 500,
    "settled_amount_cents": null,
    "difference_cents": 0,
    "direction": "none"
  }
}
```

### `POST /disputes/{dispute_id}/resolve` — 一次性审核结案

审核方提交其审核所依据的**乐观版本**与通过金额，服务在同一事务内把争议结案（版本变为 2）并写入**一条且仅一条不可变退款流水**（`refunds` 表，`refund_id = "RF-<dispute_id>"`，带禁止 `UPDATE`/`DELETE` 的触发器）。

```bash
curl -X POST http://localhost:8000/disputes/disp-2026-09-27-0001/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "expected_version": 1,
    "approved_amount_cents": 350,
    "review_note": "核定部分退款 3.50 元"
  }'
```

- `approved_amount_cents` 可在 `[0, claimed_amount_cents]` 内取值（0 表示驳回），超过申请额 → `422`。
- 争议不存在 → `404`。
- **版本冲突 `409`**：争议处于 open 但 `expected_version != 1`（客户端基于过期视图），或争议已结案且请求载荷与原结案请求不完全一致（已结退款不可覆盖、不可二次结案）。
- **幂等**：与原结案请求完全相同（同版本、同金额、同备注）的重复提交返回同一退款流水（`status="duplicate"`，`refund_id`/金额/结案时间均不变），不新增流水。
- 结案后响应中 `state="resolved"`、`version=2`、`resolved_at` 有值，`refund` 为不可变流水；`recomputed.settled_amount_cents = 快照应收 - 已退金额`（乘客实际净付）。

### `GET /disputes/{dispute_id}` — 查询争议与结算差额

返回争议、冻结快照、退款流水（若已结案）以及**实时重算结果** `recomputed`。服务在每次查询时读取该行程当前的应收金额（可能已被迟到/更正事件重算），与乘客净付（`快照应收 - 已退金额`）对比：

- `direction="top_up_due"`、`difference_cents > 0`：净付高于当前应收，**待补退**差额；
- `direction="clawback_due"`、`difference_cents < 0`：净付低于当前应收，**待追回**差额；
- `direction="none"`：无差额（或行程已作废、当前应收 0）。

差额仅作报告：**服务不会自动发起二次退款，已写入的退款流水与冻结快照永不被改写**。open 争议没有已结算金额（`settled_amount_cents=null`、`direction="none"`），差额只对已结案争议有意义。若争议行程在重算后消失，当前应收按 0 处理。

```bash
# 结案退款 350（快照应收 500，净付 150）；此后事件更正把行程重算为 800：
curl http://localhost:8000/disputes/disp-2026-09-27-0001
```

```json
{
  "dispute_id": "disp-2026-09-27-0001",
  "status": "created",
  "state": "resolved",
  "version": 2,
  "...": "...",
  "snapshot": { "amount_cents": 500, "...": "..." },
  "refund": {
    "refund_id": "RF-disp-2026-09-27-0001",
    "dispute_id": "disp-2026-09-27-0001",
    "passenger_id": "p-1001",
    "trip_id": "evt-001:evt-002",
    "amount_cents": 350,
    "review_note": "核定部分退款 3.50 元",
    "created_at": "2026-09-27T11:00:00Z"
  },
  "recomputed": {
    "current_amount_cents": 800,
    "settled_amount_cents": 150,
    "difference_cents": -650,
    "direction": "clawback_due"
  }
}
```

争议不存在返回 `404`。

争议不存在返回 `404`。

### `POST /clearings` — 运营方票款首次清分（关账）

调用方提交 `clearing_id`、按**出站时间**（`ended_at`）计算的 UTC 半开结算区间，以及按**有向起讫站对**配置的运营方万分比分摊规则。服务冻结区间与规则，读取区间内当前已配对行程的实收（`trip_charges.amount_cents`），逐笔拆分并写入不可变流水。

请求示例（完整地址 `http://localhost:8000`）：

```bash
curl -X POST http://localhost:8000/clearings \
  -H "Content-Type: application/json" \
  -d '{
    "clearing_id": "clr-2026-10-10-a",
    "window_start": "2026-10-10T00:00:00Z",
    "window_end":   "2026-10-11T00:00:00Z",
    "rules": [
      {"from_station": "S01", "to_station": "S04", "shares": [
        {"operator_id": "OA", "basis_points": 3333},
        {"operator_id": "OB", "basis_points": 3333},
        {"operator_id": "OC", "basis_points": 3334}
      ]},
      {"from_station": "S05", "to_station": "S02", "shares": [
        {"operator_id": "OA", "basis_points": 4000},
        {"operator_id": "OD", "basis_points": 4000},
        {"operator_id": "OE", "basis_points": 2000}
      ]}
    ]
  }'
```

- **整批拒绝**：`window_end <= window_start`、同一站对重复配置、规则内运营方重复、份额为负数、任一站对份额合计 ≠ 10000，或区间内存在**没有匹配规则**的行程，均返回 `422` 且**不写任何数据**（时间接受任意时区偏移并归一化为 UTC）。
- **尾分分配**：先按 `floor(金额 × 万分比 / 10000)` 取整，未分尽的分按小数余数**降序**、余数相同按运营方 ID **升序**逐分补发。例如 500 分按 3333/3333/3334 拆分为 OA=167、OB=166、OC=167（OA/OB 余数并列时 ID 较小者得尾分）；每笔拆分合计严格等于行程金额。
- **冻结语义**：首账 `seq=0`、`kind="initial"`，响应返回冻结的区间、规范化规则（按站对与运营方排序）、全部流水 `entries` 与链上运营方累计 `operator_totals`；首账流水带当时毛额 `gross_amount_cents`、万分比 `basis_points` 与 `cumulative_amount_cents`。
- **幂等与冲突**：相同 `clearing_id` + 相同载荷重放返回 `status="duplicate"`（规则与份额数组顺序不影响判定）；同号异载荷返回 `409`。

响应示例（节选）：

```json
{
  "clearing_id": "clr-2026-10-10-a",
  "root_clearing_id": "clr-2026-10-10-a",
  "status": "created",
  "kind": "initial",
  "seq": 0,
  "window_start": "2026-10-10T00:00:00Z",
  "window_end": "2026-10-11T00:00:00Z",
  "rules": [ {"from_station": "S01", "to_station": "S04", "shares": [ "..."] } ],
  "base_clearing_id": null,
  "trip_count": 3,
  "reversed_trip_count": 0,
  "ledger_row_count": 9,
  "gross_amount_cents": 1300,
  "net_amount_cents": 1300,
  "entries": [
    {
      "seq": 0,
      "trip_id": "op1:op2",
      "passenger_id": "OP",
      "from_station": "S01",
      "to_station": "S04",
      "ended_at": "2026-10-10T08:30:00Z",
      "operator_id": "OA",
      "basis_points": 3333,
      "gross_amount_cents": 500,
      "amount_cents": 167,
      "prior_amount_cents": 0,
      "cumulative_amount_cents": 167,
      "kind": "initial"
    }
  ],
  "operator_totals": [
    {"operator_id": "OA", "amount_cents": 460},
    {"operator_id": "OB", "amount_cents": 299},
    {"operator_id": "OC", "amount_cents": 301},
    {"operator_id": "OD", "amount_cents": 160},
    {"operator_id": "OE", "amount_cents": 80}
  ],
  "created_at": "2026-10-10T12:00:00Z"
}
```

### `POST /clearings/adjustments` — 更正后追补批次

刷卡更正（`POST /events` 同事件 ID 不同载荷）可能改变行程金额、站点或配对关系。**已关账流水绝不改写**：更正后用**新的** `clearing_id` 提交追补批次并引用首次关账；区间与规则沿用首账冻结值，服务读取区间内**当前**行程应收重新拆分，减去该行程/运营方的历次累计，只追加非零的正负差额。

```bash
curl -X POST http://localhost:8000/clearings/adjustments \
  -H "Content-Type: application/json" \
  -d '{
    "clearing_id": "clr-2026-10-10-b",
    "base_clearing_id": "clr-2026-10-10-a"
  }'
```

- 追补批次 `kind="adjustment"`、`seq` 在同一 `root_clearing_id` 链上自 1 递增；`base_clearing_id` 必须是存在的**首次关账**（引用追补批次 → `422`，不存在 → `404`）。
- 每条差额行 `basis_points=null`，并给出 `amount_cents`（带符号差额）、`prior_amount_cents`（本批次前链上累计）与 `cumulative_amount_cents`（本批次后累计）；未变化行程**不产生**差额行。
- **行程消失冲回**：末次批次仍在区间内、当前已消失的行程，按末次记载的运营方逐户追加负差额，累计冲回到 0（`gross_amount_cents=0`、`reversed_trip_count` 计数）。
- **站点对变化**：同一行程改配到另一个**冻结规则已覆盖**的站对时，旧规则独有运营方冲回到 0、新规则独有运营方按新拆分补入，共同运营方按新旧目标差额调整；当前出现冻结规则**未覆盖**的新站对时整批 `422`（首账规则不可变，需另开首次关账）。
- **重复调整不重复入账**：差额恒为"当前目标 − 链上累计"。同 `clearing_id` 同载荷重放为幂等 `duplicate`；状态未变时换一个新批次号再追补，返回空 `entries`、`net_amount_cents=0` 的零差额批次。同号异载荷（含 `base_clearing_id` 不同）返回 `409`。

上例（`op3:op4` 因更正而消失，其余两笔金额不变）响应节选：

```json
{
  "clearing_id": "clr-2026-10-10-b",
  "root_clearing_id": "clr-2026-10-10-a",
  "kind": "adjustment",
  "seq": 1,
  "base_clearing_id": "clr-2026-10-10-a",
  "trip_count": 2,
  "reversed_trip_count": 1,
  "gross_amount_cents": 900,
  "net_amount_cents": -400,
  "entries": [
    {"seq": 0, "trip_id": "op3:op4", "operator_id": "OA", "basis_points": null,
     "gross_amount_cents": 0, "amount_cents": -160,
     "prior_amount_cents": 160, "cumulative_amount_cents": 0, "kind": "adjustment"},
    {"seq": 1, "trip_id": "op3:op4", "operator_id": "OD", "basis_points": null,
     "gross_amount_cents": 0, "amount_cents": -160,
     "prior_amount_cents": 160, "cumulative_amount_cents": 0, "kind": "adjustment"},
    {"seq": 2, "trip_id": "op3:op4", "operator_id": "OE", "basis_points": null,
     "gross_amount_cents": 0, "amount_cents": -80,
     "prior_amount_cents": 80, "cumulative_amount_cents": 0, "kind": "adjustment"}
  ],
  "operator_totals": [
    {"operator_id": "OA", "amount_cents": 300},
    {"operator_id": "OB", "amount_cents": 299},
    {"operator_id": "OC", "amount_cents": 301},
    {"operator_id": "OD", "amount_cents": 0},
    {"operator_id": "OE", "amount_cents": 0}
  ]
}
```

### `GET /clearings/{clearing_id}` — 查询清分冻结结果

返回该批次创建时固化的完整内容：首账为 `kind="initial"/seq=0`（含冻结区间、规范化规则与全额拆分流水），追补批次为 `kind="adjustment"`（含 `base_clearing_id` 与正负差额行）；`operator_totals` 始终是截至查询时**整条链**的运营方累计头寸（首账自己的流水永不改变，但后续追补会改变链上累计）。`status` 在 GET 中固定为 `"created"`（重放标记只出现在 POST 响应里）。不存在返回 `404`。

```bash
curl http://localhost:8000/clearings/clr-2026-10-10-a
```

### `POST /event-quality/scans` — 刷卡事件质量扫描

调用方提交扫描号 `scan_id`、UTC 半开区间 `[window_start, window_end)`（作用于事件的 `occurred_at`）、可选乘客白名单 `passengers`、**重复刷卡时间窗** `duplicate_window_seconds`（秒）以及**站点间最短行程秒数映射** `min_travel_seconds`（有向，形如 `{"出站站": {"进站站": 秒数}}`）。服务只读取 `events`，对区间内事件按乘客分组、按 `(occurred_at, seq)` 排序，逐条与该乘客的**上一条事件**比对，识别两类告警：

- `duplicate_tap`（重复同类刷卡）：相邻两条事件类型相同（`in→in` 或 `out→out`，即在没有互补动作的情况下重复操作）且时间间隔 **≤** `duplicate_window_seconds`（边界取等仍算重复）。
- `min_travel_violation`（最短行程时序冲突）：相邻事件为 `out→in`，出站站 A 到进站站 B 在 `min_travel_seconds["A"]["B"]` 中有配置，而实际间隔 **严格小于** 该最短秒数（恰好相等合法）；映射中没有的有向站对不检查。

每条告警返回触发（较晚）事件的 `event_id`、`passenger_id`、规则 `rule`、`actual_interval_seconds`（两条证据事件的实际间隔，秒）、`threshold_seconds`（被突破的配置阈值）以及按时间排序的**证据链** `evidence`（两条刷卡事件的完整信息：事件 ID、乘客、站点、刷卡类型、UTC 时间、到达序号）。告警整体按触发事件的 `(occurred_at, seq)` 全局排序。

请求示例：

```bash
curl -X POST http://localhost:8000/event-quality/scans \
  -H "Content-Type: application/json" \
  -d '{
    "scan_id": "eqs-2026-10-05-a",
    "window_start": "2026-10-05T00:00:00Z",
    "window_end": "2026-10-06T00:00:00Z",
    "passengers": ["p-1001", "p-1002"],
    "duplicate_window_seconds": 60,
    "min_travel_seconds": {
      "S04": {"S01": 300},
      "S07": {"S01": 600}
    }
  }'
```

- `passengers` 省略（或为 `null`）时扫描全部乘客；提供时为至少一个乘客 ID 的白名单（重复 ID 自动去重）。时间接受任意时区偏移并归一化为 UTC。
- 非法请求返回 `422`：区间为空（`window_end <= window_start`）、`duplicate_window_seconds < 0`、最短行程秒数为负。
- **幂等与冲突**：相同 `scan_id` + 相同载荷（乘客顺序 / 重复、映射键顺序、时间偏移写法均不影响判定）重复提交返回已固化结果（`status="duplicate"`，绝不重算）；相同 `scan_id` + 不同载荷返回 `409`。
- **隔离与只追加**：扫描结果只写入独立的 `event_quality_scans` / `event_quality_alerts` 两张只追加表（存储层触发器禁止 `UPDATE` / `DELETE`，告警表带外键约束）；**不改动**事件、行程、账单、争议 / 退款，也**不追加审计记录**。后续事件更正不影响已固化扫描，只有新扫描才能看到新数据。

响应示例：

```json
{
  "scan_id": "eqs-2026-10-05-a",
  "status": "created",
  "window_start": "2026-10-05T00:00:00Z",
  "window_end": "2026-10-06T00:00:00Z",
  "passengers": ["p-1001", "p-1002"],
  "duplicate_window_seconds": 60,
  "min_travel_seconds": {"S04": {"S01": 300}, "S07": {"S01": 600}},
  "event_count": 13,
  "alert_count": 2,
  "alerts": [
    {
      "event_id": "qe3",
      "passenger_id": "p-1001",
      "rule": "min_travel_violation",
      "actual_interval_seconds": 20.0,
      "threshold_seconds": 300,
      "evidence": [
        {"event_id": "qe2", "passenger_id": "p-1001", "station_id": "S04",
         "tap_type": "out", "occurred_at": "2026-10-05T08:30:00Z", "seq": 2},
        {"event_id": "qe3", "passenger_id": "p-1001", "station_id": "S01",
         "tap_type": "in", "occurred_at": "2026-10-05T08:30:20Z", "seq": 3}
      ]
    },
    {
      "event_id": "qe4",
      "passenger_id": "p-1001",
      "rule": "duplicate_tap",
      "actual_interval_seconds": 10.0,
      "threshold_seconds": 60,
      "evidence": [
        {"event_id": "qe3", "passenger_id": "p-1001", "station_id": "S01",
         "tap_type": "in", "occurred_at": "2026-10-05T08:30:20Z", "seq": 3},
        {"event_id": "qe4", "passenger_id": "p-1001", "station_id": "S01",
         "tap_type": "in", "occurred_at": "2026-10-05T08:30:30Z", "seq": 4}
      ]
    }
  ]
}
```

### `GET /event-quality/scans/{scan_id}` — 查询扫描结果

返回扫描创建时固化的完整结果（`status` 固定为 `"created"`；重放标记只在 `POST` 响应中出现）。不存在返回 `404`。

```bash
curl http://localhost:8000/event-quality/scans/eqs-2026-10-05-a
```

### `POST /privacy/exports` — 匿名出行轨迹导出（k-匿名整组抑制）

调用方提交导出号 `export_id`、UTC 半开区间 `[window_start, window_end)`（作用于行程的**进站时间** `started_at`）、**时间桶分钟** `bucket_minutes`、**起终点站分组映射** `station_groups`（`{站点: 组名}`，同一映射同时用于起点 `from_station` 与终点 `to_station`）以及最小群体 `k`。服务只读取已配对的 `trips`，对区间内行程做如下处理：

1. **稳定假名化**：每个 `passenger_id` 用服务器配置的 `PRIVACY_SALT` 做 `HMAC-SHA256`，输出为十六进制假名。同一真实乘客在本次导出的多段行程、以及同一服务器上的多次导出中保持**同一假名**；原始乘客 ID 不会出现在响应或任何存储行中，没有盐值也无法由假名反推。
2. **时间桶**：按 `bucket_minutes` 把进站时刻归入**自 Unix 纪元对齐**的固定宽度 UTC 桶（桶起点为 `floor(t / bucket) / bucket`），与请求窗口起点无关；例如 08:50 在 60 分钟桶中归入 08:00。
3. **等价类与去重计数**：等价类键为 `(时间桶起点, 起点组, 终点组)`，统计每类中**去重后的假名乘客数**（同一乘客在同一类中的多段行程只计 1 人）。
4. **整组抑制**：去重乘客数 `< k` 的等价类**整组丢弃**——该类没有任何一行业务行程被导出，避免逐行泄露小群体；只保留人数 `>= k` 的等价类行程。响应中的 `trips` 每行仅含 `pseudonym`、`bucket_start`、`origin_group`、`destination_group` 四个字段。

请求示例（`k=3`，站点分为 `core` / `mid` / `suburb` 三组）：

```bash
curl -X POST http://localhost:8000/privacy/exports \
  -H "Content-Type: application/json" \
  -d '{
    "export_id": "pex-2026-11-15-a",
    "window_start": "2026-11-15T00:00:00Z",
    "window_end": "2026-11-16T00:00:00Z",
    "bucket_minutes": 60,
    "station_groups": {
      "S01": "core", "S02": "core", "S03": "core",
      "S04": "mid",  "S05": "mid",  "S06": "mid",
      "S07": "suburb","S08": "suburb","S09": "suburb"
    },
    "k": 3
  }'
```

- 时间接受任意时区偏移并归一化为 UTC；`station_groups` 的键顺序不影响载荷判定。
- 非法请求返回 `422`：区间为空（`window_end <= window_start`）、`bucket_minutes < 1`、`k < 1`、`station_groups` 为空，或区间内某段行程的起点 / 终点站点缺少分组映射（错误信息会列出未映射站点）。
- **幂等与冲突**：相同 `export_id` + 相同载荷（映射键顺序、时间偏移写法均不影响判定）重复提交返回已固化结果（`status="duplicate"`，假名与抑制决策绝不重算）；相同 `export_id` + 不同载荷返回 `409`。
- **只追加、不可变**：结果在创建时一次性固化到独立的 `privacy_exports` / `privacy_export_trips` 两张只追加表（存储层触发器禁止 `UPDATE` / `DELETE`）；后续刷卡事件更正不影响已固化导出，只有新导出才能看到新数据。导出**不改动**事件、行程、账单、争议 / 退款，也**不追加审计记录**。

响应示例：

```json
{
  "export_id": "pex-2026-11-15-a",
  "status": "created",
  "window_start": "2026-11-15T00:00:00Z",
  "window_end": "2026-11-16T00:00:00Z",
  "bucket_minutes": 60,
  "k": 3,
  "station_groups": {"S01": "core", "S04": "mid", "...": "..."},
  "input_trip_count": 11,
  "equivalence_class_count": 5,
  "retained_class_count": 2,
  "suppressed_class_count": 3,
  "retained_trip_count": 6,
  "suppressed_trip_count": 5,
  "classes": [
    {"bucket_start": "2026-11-15T08:00:00Z", "origin_group": "core",
     "destination_group": "mid", "passenger_count": 3, "trip_count": 3},
    {"bucket_start": "2026-11-15T09:00:00Z", "origin_group": "mid",
     "destination_group": "core", "passenger_count": 3, "trip_count": 3}
  ],
  "trips": [
    {"pseudonym": "2f9c…（64 位十六进制 HMAC）",
     "bucket_start": "2026-11-15T08:00:00Z",
     "origin_group": "core", "destination_group": "mid"}
  ]
}
```

`classes` 给出每个保留等价类的去重乘客数 `passenger_count` 与保留行程数 `trip_count`；`suppressed_class_count` / `suppressed_trip_count` 报告因不足 `k` 而被整组抑制的类与行程数量（被抑制的行程本身不返回）。

### `GET /privacy/exports/{export_id}` — 查询导出结果

返回导出创建时固化的完整结果（`status` 固定为 `"created"`；重放标记只在 `POST` 响应中出现）。不存在返回 `404`。

```bash
curl http://localhost:8000/privacy/exports/pex-2026-11-15-a
```

### `POST /disruption/replans` — 突发中断疏运重排

突发中断导致既有班次不可用时，调用方提交重排号 `replan_id`、**最小换乘秒数** `min_transfer_seconds`、一批**替代班次** `services` 与待疏运**乘客组** `groups`，服务一次性算出冻结的重排结果。

- 每列替代班次 `services[i]`：`service_id`、核定容量 `capacity`（一个整数，表示该班次在**每个相邻区段**上可承载的人数）、以及**按站序排列**的停靠站 `stops`；每个停靠站含 `station_id`、到站 `arrival_at`、发时 `departure_at`（时间接受任意时区偏移并归一化为 UTC，同一站可到发同时刻）。
- 每个乘客组 `groups[i]`：`group_id`、`origin`、`destination`、最早可出发时刻 `ready_at` 与 `party_size`。**每组必须整体搭乘**，不允许拆分。

服务按 **`(ready_at, group_id)` 升序**逐组处理；每组在当前**各相邻区段剩余容量**约束下选出最优行程，行程择优规则依次为：

1. **到达目的地最早**；
2. 到达时刻相同，**换乘次数更少**（乘坐班次数 − 1）；
3. 仍并列，**班次 ID 序列字典序更小**（逐段比较 `service_id`）。

搜索在每个站点保留所有仍可能胜出的**非支配标签**，而不是只保留到达最早的单一标签：两条路径先后到达同一换乘站时，较早但已乘坐更多班次的路径不会挤掉稍晚但换乘更少的路径——只要两者都能搭上同一末段班次并同时抵达终点，稍晚路径仍可凭"换乘更少"在终点反超。一个标签仅当被全面支配时才被剪除，即存在另一同站标签到达不更晚、换乘不更多、班次 ID 序列字典序不更大，且其已用班次与已访站点均为对方子集（此后者能走的后续路线前者也都能走）。最终仍按上述三条依次决胜；剩余容量不足的标签在扩展时即被剪除，不可行标签绝不参与决胜。

选出行程后按每组人数**逐段扣减**该行程经过的每个相邻区段容量，后续乘客组只能看到剩余座位。首班乘车只要求发时不早于 `ready_at`；在某站换乘另一班次时，后班发时距前班到站必须 **≥** `min_transfer_seconds`（恰好相等合法）。起点与终点相同的乘客组视为零站段成行（不消耗容量）。找不到可行行程时整组滞留，并按最先遇到的阻碍返回原因：

- `origin_not_served`：没有任何替代班次停靠起点；
- `destination_not_served`：没有任何替代班次停靠终点；
- `unreachable_destination`：时刻网络中不存在从起点到终点的可行行程；
- `insufficient_capacity`：忽略容量时可达，但考虑逐段剩余容量后没有任何行程能整体容纳该组。

请求示例：

```bash
curl -X POST http://localhost:8000/disruption/replans \
  -H "Content-Type: application/json" \
  -d '{
    "replan_id": "dr-2026-09-27-a",
    "min_transfer_seconds": 300,
    "services": [
      {"service_id": "ALPHA", "capacity": 100, "stops": [
        {"station_id": "A", "arrival_at": "2026-09-27T08:00:00Z",
         "departure_at": "2026-09-27T08:05:00Z"},
        {"station_id": "B", "arrival_at": "2026-09-27T08:20:00Z",
         "departure_at": "2026-09-27T08:25:00Z"},
        {"station_id": "E", "arrival_at": "2026-09-27T08:55:00Z",
         "departure_at": "2026-09-27T08:55:00Z"}]},
      {"service_id": "BRAVO", "capacity": 100, "stops": [
        {"station_id": "B", "arrival_at": "2026-09-27T08:30:00Z",
         "departure_at": "2026-09-27T08:30:00Z"},
        {"station_id": "F", "arrival_at": "2026-09-27T09:00:00Z",
         "departure_at": "2026-09-27T09:00:00Z"}]}
    ],
    "groups": [
      {"group_id": "g1", "origin": "A", "destination": "F",
       "ready_at": "2026-09-27T08:00:00Z", "party_size": 1},
      {"group_id": "g2", "origin": "E", "destination": "A",
       "ready_at": "2026-09-27T09:00:00Z", "party_size": 4}
    ]
  }'
```

响应（乘客组按处理顺序 `(ready_at, group_id)` 返回；`legs` 为零表示滞留或起终点相同）：

```json
{
  "replan_id": "dr-2026-09-27-a",
  "status": "created",
  "min_transfer_seconds": 300,
  "summary": {"group_count": 2, "assigned_count": 1, "stranded_count": 1},
  "groups": [
    {
      "group_id": "g1",
      "origin": "A",
      "destination": "F",
      "ready_at": "2026-09-27T08:00:00Z",
      "party_size": 1,
      "status": "assigned",
      "arrival_at": "2026-09-27T09:00:00Z",
      "transfers": 1,
      "legs": [
        {"service_id": "ALPHA", "from_station": "A", "to_station": "B",
         "departure_at": "2026-09-27T08:05:00Z", "arrival_at": "2026-09-27T08:20:00Z"},
        {"service_id": "BRAVO", "from_station": "B", "to_station": "F",
         "departure_at": "2026-09-27T08:30:00Z", "arrival_at": "2026-09-27T09:00:00Z"}
      ],
      "stranded_reason": null
    },
    {
      "group_id": "g2",
      "origin": "E",
      "destination": "A",
      "ready_at": "2026-09-27T09:00:00Z",
      "party_size": 4,
      "status": "stranded",
      "arrival_at": null,
      "transfers": null,
      "legs": [],
      "stranded_reason": "unreachable_destination: no feasible itinerary reaches the destination from the origin"
    }
  ]
}
```

- **时刻校验**：每列班次的停靠站必须唯一（同一站不得重复停靠）；同站 `departure_at >= arrival_at`，且每站到站必须**严格晚于**上一站发时（端点相接合法、到发倒挂非法）。`service_id` / `group_id` 重复、`min_transfer_seconds < 0`、`party_size < 1`、班次少于两个停靠站、`capacity < 0` 等均返回 `422`；**校验错误一律不写库**。
- **幂等与冲突**：相同 `replan_id` + 相同载荷（班次 / 乘客组的数组顺序、时间偏移写法均不影响判定；班内站序是路线定义，始终保留）重复提交返回已固化结果（`status="duplicate"`，绝不重算、容量分配不变）；相同 `replan_id` + 不同载荷返回 `409`。
- **冻结与隔离**：结果在创建时一次性固化到独立的只追加表 `disruption_replans`（存储层触发器禁止 `UPDATE` / `DELETE`）。重排只依据请求载荷计算，**不读取也不改动**事件、行程、账单、争议 / 退款，也**不追加审计记录**。

### `GET /disruption/replans/{replan_id}` — 查询重排结果

返回重排创建时固化的完整结果（`status` 固定为 `"created"`；重放标记只在 `POST` 响应中出现）。不存在返回 `404`。

```bash
curl http://localhost:8000/disruption/replans/dr-2026-09-27-a
```

### `GET /config/fares` — 当前票价配置

## 业务审计链

每次**成功**的业务写入都会在同一 SQLite 事务内向只追加的 `audit_log` 表追加一条规范化审计记录，所有操作共享一条从 1 开始的全局序列：

- 刷卡事件首次处理成功 → `event_processed`；同 `event_id` 更正成功 → `event_corrected`；
- 对账批次创建成功 → `recon_batch_created`；
- 网络校验首次提交成功（无论是否通过）→ `network_validated`；网络快照固化成功 → `network_snapshot_created`；
- 争议创建成功（冻结收费快照）→ `dispute_opened`；争议结案成功（写入不可变退款流水）→ `dispute_resolved`；
- 运营方清分首次关账或追补批次创建成功（批次 + 不可变流水同事务提交）→ `clearing_closed`；
- 刷卡事件复核暂存创建成功（暂存 + 逐事件快照 + 事件标记 held + 重算同事务提交）→ `event_held`；暂存释放成功（释放结果只追加 + 事件解除 held + 重算同事务提交）→ `event_hold_released`；
- **重复请求不产生审计记录**：事件幂等重复（`duplicate`）、批次幂等重复（`duplicate`）、网络校验 / 快照幂等重复、争议 / 结案幂等重复（`duplicate`）、清分首账 / 追补幂等重复（`duplicate`）、暂存 / 释放幂等重复（`duplicate`）、冲突（`409`）与校验失败（`422`）均不追加；
- **只读分析不产生审计记录**：`POST /event-quality/scans` 只写其独立的只追加扫描表，`POST /privacy/exports` 只写其独立的只追加匿名导出表，`POST /disruption/replans` 只写其独立的只追加重排表；三者都不触碰 `audit_log`，也不改动事件、行程、账单或争议数据。更正影响预览 `POST /events/correction-preview` 连独立表也不写，纯粹只读模拟；
- 审计与业务数据同事务提交：业务写入（或审计写入）失败时一并回滚，不留半截记录。

每条记录包含：全局序号 `seq`、操作类型 `operation`、UTC 时间 `occurred_at`、规范化请求摘要 `request_summary`（时间归一化、记录按流水号排序）、规范化结果摘要 `result_summary`、上一条哈希 `prev_hash` 与当前哈希 `record_hash`。哈希规则为对上述字段（含 `prev_hash`）做规范 JSON 后取 SHA-256；首条记录的 `prev_hash` 为空串。表上带有触发器，禁止任何 `UPDATE` / `DELETE`，记录只追加、不可修改。

### `GET /audit` — 查询审计记录

可选查询参数：

- `operation`：按操作类型过滤（`event_processed` / `event_corrected` / `recon_batch_created` / `network_validated` / `network_snapshot_created` / `dispute_opened` / `dispute_resolved` / `clearing_closed` / `event_held` / `event_hold_released`）；
- `after`：只返回 `seq > after` 的记录（分页游标，默认 0）；
- `limit`：每页条数（默认 100，最大 1000）。

记录始终按 `seq` 升序返回。

```bash
curl "http://localhost:8000/audit?limit=2"
```

```json
{
  "count": 2,
  "entries": [
    {
      "seq": 1,
      "operation": "event_processed",
      "occurred_at": "2026-09-22T03:14:07.123456Z",
      "request_summary": {
        "event_id": "evt-001",
        "passenger_id": "p-1001",
        "station_id": "S01",
        "tap_type": "in",
        "occurred_at": "2026-09-21T08:30:00Z"
      },
      "result_summary": {
        "event_id": "evt-001",
        "status": "processed",
        "event_pairing": "pending",
        "pending_reason": "missing_tap_out",
        "adjustments": []
      },
      "prev_hash": "",
      "record_hash": "9f0c5a..."
    },
    {
      "seq": 2,
      "operation": "event_processed",
      "occurred_at": "2026-09-22T03:14:08.234567Z",
      "request_summary": { "...": "..." },
      "result_summary": { "...": "..." },
      "prev_hash": "9f0c5a...",
      "record_hash": "3b71e2..."
    }
  ]
}
```

### `GET /audit/verify` — 校验审计链

服务端从链头开始逐条重算哈希，检查序号连续、`prev_hash` 衔接以及内容与 `record_hash` 一致。链完好时返回链头序号与哈希；发现断裂时返回**首个断裂序号** `broken_seq` 与原因。

```bash
curl http://localhost:8000/audit/verify
```

完好：

```json
{
  "valid": true,
  "head_seq": 12,
  "head_hash": "0e23a298f3b4f72400f74ccf4560efc92e9c542abcec5c1251b5e33354929ecd",
  "broken_seq": null,
  "reason": "chain intact",
  "count": 12
}
```

被篡改或出现缺号时：

```json
{
  "valid": false,
  "head_seq": null,
  "head_hash": null,
  "broken_seq": 7,
  "reason": "record 7 content or hash has been tampered with: stored record_hash does not match the recomputed hash",
  "count": 11
}
```

审计链只追加、不参与任何业务判定；已冻结的对账批次语义不受影响。

## 在线备份

### `GET /admin/backup` — 在线导出一致性数据库副本

服务运行期间可随时调用，无需停服，也**不会**直接复制主库文件：

- 备份通过独立的**只读连接**使用 SQLite 在线备份机制（Online Backup API）逐页导出一致快照；整个过程**不持有应用写入锁**，因此备份进行期间 `POST /events` 刷卡写入仍可继续提交；
- 临时备份生成后，服务端再用另一条**独立连接**打开该副本并执行 `PRAGMA integrity_check` 与 `PRAGMA foreign_key_check`；只有完整性检查通过才会返回文件，同时在响应头 `X-Backup-SHA256` 中给出文件的 SHA-256 摘要（`X-Backup-Size-Bytes` 为字节数）；
- 返回的是标准 SQLite 数据库文件（`Content-Type: application/vnd.sqlite3`，文件名形如 `transit-backup-20260923T101530Z.db`），可直接用 `sqlite3` 打开；
- 备份失败（导出或完整性检查异常）时服务端会删除临时文件并返回 `500`，错误体中给出明确原因；成功返回的临时文件也会在响应发送完毕后自动清理；
- 备份是只读操作，不产生审计记录，也不影响任何业务数据。

#### 下载并校验（推荐保存方法）

用 `curl -D` 同时保存响应头与数据库文件，再用 `sha256sum` 比对摘要：

```bash
# -D headers.txt 保存响应头，-o 指定本地保存路径
curl -D headers.txt -o transit-backup.db http://localhost:8000/admin/backup

# 查看服务端返回的 SHA-256
grep -i '^X-Backup-SHA256' headers.txt

# 本地重新计算并比对（两者应一致）
sha256sum transit-backup.db
```

也可用一条命令同时下载并校验（需要响应头）：

```bash
curl -sS -D - -o transit-backup.db http://localhost:8000/admin/backup \
  | grep -i '^X-Backup-SHA256'
sha256sum transit-backup.db
```

保存后的副本可直接离线检查：

```bash
sqlite3 transit-backup.db "PRAGMA integrity_check;"   # 应输出 ok
```

Docker Compose 启动方式保持不变（`docker compose up --build`），备份文件通过 HTTP 从持久化卷中的数据库导出，无需进入容器或挂载额外路径。

## 配置

票价规则在 `config/fares.json` 中，可通过环境变量 `FARE_CONFIG` 指向其他文件；数据库路径通过 `DB_PATH` 配置。配置在启动时校验（分区票价矩阵必须覆盖所有出现的分区对）。

**匿名导出盐值 `PRIVACY_SALT`（必需）**：`POST /privacy/exports` 用它对 `passenger_id` 做稳定 HMAC 假名化。必须是仅服务器持有的高熵密钥（建议 `openssl rand -hex 32` 生成），并长期保持稳定——**更换盐值会轮换全部历史假名**，使同一乘客在新旧导出之间无法关联。未配置或为空时服务**拒绝启动**（避免在弱匿名配置下静默运行）。盐值只在服务端使用，绝不随请求或响应出现，也不会写入数据库。

`docker compose` 内置了一个仅用于本地开发的默认值，正式部署务必覆盖：

```bash
PRIVACY_SALT=$(openssl rand -hex 32) docker compose up --build
```

票价配置示例：

```json
{
  "currency": "CNY",
  "transfer_window_minutes": 90,
  "transfer_discount_cents": 100,
  "daily_cap_cents": 1500,
  "weekly_cap_cents": 5000,
  "stations": { "S01": 1, "S04": 2 },
  "zone_fares": { "1-1": 300, "1-2": 500 }
}
```

## 本地开发（不用 Docker）

`PRIVACY_SALT` 为必需环境变量，运行前先设置（开发期任意长随机串即可）：

```bash
pip install -r requirements.txt
export PRIVACY_SALT=$(openssl rand -hex 32)   # PowerShell: $env:PRIVACY_SALT="..."
uvicorn app.main:app --reload
```

运行冒烟测试：

```bash
python tests/smoke_test.py
```
