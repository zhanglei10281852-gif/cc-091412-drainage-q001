# 合流制溢流（CSO）调度后端

面向台风/暴雨期间的排水调度：接收雨量、液位、河道水位、水质与闸门遥测和人工复核，
按分区调蓄容量、泵闸能力、污染阈值、预警等级与河道水位生成**可解释**调度方案，
并把每次方案生成、确认、拒绝、现场执行/拒绝、人工接管记录成哈希链接的事件链。

仅依赖 Python 3.11 标准库。

## 运行

```bash
python3 src/index.py            # 默认 0.0.0.0:8000，数据落 ./.runtime/data
DATA_DIR=/data python3 src/index.py
python3 -m unittest discover    # 测试（基线 + 31 个业务用例）
docker compose up --build
```

`DATA_DIR` 下保存两类文件：`events.jsonl`（只追加的事件链）与
`state.snapshot.json`（原子写入的状态快照）。删除该目录即回到空状态。

## 核心语义

- **重传幂等**：遥测报文必须带 `message_id`；相同 id 重传原样返回首次处理结果，
  不再归约，闸门开度不会被重复/篡改报文改变。
- **失联不补零**：传感器超过 `interval × 3` 未上报即降级为 `unconfirmed`，
  水质标志异常为 `suspect`，无历史帧为 `unknown`；缺测值一律是 `null`，
  绝不按 0 参与调度，相关闸门动作转 `needs_confirmation`。
- **跨午夜同一暴雨事件**：按相邻雨量帧间隔（默认 120 分钟雨峰间歇）归并，
  与自然日无关；间隔超时才另开事件。可用 `POST /api/samples/load` 装载
  `reference/cso_assets.json` 中的历史暴雨样例验证。
- **冲突不臆断**：闸门遥测开度、人工复核开度、系统下发指令两两比对，超容差即
  记录结构化冲突（双方数值、观测时间、复核人），保持现场现状待人工确认。
- **确认保护**：方案一旦确认，仍有未完成动作时，新方案生成返回
  `409 confirmed_plan_active`；必须显式 `supersede_active` + `reason`，
  旧方案与未完成动作被标记 `superseded/cancelled` 并留痕，不可静默覆盖。
- **重启可查**：服务重启后从快照+事件链重建，未完成动作、通知截止时间
  （橙 30 分钟/红与监管 15 分钟）、河道影响范围（河段、里程、溢流量、污染负荷）
  与人工接管状态均可查询；事件链可通过 `/api/audit/verify` 校验，篡改即报错。

## 调度决策

每个分区按以下顺序判定（全部理由写入动作的 `rationale`）：

1. 人工接管中 → 不自动动作，责任人=接管人；
2. 闸门来源冲突 / 液位不可信 / 河道水位未知 → 保持现状，待确认；
3. 河道回水（≥ 回水限值）→ 关闸防倒灌；若同时液位 ≥95% 满管 →
   在“内涝”与“倒灌污染”两种危害间升级人工，机器不取舍；
4. 未到内涝临界但来水可调蓄/泵抽 → 关闸（默认保护态）；
5. COD 超阈值且未到内涝临界 → 关闸保河道、泵组满负荷送厂；
6. 确需泄放 → 按超量水量与闸门能力反算开度，河道警戒水位折减 50%；
   超污染阈值时限时限量并要求监管先确认，同时给出河段影响范围与污染负荷。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/telemetry` | 雨量/液位/河道水位/水质/闸门遥测（必带 `message_id`） |
| POST | `/api/review` | 人工复核读数或闸门开度（遥测与复核并存以发现冲突） |
| POST | `/api/plans/generate?as_of=...` | 生成方案；已确认方案未完成时需 `supersede_active`+`reason` |
| POST | `/api/plans/{id}/confirm` `/reject` `/supersede` | 方案生命周期（带 actor 与原因） |
| POST | `/api/actions/{id}/feedback` | 现场回执 `executed`/`rejected`/`failed` |
| POST | `/api/takeovers`、`/api/takeovers/{scope}/{id}/release` | 人工接管/解除（zone 或 gate，可带 `until`） |
| POST | `/api/notifications/{id}/ack` | 通知签收 |
| GET | `/api/sensors` | 全部传感器/闸门工况（fresh/unconfirmed/suspect、冲突） |
| GET | `/api/actions/pending`、`/api/notifications` | 未完成动作、通知（含逾期标记） |
| GET | `/api/storms`、`/api/storms/{id}` | 降雨事件与帧序列 |
| GET | `/api/plans/latest`、`/api/impacts` | 方案与河道影响范围 |
| GET | `/api/management/overview` | 管理视图：方案采用读数、冲突来源、当前责任人 |
| GET | `/api/events?since=N`、`/api/audit/verify` | 事件链查询与完整性校验 |
| POST | `/api/samples/load` | 装载历史暴雨样例 |

所有时间字段使用带时区的 ISO 8601；写操作需 `actor: {id, name, role}`，
角色取自 `reference/domain.json`（调度员/运维人员/监管人员/只读用户）。
溢流口清单、泵闸能力、阈值、预警等级与样例位于 `reference/cso_assets.json`。
