# 合流制溢流（CSO）调度后端

面向台风/暴雨场景的合流制溢流调度服务：接收雨量、液位、河道水位、闸门遥测与人工复核，
按分区调蓄容量与河道水位生成**可解释**的调度方案，并把每次执行、拒绝、人工接管记录成事件链。

## 运行

需要 Python 3.11+，无第三方依赖。

```bash
python src/index.py                 # 默认 8000 端口
DRAINAGE_DATA_DIR=/var/lib/cso PORT=8000 python src/index.py
python -m unittest discover -s tests
docker compose up --build
```

数据落盘位置由 `DRAINAGE_DATA_DIR`（默认 `.runtime/data`）指定：
`state.json`（原子替换）+ `events.jsonl`（只追加事件链）。服务重启后未完成的动作、
通知截止时间、河道影响范围、幂等去重表全部从磁盘恢复。

## 核心规则

- **幂等**：同一 `message_id` 遥测重传只回执不重复改变闸门状态；同 `message_id` 不同内容判链路异常；
  动作执行回执按 `request_id` 幂等。
- **失联降级**：传感器超时/自检异常降级为 `STALE/BAD`（待确认），任何计算不得把缺测当零值；
  人工复核读数（`/api/reviews`）可恢复该口的调度依据。
- **跨午夜同事件**：降雨按雨停间隙（默认 360 分钟）归并，与是否跨过 0 点无关；
  事件关闭时标记 `cross_midnight`。
- **不可静默覆盖**：方案经现场确认（confirm）后，重新生成被拒绝，只能显式 `supersede`（留痕原因与授权人）。
- **闸位冲突**：遥测闸位与调度指令/人工复核不一致即登记冲突（含来源消息），
  该口闸门动作挂起，冲突消解前不得自动改闸。
- **人工接管**：接管后系统不再自动改该口闸门，非接管人的执行回执被拒绝并记入事件链。

## 主要接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/telemetry` | 遥测入库（幂等/失联判定） |
| POST | `/api/reviews` | 人工复核读数/闸位 |
| POST | `/api/districts/{分区}/plans` | 生成可解释方案（读数来源/规则码/阈值校核） |
| POST | `/api/plans/{id}/confirm` | 现场确认：派发动作、通知、影响范围 |
| POST | `/api/plans/{id}/supersede` | 显式取代已确认方案 |
| POST | `/api/commands/{id}/execute` `/reject` | 动作执行/拒绝回执 |
| GET | `/api/actions/pending` | 重启后仍需完成的动作 |
| POST/DELETE | `/api/outfalls/{id}/takeover` | 人工接管/解除 |
| GET | `/api/conflicts`、`/api/notifications`、`/api/impacts` | 冲突、通知截止、河道影响范围 |
| GET | `/api/overview` | 管理总览：方案采用的读数、冲突来源、当前责任人 |
| GET | `/api/plans/{id}/chain`、`/api/events` | 事件链回放 |
| GET | `/api/reference` | 溢流口清单、泵闸能力、污染阈值、预警等级、历史暴雨样例 |

静态台账（溢流口/泵闸/分区容量/污染阈值/预警等级/历史暴雨样例）见 `src/overflow/reference.py`。
