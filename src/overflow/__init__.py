"""合流制溢流调度后端。

模块划分：
- reference: 溢流口/泵闸/分区/阈值等静态台账与历史暴雨样例
- timeutil: 统一的带时区时间处理
- store: 落盘状态与只追加事件链
- ingest: 遥测去重、失联降级、冲突识别、降雨事件归并
- planning: 基于分区容量与河道水位的可解释调度计算
- service: 调度领域服务（方案/动作/接管/通知/恢复）
- web: HTTP 路由
"""

SERVICE_NAME = "cso-dispatch-service"
