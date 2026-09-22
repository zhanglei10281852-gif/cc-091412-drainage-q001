"""静态台账：溢流口清单、泵闸能力、分区容量、污染阈值、预警等级、历史暴雨样例。

这些是业务基础数据，运行中不允许遥测改写；人工维护通过版本号管理。
"""

# 预警等级（数值越大越严重），含调度基调与响应时限
WARNING_LEVELS = {
    "蓝色": {"rank": 1, "label": "IV级（蓝色）", "response_deadline_minutes": 120,
            "policy": "加密监测，泵组热备，闸门维持常态"},
    "黄色": {"rank": 2, "label": "III级（黄色）", "response_deadline_minutes": 60,
            "policy": "污水泵提前抽空调蓄池，溢流闸门进入受控状态"},
    "橙色": {"rank": 3, "label": "II级（橙色）", "response_deadline_minutes": 30,
            "policy": "管网水位优先，错峰削峰，必要时有限溢流并同步拦截"},
    "红色": {"rank": 4, "label": "I级（红色）", "response_deadline_minutes": 15,
            "policy": "防洪优先，允许溢流但必须上报污染影响与责任人"},
}

# 河道防洪控制水位（米，吴淞高程）；超过后禁止向河排口新增溢流
RIVER_STAGES = {
    "梅溪河": {"warning_stage": 3.20, "guarantee_stage": 3.80, "top_bank": 4.30},
    "东塘港": {"warning_stage": 2.80, "guarantee_stage": 3.40, "top_bank": 3.90},
}

# 污染阈值：溢流浓度/负荷超过阈值必须在方案中显式标注并触发拦截措施
POLLUTION_THRESHOLDS = {
    "COD_mg_L": 50.0,
    "NH3N_mg_L": 5.0,
    "TP_mg_L": 0.5,
    # 单次溢流污染负荷（kg COD）达到该值必须上报监管
    "COD_load_kg_report": 200.0,
}

# 分区调蓄容量。district -> 调蓄池/管网可用调蓄量（立方米）及抽排能力
DISTRICTS = {
    "老城区": {
        "storage_m3": 12000.0,
        "pump_capacity_m3h": 9000.0,
        "treatment_inflow_m3h": 6500.0,  # 进厂处理能力（清洁/低污染水优先进厂）
        "outfalls": ["CSO-01", "CSO-02"],
        "river": "梅溪河",
    },
    "滨江新区": {
        "storage_m3": 18000.0,
        "pump_capacity_m3h": 14000.0,
        "treatment_inflow_m3h": 10000.0,
        "outfalls": ["CSO-03"],
        "river": "东塘港",
    },
}

# 溢流口清单与泵闸能力。gate_modes: OPEN(开闸溢流)/CLOSED(落闸截污)/REGULATED(受控开度)
# catchment_ha: 汇水面积；dry/alarm/critical_level_m: 管网晴雨天/警戒/危急液位
OUTFALLS = {
    "CSO-01": {
        "name": "梅溪路合流溢流口",
        "district": "老城区",
        "river": "梅溪河",
        "catchment_ha": 180.0,
        "dwf_m3h": 900.0,
        "dry_level_m": 1.10, "alarm_level_m": 3.60, "critical_level_m": 4.40,
        "runoff_coef": 0.80,
        "gate": {"gate_id": "G-01", "capable_modes": ["OPEN", "CLOSED", "REGULATED"],
                 "normal_mode": "CLOSED", "stroke_seconds": 90},
        "pump": {"pump_id": "P-01", "capacity_m3h": 4500.0, "min_run_m3h": 1200.0},
        "baseline_quality": {"COD_mg_L": 62.0, "NH3N_mg_L": 6.8, "TP_mg_L": 0.72},
        "water_supply_km": 0.0, "downstream_sensitive_km": 1.5,  # 下游取水口/敏感目标距离
    },
    "CSO-02": {
        "name": "南关厢合流溢流口",
        "district": "老城区",
        "river": "梅溪河",
        "catchment_ha": 150.0,
        "dwf_m3h": 780.0,
        "dry_level_m": 1.00, "alarm_level_m": 3.40, "critical_level_m": 4.20,
        "runoff_coef": 0.85,
        "gate": {"gate_id": "G-02", "capable_modes": ["OPEN", "CLOSED"],
                 "normal_mode": "CLOSED", "stroke_seconds": 60},
        "pump": {"pump_id": "P-02", "capacity_m3h": 4500.0, "min_run_m3h": 1200.0},
        "baseline_quality": {"COD_mg_L": 88.0, "NH3N_mg_L": 9.5, "TP_mg_L": 1.10},
        "water_supply_km": 0.0, "downstream_sensitive_km": 0.8,
    },
    "CSO-03": {
        "name": "滨江1号合流溢流口",
        "district": "滨江新区",
        "river": "东塘港",
        "catchment_ha": 260.0,
        "dwf_m3h": 1200.0,
        "dry_level_m": 1.20, "alarm_level_m": 3.80, "critical_level_m": 4.60,
        "runoff_coef": 0.70,
        "gate": {"gate_id": "G-03", "capable_modes": ["OPEN", "CLOSED", "REGULATED"],
                 "normal_mode": "CLOSED", "stroke_seconds": 120},
        "pump": {"pump_id": "P-03", "capacity_m3h": 7000.0, "min_run_m3h": 1800.0},
        "baseline_quality": {"COD_mg_L": 45.0, "NH3N_mg_L": 4.2, "TP_mg_L": 0.45},
        "water_supply_km": 2.0, "downstream_sensitive_km": 3.0,
    },
}

# 汛期值班责任人（缺省责任人；人工接管/现场确认后责任人随动作转移）
DUTY_OFFICERS = {
    "老城区": {"name": "周建国", "role": "调度员"},
    "滨江新区": {"name": "林岚", "role": "调度员"},
}

# 传感器台账：哪些站点为哪些溢流口/分区提供哪类量测
SENSORS = {
    "RAIN-LC": {"kind": "rain", "district": "老城区", "outfall": None, "stale_timeout_s": 600},
    "RAIN-BJ": {"kind": "rain", "district": "滨江新区", "outfall": None, "stale_timeout_s": 600},
    "LV-01": {"kind": "level", "district": "老城区", "outfall": "CSO-01", "stale_timeout_s": 300},
    "LV-02": {"kind": "level", "district": "老城区", "outfall": "CSO-02", "stale_timeout_s": 300},
    "LV-03": {"kind": "level", "district": "滨江新区", "outfall": "CSO-03", "stale_timeout_s": 300},
    "RS-MX": {"kind": "river_stage", "district": None, "outfall": None,
              "river": "梅溪河", "stale_timeout_s": 600},
    "RS-DT": {"kind": "river_stage", "district": None, "outfall": None,
              "river": "东塘港", "stale_timeout_s": 600},
    # 闸位/泵状态由 SCADA 网关上报，站点 id 即设备 id
    "G-01": {"kind": "gate", "district": "老城区", "outfall": "CSO-01", "stale_timeout_s": 300},
    "G-02": {"kind": "gate", "district": "老城区", "outfall": "CSO-02", "stale_timeout_s": 300},
    "G-03": {"kind": "gate", "district": "滨江新区", "outfall": "CSO-03", "stale_timeout_s": 300},
}

# 历史暴雨样例：用于方案回溯参照与降雨事件归并参数标定
# gap_minutes: 同一场降雨允许的最大雨停间隙（跨午夜归并的核心参数）
STORM_SAMPLES = [
    {"id": "STORM-20210725", "name": "2021-07-25 台风烟花",
     "start": "2021-07-25T19:40:00+08:00", "end": "2021-07-26T08:10:00+08:00",
     "rainfall_mm": 268.4, "peak_intensity_mmh": 78.0, "gap_minutes": 360,
     "outfalls_active": ["CSO-01", "CSO-02"], "peak_river_stage": 3.92,
     "note": "雨峰跨越 0 点，两场上报曾在旧系统被拆成两个事件"},
    {"id": "STORM-20230912", "name": "2023-09-12 台前飑线",
     "start": "2023-09-12T15:05:00+08:00", "end": "2023-09-12T21:30:00+08:00",
     "rainfall_mm": 96.0, "peak_intensity_mmh": 54.0, "gap_minutes": 240,
     "outfalls_active": ["CSO-03"], "peak_river_stage": 3.05,
     "note": "雨强与浓度关系标定样例"},
    {"id": "STORM-20250818", "name": "2025-08-18 持续性梅雨锋面",
     "start": "2025-08-18T22:20:00+08:00", "end": "2025-08-19T06:50:00+08:00",
     "rainfall_mm": 142.5, "peak_intensity_mmh": 41.0, "gap_minutes": 300,
     "outfalls_active": ["CSO-01", "CSO-02", "CSO-03"], "peak_river_stage": 3.46,
     "note": "长历时低强度，调蓄池抽排节奏参照"},
]

# 同场降雨归并参数：相邻雨量消息间隔不超过该值（分钟）即属于同一事件，
# 事件结束（gap 超时）后再降雨才另开事件。跨午夜不切分。
RAIN_MERGE_GAP_MINUTES = 360
# 液位估算入流的转换系数（量测驱动，保守缺省；可由人工复核覆盖）
LEVEL_TO_INFLOW = 4200.0  # m3/h per m over dry-weather level

CATALOG_VERSION = "2026-09-01"


def district_of_outfall(outfall_id: str) -> str:
    return OUTFALLS[outfall_id]["district"]


def outfalls_for_district(district: str):
    return list(DISTRICTS[district]["outfalls"])
