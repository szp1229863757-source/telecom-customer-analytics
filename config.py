"""
运营商客户行为大数据分析系统 - 全局配置
Telecom Customer Behavior Big Data Analysis System

数据源: Telecom Customer Churn Case Study (Kaggle)
真实印度运营商预付费客户数据: 10万客户 × 226列 × 4个月(2014.06-09)
"""
import os

# ============================================
# 项目路径
# ============================================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
HIVE_DDL_DIR = os.path.join(PROJECT_ROOT, "hive_ddl")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================
# 数据源配置
# ============================================
DATA_CONFIG = {
    "csv_file": os.path.join(PROJECT_ROOT, "telecom_churn_data.csv"),
    "total_rows": 99999,
    "total_columns": 226,
    "months": [6, 7, 8, 9],                   # 对应 2014年6-9月
    "month_labels": {6: "June", 7: "July", 8: "August", 9: "September"},
    "customer_id_col": "mobile_number",
    "circle_id_col": "circle_id",
    "tenure_col": "aon",                       # Age on Network (天)
    # 核心指标分组
    "metric_groups": {
        "mou_incoming": ["total_ic_mou"],      # 来电分钟
        "mou_outgoing": ["total_og_mou"],      # 去电分钟
        "mou_by_type": ["loc_og_mou", "std_og_mou", "isd_og_mou",
                        "loc_ic_mou", "std_ic_mou", "isd_ic_mou"],
        "recharge": ["total_rech_num", "total_rech_amt", "max_rech_amt"],
        "data_usage": ["vol_2g_mb", "vol_3g_mb"],
        "arpu": ["arpu"],
        "roaming": ["roam_og_mou", "roam_ic_mou"],
        "pack_usage": ["monthly_2g", "monthly_3g", "sachet_2g", "sachet_3g",
                       "night_pck_user", "fb_user"],
    },
    # 数据仓库宽表转长表需要的列后缀
    "monthly_suffixes": ["_6", "_7", "_8", "_9"],
}

# ============================================
# 数据仓库分层表定义
# ============================================
DWH_LAYERS = {
    "ODS": {
        "table": "ods_telecom_raw",
        "description": "贴源层 - 原始宽表(226列)，按circle_id分区",
        "partition": "circle_id STRING",
        "stored_as": "PARQUET",
    },
    "DWD": {
        "tables": [
            {
                "table": "dwd_customer_monthly",
                "description": "明细层 - 宽表转长表：每客户每自然月一行",
                "partition": "report_month STRING",
                "granularity": "customer_id + month",
            },
        ],
    },
    "DWS": {
        "tables": [
            {
                "table": "dws_customer_monthly_kpi",
                "description": "汇总层 - 每客户每月综合KPI（总通话/总数据/总充值/ARPU/余额）",
                "partition": "report_month STRING",
                "granularity": "customer_id + month",
            },
            {
                "table": "dws_circle_monthly_kpi",
                "description": "汇总层 - 每个circle每月汇总：客户数/总营收/总通话量/平均ARPU/流失率",
                "partition": "report_month STRING",
                "granularity": "circle_id + month",
            },
        ],
    },
    "ADS": {
        "tables": [
            {
                "table": "ads_circle_performance_ranking",
                "description": "应用层 - circle(地区)业绩排名：按ARPU/通话/数据/充值综合打分",
                "partition": "report_month STRING",
            },
            {
                "table": "ads_usage_decline_alert",
                "description": "应用层 - 用量骤降告警：环比通话或数据下降>50%的客户",
                "partition": "report_month STRING",
            },
            {
                "table": "ads_silent_customer_report",
                "description": "应用层 - 沉默客户报告：当月无通话无数据的客户",
                "partition": "report_month STRING",
            },
        ],
    },
}

# ============================================
# KPI阈值
# ============================================
KPI_THRESHOLDS = {
    "arpu": {
        "high": 500.0,     # 高价值客户
        "medium": 200.0,   # 中等
        "low": 100.0,      # 低价值
    },
    "usage_decline_pct": 0.5,    # 用量下降超过50%算骤降
    "silent_threshold_mou": 0.0, # 零通话
    "silent_threshold_data": 0.0,# 零数据
    "churn_risk_days": 0,        # 当月无活跃(无通话/无数据)=流失高风险
}

# ============================================
# Spark 批处理参数
# ============================================
SPARK_CONFIG = {
    "app_name": "TelecomCustomerAnalytics",
    "executor_memory": "2g",
    "executor_cores": 2,
    "driver_memory": "1g",
    "shuffle_partitions": 50,
}

# ============================================
# Kafka 配置（模拟实时用户行为上报）
# ============================================
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topic_customer_events": "customer_usage_events",
    "topic_churn_alert": "churn_risk_alert",
    "batch_size": 1000,
    "linger_ms": 5,
}

# ============================================
# Flink 实时处理参数
# ============================================
FLINK_CONFIG = {
    "window_size_seconds": 300,
    "usage_decline_threshold": 0.5,       # 50% decline triggers alert
}
