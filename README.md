# 运营商客户行为大数据分析系统

**Telecom Customer Behavior Big Data Analysis System**

基于大数据技术栈(Hadoop/Spark/Hive/Kafka/Flink)的运营商客户行为分析平台。使用 **Kaggle 真实印度运营商预付费客户数据**(99,999客户 × 226特征 × 4个月),模拟BSS/计费系统数据采集、数仓分层存储、批量ETL、实时流处理与可视化全流程。

---

## 项目架构

```
                     ┌─────────────────────────────────────────┐
                     │      Customer Analytics Dashboard        │
                     │          (Plotly Interactive)            │
                     └────────────────┬────────────────────────┘
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        │                             │                             │
        ▼                             ▼                             ▼
┌───────────────┐          ┌───────────────────┐        ┌───────────────────┐
│  ADS 应用层    │          │   Kafka Alert      │        │                   │
│ Circle业绩排名 │          │   Topic            │◄───────┤  Flink Streaming  │
│ 用量骤降告警   │          │ churn_risk_alert   │        │  实时流失检测      │
│ 沉默客户报告   │          └───────────────────┘        │  5min滚动窗口      │
└───────┬───────┘                                        └─────────┬─────────┘
        │                                                          │
        ▼                                                          │
┌───────────────┐                                        ┌─────────┴─────────┐
│  DWS 汇总层    │                                        │   Kafka Raw       │
│ 客户月度KPI    │                                        │   Topic           │
│ Circle月度KPI  │                                        │ customer_usage    │
│               │                                        │ _events           │
└───────┬───────┘                                        │ (模拟BSS实时推送)   │
        │                                                └───────────────────┘
        ▼
┌───────────────┐
│  DWD 明细层    │
│ ★宽表→长表     │
│ (UNPIVOT核心)  │
│ 每客户每月一行  │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  ODS 贴源层    │
│ 原始CSV宽表    │
│ 226列×10万行   │
│ 按circle_id分区 │
└───────────────┘
```

## 技术栈

| 层次 | 技术 | 说明 |
|------|------|------|
| **数据源** | Kaggle Telecom Churn Dataset | 真实印度运营商预付费客户数据 |
| **数据存储** | HDFS + Hive | 数仓分层: ODS→DWD→DWS→ADS, Parquet+Snappy |
| **批量ETL** | PySpark (DataFrame API) | ★核心: 226列宽表→长表(UNPIVOT), KPI计算 |
| **实时处理** | Kafka + Flink | 模拟BSS实时推送 + 5min窗口流失风险检测 |
| **可视化** | Plotly + HTML | 交互式客户行为看板 |
| **数据格式** | Parquet (Snappy压缩) | 列存, 查询高效 |

## 项目结构

```
signal-quality-analysis/
├── config.py                  # 全局配置(数据参数/Kafka/Spark/Flink/KPI阈值)
├── spark_batch_etl.py         # PySpark批量ETL管道(ODS→DWD→DWS→ADS)
├── hive_ddl/
│   └── create_tables.sql      # Hive建表DDL(7张表, 4层架构)
├── kafka_producer.py          # Kafka生产者(模拟BSS实时推送客户事件)
├── flink_processor.py         # Flink实时流处理器(窗口聚合+流失告警)
├── visualization.py           # Plotly可视化看板
├── download_data.py           # Kaggle数据下载脚本
├── telecom_churn_data.csv     # 源数据(需下载, ~79MB, gitignored)
├── output/                    # ETL产出目录(gitignored)
└── README.md
```

## 快速开始

### 1. 下载数据

```bash
# 使用 Kaggle API 下载(需先配置 ~/.kaggle/kaggle.json)
python download_data.py

# 或手动从以下链接下载:
# https://www.kaggle.com/datasets/abhinavthomas/telecom-customer-churn
```
文件: `telecom_churn_data.csv` (99,999行 × 226列, ~79MB)

### 2. 运行批量ETL

```bash
pip install pyspark pandas
python spark_batch_etl.py
```

产出: `output/` 目录下 circle_ranking.csv / decline_alerts.csv / silent_customers.csv
控制台打印 Circle业绩Top5、完整ETL Pipeline总结

### 3. 实时处理演示

```bash
# 终端1: 启动生产者(模拟BSS实时推送)
pip install kafka-python
python kafka_producer.py

# 终端2: 查看Flink处理架构 + 本地模拟
python flink_processor.py
```

### 4. 生成可视化看板

```bash
pip install plotly
python visualization.py
# 打开 output/telecom_dashboard.html
```

## 数据仓库分层说明

| 层级 | 表名 | 粒度 | 核心加工 |
|------|------|------|---------|
| **ODS** | ods_telecom_raw | 原始宽表(226列) | 贴源存储, 按circle_id分区 |
| **DWD** | dwd_customer_monthly | 客户+月 | ★宽表→长表(UNPIVOT, 核心ETL) |
| **DWS** | dws_customer_monthly_kpi | 客户+月 | 综合KPI+行为标签(重度/高价值/漫游) |
| **DWS** | dws_circle_monthly_kpi | 地区+月 | 客户数/ARPU/通话/数据/流失率汇总 |
| **ADS** | ads_circle_performance_ranking | 地区+月 | 多维综合打分排名(ARPU/活跃/数据) |
| **ADS** | ads_usage_decline_alert | 客户+月 | 环比通话/流量下降>50%告警 |
| **ADS** | ads_silent_customer_report | 客户+月 | 当月零通话零流量客户+流失风险定级 |

## KPI指标体系

| 指标 | 计算方式 | 阈值 |
|------|---------|------|
| **高价值客户** | ARPU ≥ 500 | 重点关注, 沉默须优先回访 |
| **重度通话用户** | 月通话 > 500min | 核心收入来源 |
| **重度数据用户** | 月流量 > 1GB | 数据业务核心用户 |
| **用量骤降** | 环比下降 > 50% | 触发告警, 可能流失 |
| **沉默客户** | 月零通话且零流量 | 高风险流失, 按上月ARPU分级 |
| **Circle活跃率** | 活跃客户/总客户 | 地区健康度核心指标 |

## 面试可讲亮点

1. **数据仓库分层设计**: 解释ODS→DWD→DWS→ADS每层做什么, 为什么要分层
2. **宽表转长表(★核心)**: 226列 `{metric}_{month}` 宽表 → UNPIVOT为长表, 是真实数仓开发中最常见的ETL场景
3. **Spark ETL管道**: DataFrame API + Window函数(LAG环比/ROW_NUMBER排名) + 广播Join优化
4. **Kafka+Flink实时处理**: 5分钟窗口聚合 + 流失风险多级告警规则
5. **维度建模**: 星型模型, 事实表(客户月度行为) + 维度表(circle地区/时间)
6. **真实数据**: 使用Kaggle真实运营商数据而非模拟数据, 更有说服力
7. **完整数据链路**: 从CSV到可视化看板, 覆盖批处理+流处理双链路
8. **Parquet列存 + Snappy压缩**: 存储优化, 压缩比高且查询快

## 作者

胡中杰 — 北京邮电大学 信息与计算科学

*本项目为求职作品集项目, 展示大数据工程和数据分析能力。*
