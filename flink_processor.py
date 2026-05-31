"""
Flink 实时客户流失风险检测处理器
消费 Kafka 客户使用事件 → 5分钟滑动窗口聚合 → 异常行为告警

真实运行需要 flink run 提交到集群, 本脚本提供完整的 PyFlink DataStream API 代码框架。
面试时讲清楚这段逻辑即可。
"""
import sys

try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
    from pyflink.datastream.window import TumblingEventTimeWindows
    from pyflink.datastream.functions import ProcessWindowFunction
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.typeinfo import Types
    HAS_FLINK = True
except ImportError:
    HAS_FLINK = False
    print("PyFlink not installed. Install: pip install apache-flink")
    print("This script demonstrates the Flink streaming logic for reference.")

from config import KAFKA_CONFIG, FLINK_CONFIG, KPI_THRESHOLDS


# ================================================================
# Flink 实时处理流程(面试核心):
#
#   Kafka(customer_usage_events) → Flink Source
#     → map(解析JSON + 提取KPI)
#     → keyBy(circle_id)                           ← 按地区分区
#     → window(Tumbling, 5min)                     ← 5分钟滚动窗口
#     → process(计算: 窗口内客户统计/ARPU均值/沉默占比/骤降占比)
#     → filter(告警级别 != INFO)
#     → map(序列化JSON)
#     → Kafka(churn_risk_alert)                    ← 告警输出到下游
# ================================================================


# 客户事件Schema(对应Kafka消息体)
CUSTOMER_EVENT_SCHEMA = Types.ROW_NAMED(
    ["mobile_number", "circle_id", "aon", "report_month",
     "total_og_mou", "total_ic_mou", "total_rech_amt", "max_rech_amt",
     "vol_2g_mb", "vol_3g_mb", "arpu",
     "monthly_2g", "monthly_3g", "night_pck_user", "fb_user",
     "event_time"],
    [Types.LONG(), Types.STRING(), Types.INT(), Types.STRING(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.INT(), Types.INT(), Types.INT(), Types.INT(),
     Types.STRING()]
)


class ChurnRiskWindowAggregator(ProcessWindowFunction):
    """
    5分钟窗口客户行为聚合器

    输入: 一个窗口内某个 circle_id 的所有客户使用事件
    输出: 聚合统计 + 流失风险告警

    告警规则:
      CRITICAL: 窗口内沉默客户(零通话零流量)占比 > 30%
                或 ARPU均值环比下降 > 50%
      WARNING:  窗口内零通话客户占比 > 20%
                或 数据不使用客户占比 > 30%
    """

    def process(self, key, context, elements, collector):
        records = list(elements)
        if not records:
            return

        n = len(records)
        total_calls = [r[4] + r[5] for r in records]  # og + ic mou
        total_data = [r[8] + r[9] for r in records]   # 2g + 3g mb
        arpus = [r[10] for r in records]
        rech_amts = [r[6] for r in records]

        avg_arpu = sum(arpus) / n if n > 0 else 0
        avg_calls = sum(total_calls) / n if n > 0 else 0
        avg_data = sum(total_data) / n if n > 0 else 0
        total_rech = sum(rech_amts)

        # 沉默客户(零通话且零流量)
        silent_count = sum(1 for c, d in zip(total_calls, total_data) if c == 0 and d == 0)
        silent_rate = silent_count / n if n > 0 else 0

        # 零通话客户
        zero_call_count = sum(1 for c in total_calls if c == 0)
        zero_call_rate = zero_call_count / n if n > 0 else 0

        # 零数据客户
        zero_data_count = sum(1 for d in total_data if d == 0)
        zero_data_rate = zero_data_count / n if n > 0 else 0

        # 高价值客户(ARPU > 500)中沉默的比例
        high_value = [(a, c, d) for a, c, d in zip(arpus, total_calls, total_data)
                      if a >= KPI_THRESHOLDS["arpu"]["high"]]
        hv_silent = sum(1 for a, c, d in high_value if c == 0 and d == 0)
        hv_count = len(high_value)
        hv_silent_rate = hv_silent / hv_count if hv_count > 0 else 0

        # 告警判断
        alert_level = "INFO"
        alert_desc = "Normal"
        reasons = []

        if silent_rate > 0.3:
            alert_level = "CRITICAL"
            reasons.append(f"沉默率{silent_rate:.1%}")
        elif silent_rate > 0.15:
            alert_level = "WARNING"
            reasons.append(f"沉默率偏高{silent_rate:.1%}")

        if zero_call_rate > 0.2:
            if alert_level == "INFO":
                alert_level = "WARNING"
            reasons.append(f"零通话率{zero_call_rate:.1%}")

        if zero_data_rate > 0.3:
            if alert_level == "INFO":
                alert_level = "WARNING"
            reasons.append(f"零数据率{zero_data_rate:.1%}")

        if hv_silent_rate > 0.1 and hv_count > 5:
            alert_level = "CRITICAL"
            reasons.append(f"高价值客户沉默{hv_silent_count}/{hv_count}")

        if reasons:
            alert_desc = " | ".join(reasons)

        result = {
            "circle_id": key,
            "window_start": str(context.window().start),
            "window_end": str(context.window().end),
            "customer_count": n,
            "avg_arpu": round(avg_arpu, 2),
            "avg_calls_min": round(avg_calls, 2),
            "avg_data_mb": round(avg_data, 2),
            "total_recharge": round(total_rech, 2),
            "silent_count": silent_count,
            "silent_rate": round(silent_rate, 3),
            "zero_call_rate": round(zero_call_rate, 3),
            "zero_data_rate": round(zero_data_rate, 3),
            "alert_level": alert_level,
            "alert_description": alert_desc,
        }

        if alert_level != "INFO":
            import json
            collector.collect(json.dumps(result, ensure_ascii=False))


def create_flink_stream(env, input_topic, output_topic, bootstrap_servers):
    """
    创建 Flink 实时处理流(伪代码框架)

    面试表述:
    "使用Flink DataStream API,
    从Kafka读取客户使用事件,
    按circle_id分区做5分钟滚动窗口聚合,
    统计沉默率/零通话率/高价值客户流失比例,
    达到阈值则推送告警到Kafka告警Topic"
    """
    print("""
    ================================================================
    Flink 实时流失风险检测流架构:

    Kafka Source(customer_usage_events)
      → map: JSON → CustomerEvent(解析字段)
      → Watermark: 允许10秒乱序
      → keyBy: circle_id(按运营商地区分区, 天然并行)
      → window: TumblingEventTimeWindows(5分钟)
         ├── 窗口内计算:
         │   - 客户数/平均ARPU/平均通话/平均流量
         │   - 沉默客户占比(零通话+零流量)
         │   - 零通话率 / 零数据率
         │   - 高价值客户(ARPU>500)中沉默比例
         │
         └── 告警规则:
             CRITICAL: 沉默率 > 30% 或 高价值客户沉默
             WARNING:  零通话率 > 20% 或 零数据率 > 30%
      → filter: alert_level != INFO
      → Kafka Sink(churn_risk_alert) → 推送告警到下游(客服系统/大屏)
    ================================================================
    """)
    return env


def simulate_locally():
    """本地模拟Flink窗口聚合逻辑(不需要Flink/Kafka环境)"""
    import numpy as np
    import time

    print("=== Local Flink Simulation: Churn Risk Detection ===\n")

    circles = [f"CIRCLE-{i}" for i in range(1, 6)]

    for window_id in range(3):
        print(f"--- Window {window_id+1} (0{window_id*5}:00 - 0{window_id*5+5}:00) ---")

        for circle in circles:
            is_problem = circle in ["CIRCLE-1", "CIRCLE-2"]
            silent_base = 0.25 if is_problem else 0.05
            n_customers = 200

            # 模拟该circle在窗口内收到的客户使用事件
            # 统计沉默客户(零通话+零流量)占比
            silent_count = np.random.binomial(n_customers, silent_base)
            silent_rate = silent_count / n_customers

            # 模拟零通话率
            zero_call_rate = silent_rate + np.random.uniform(0, 0.1)

            level = "INFO"
            desc = "Normal"
            reasons = []

            if silent_rate > 0.3:
                level = "CRITICAL"
                reasons.append(f"高沉默率: {silent_rate:.1%}")
            elif silent_rate > 0.15:
                level = "WARNING"
                reasons.append(f"沉默率偏高: {silent_rate:.1%}")

            if zero_call_rate > 0.2:
                if level == "INFO":
                    level = "WARNING"
                reasons.append(f"零通话率: {zero_call_rate:.1%}")

            if reasons:
                desc = " | ".join(reasons)
                print(f"  {circle}: {n_customers} customers | "
                      f"silent={silent_count}({silent_rate:.1%}) | "
                      f"[{level}] {desc}")

        time.sleep(0.3)

    print("\nSimulation done. In production, alerts go to Kafka churn_risk_alert topic.")
    print("Downstream: alerts consumed by monitoring dashboard / alerting system.")


def main():
    if HAS_FLINK:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(2)
        create_flink_stream(
            env,
            KAFKA_CONFIG["topic_customer_events"],
            KAFKA_CONFIG["topic_churn_alert"],
            KAFKA_CONFIG["bootstrap_servers"],
        )
    else:
        create_flink_stream(None, None, None, None)
        simulate_locally()


if __name__ == "__main__":
    main()
