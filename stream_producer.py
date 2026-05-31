"""
运营商客户事件流式生成器（模拟 Kafka Producer）
==============================================
从 CSV 读取客户月度数据，按时间顺序逐条生成客户使用事件，
写入本地 JSON Lines 文件，模拟运营商 BSS 系统实时上报。

可用性: 无需 Kafka 集群，纯 Python + pandas 即可运行。
真实 Kafka 版本见 kafka_producer.py（需 kafka-python + broker）。

用法:
    python stream_producer.py
    python stream_producer.py --speed 1000   # 每秒 1000 条，默认 500
    python stream_producer.py --months 7,8   # 只生成 7-8 月数据
"""
import argparse
import json
import os
import random
import time
from datetime import datetime

import numpy as np
import pandas as pd

from config import DATA_CONFIG, OUTPUT_DIR

# ── 事件生成配置 ──────────────────────────────────────────────
EVENT_INTERVAL_MS = 2       # 每条事件间隔（毫秒），500 条/秒
BATCH_REPORT_EVERY = 5000   # 每 N 条打印一次进度
ANOMALY_PROBABILITY = 0.03  # 3% 概率注入异常事件（模拟真实波动）


class StreamingEventProducer:
    """
    模拟运营商 BSS 计费系统实时推送客户使用事件。

    原理:
    1. 从 CSV 读取每个客户 × 每个月的 KPI
    2. 按时间和 circle 排序后逐条生成 JSON 事件
    3. 写入 events.jsonl（每行一个 JSON，可直接 tail -f 查看）
    4. 下游 stream_processor.py 消费该文件做窗口聚合

    输出文件: data/events.jsonl
    """

    def __init__(self, csv_path=None):
        self.csv_path = csv_path or DATA_CONFIG["csv_file"]
        self.month_suffixes = DATA_CONFIG["monthly_suffixes"]
        self.month_labels = {
            "_6": "2014-06", "_7": "2014-07",
            "_8": "2014-08", "_9": "2014-09",
        }
        self.output_path = os.path.join(OUTPUT_DIR, "events.jsonl")
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def load_and_prepare(self):
        """加载 CSV 并转为长表（每客户每月一行）"""
        print(f"[Producer] Loading {self.csv_path} ...")
        df = pd.read_csv(self.csv_path)
        print(f"[Producer] Loaded {len(df):,} customers, {len(df.columns)} features")

        # 提取每月的核心指标，构建长表
        rows = []
        for suffix, month_label in self.month_labels.items():
            for _, row in df.iterrows():
                total_og = float(row.get(f"total_og_mou{suffix}", 0) or 0)
                total_ic = float(row.get(f"total_ic_mou{suffix}", 0) or 0)
                data_2g = float(row.get(f"vol_2g_mb{suffix}", 0) or 0)
                data_3g = float(row.get(f"vol_3g_mb{suffix}", 0) or 0)
                arpu_val = float(row.get(f"arpu{suffix}", 0) or 0)
                rech_amt = float(row.get(f"total_rech_amt{suffix}", 0) or 0)

                rows.append({
                    "mobile_number": int(row["mobile_number"]),
                    "circle_id": str(row.get("circle_id", "")),
                    "aon": int(row.get("aon", 0)),
                    "report_month": month_label,
                    "total_og_mou": total_og,
                    "total_ic_mou": total_ic,
                    "total_calls": total_og + total_ic,
                    "total_data_mb": data_2g + data_3g,
                    "arpu": arpu_val,
                    "total_rech_amt": rech_amt,
                })

        self.events = pd.DataFrame(rows)
        # 按 circle 和时间排序，模拟真实上报顺序
        self.events = self.events.sort_values(
            ["report_month", "circle_id"]
        ).reset_index(drop=True)
        print(f"[Producer] Prepared {len(self.events):,} events "
              f"({len(self.events) // 4:,} customers × 4 months)")

    def inject_anomaly(self, event):
        """以一定概率注入异常：通话/流量骤降 50-90%，模拟真实流失信号"""
        if random.random() < ANOMALY_PROBABILITY:
            decline = random.uniform(0.5, 0.9)
            event["total_calls"] = max(0, event["total_calls"] * (1 - decline))
            event["total_data_mb"] = max(0, event["total_data_mb"] * (1 - decline))
            event["arpu"] = max(0, event["arpu"] * (1 - decline))
            event["_anomaly"] = True
        else:
            event["_anomaly"] = False
        return event

    def produce(self, months=None, speed=500):
        """
        生成事件流并写入 events.jsonl。

        Args:
            months: 要生成的月份列表，如 ["_7", "_8"]，None = 全部
            speed: 每秒生成事件数
        """
        suffixes = months or self.month_suffixes
        filtered = self.events[
            self.events["report_month"].isin(
                [self.month_labels[s] for s in suffixes]
            )
        ]

        total = len(filtered)
        interval = 1.0 / speed
        print(f"\n{'='*55}")
        print(f"[Producer] Starting event stream...")
        print(f"[Producer]   Events: {total:,}")
        print(f"[Producer]   Speed:  {speed} events/sec")
        print(f"[Producer]   Output: {self.output_path}")
        print(f"{'='*55}\n")

        start_time = time.time()
        sent = 0

        with open(self.output_path, "w", encoding="utf-8") as f:
            for _, row in filtered.iterrows():
                event = {
                    "mobile_number": int(row["mobile_number"]),
                    "circle_id": str(row["circle_id"]),
                    "report_month": str(row["report_month"]),
                    "total_calls": round(float(row["total_calls"]), 2),
                    "total_data_mb": round(float(row["total_data_mb"]), 2),
                    "arpu": round(float(row["arpu"]), 2),
                    "total_rech_amt": round(float(row["total_rech_amt"]), 2),
                    "event_time": datetime.now().isoformat(),
                }

                # 注入异常
                event = self.inject_anomaly(event)

                f.write(json.dumps(event, ensure_ascii=False) + "\n")
                sent += 1

                # 进度报告
                if sent % BATCH_REPORT_EVERY == 0:
                    elapsed = time.time() - start_time
                    rate = sent / elapsed if elapsed > 0 else 0
                    pct = sent / total * 100
                    print(f"  [{sent:,}/{total:,} | {pct:.1f}% | "
                          f"{rate:.0f} eps]")

                time.sleep(interval)

        elapsed = time.time() - start_time
        print(f"\n[Producer] Done! {sent:,} events in {elapsed:.1f}s "
              f"({sent/elapsed:.0f} eps)")
        print(f"[Producer] Output: {self.output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="模拟运营商 BSS 实时事件推送"
    )
    parser.add_argument(
        "--speed", type=int, default=500,
        help="每秒生成事件数（默认 500）"
    )
    parser.add_argument(
        "--months", type=str, default=None,
        help="要生成的月份后缀，逗号分隔，如 '7,8'（默认全部 4 个月）"
    )
    parser.add_argument(
        "--csv", type=str, default=None,
        help="CSV 数据文件路径"
    )
    args = parser.parse_args()

    csv_path = args.csv or DATA_CONFIG["csv_file"]
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        print("Please run:  python download_data.py  first.")
        return

    months = None
    if args.months:
        months = [f"_{m.strip()}" for m in args.months.split(",")]

    producer = StreamingEventProducer(csv_path)
    producer.load_and_prepare()
    producer.produce(months=months, speed=args.speed)


if __name__ == "__main__":
    main()
