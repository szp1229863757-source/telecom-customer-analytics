"""
Kafka 客户行为事件生产者
模拟从CSV读取客户月度行为数据, 逐批发送到Kafka Topic

场景: 模拟运营商计费系统/BSS实时上报客户使用事件
      每条消息 = 一个客户在某个月的使用记录
"""
import json
import time
import os
import random
from datetime import datetime
from config import KAFKA_CONFIG, DATA_CONFIG, DATA_DIR

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    print("kafka-python not installed. Install: pip install kafka-python")
    print("Running in DRY RUN mode (no actual Kafka connection)")

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


class CustomerEventProducer:
    """
    模拟运营商BSS系统实时推送客户使用事件

    工作原理:
    1. 从telecom_churn_data.csv读取客户数据
    2. 将每条记录转为JSON(包含当月通话/流量/充值/ARPU等字段)
    3. 发送到Kafka Topic: customer_usage_events
    4. 下游Flink消费该Topic做实时流失风险检测

    使用方式:
        producer = CustomerEventProducer()
        producer.send_batch(batch_size=100, interval_seconds=0.5)
    """

    def __init__(self, csv_path=None, bootstrap_servers=None):
        self.topic = KAFKA_CONFIG["topic_customer_events"]
        self.alert_topic = KAFKA_CONFIG["topic_churn_alert"]
        self.bootstrap_servers = bootstrap_servers or KAFKA_CONFIG["bootstrap_servers"]
        self.csv_path = csv_path or DATA_CONFIG["csv_file"]

        if HAS_PANDAS:
            print(f"Loading customer data from {self.csv_path}...")
            self.df = pd.read_csv(self.csv_path)
            self.total_rows = len(self.df)
            self.current_idx = 0
            print(f"Loaded {self.total_rows:,} customers, {len(self.df.columns)} features")
        else:
            self.df = None
            self.total_rows = 100000
            self.current_idx = 0

        if HAS_KAFKA:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all',
                retries=3,
                linger_ms=KAFKA_CONFIG["linger_ms"],
                batch_size=KAFKA_CONFIG["batch_size"] * 1024,
                compression_type='gzip',
            )
            print(f"Kafka Producer connected to {self.bootstrap_servers}")
        else:
            self.producer = None

    def _build_customer_event(self, row, month_suffix, month_label):
        """将CSV一行 + 月份 → Kafka消息(模拟该月使用事件)"""
        suffix = month_suffix  # e.g. "_7"
        return {
            "mobile_number": int(row["mobile_number"]),
            "circle_id": str(row.get("circle_id", "")),
            "aon": int(row.get("aon", 0)),
            "report_month": month_label,
            # 通话指标
            "total_og_mou": float(row.get(f"total_og_mou{suffix}", 0) or 0),
            "total_ic_mou": float(row.get(f"total_ic_mou{suffix}", 0) or 0),
            "loc_og_mou": float(row.get(f"loc_og_mou{suffix}", 0) or 0),
            "std_og_mou": float(row.get(f"std_og_mou{suffix}", 0) or 0),
            "isd_og_mou": float(row.get(f"isd_og_mou{suffix}", 0) or 0),
            "roam_og_mou": float(row.get(f"roam_og_mou{suffix}", 0) or 0),
            "roam_ic_mou": float(row.get(f"roam_ic_mou{suffix}", 0) or 0),
            # 充值指标
            "total_rech_num": float(row.get(f"total_rech_num{suffix}", 0) or 0),
            "total_rech_amt": float(row.get(f"total_rech_amt{suffix}", 0) or 0),
            "max_rech_amt": float(row.get(f"max_rech_amt{suffix}", 0) or 0),
            # 数据用量
            "vol_2g_mb": float(row.get(f"vol_2g_mb{suffix}", 0) or 0),
            "vol_3g_mb": float(row.get(f"vol_3g_mb{suffix}", 0) or 0),
            # ARPU
            "arpu": float(row.get(f"arpu{suffix}", 0) or 0),
            # 套餐
            "monthly_2g": int(row.get(f"monthly_2g{suffix}", 0) or 0),
            "monthly_3g": int(row.get(f"monthly_3g{suffix}", 0) or 0),
            "night_pck_user": int(row.get(f"night_pck_user{suffix}", 0) or 0),
            "fb_user": int(row.get(f"fb_user{suffix}", 0) or 0),
            # 元数据
            "event_time": datetime.now().isoformat(),
        }

    def send_batch(self, batch_size=100, interval_seconds=0.5, month_filter=None):
        """批量发送客户事件, 模拟BSS实时推送

        Args:
            batch_size: 每批发送条数
            interval_seconds: 批次间隔(秒)
            month_filter: 仅发送指定月份(None=全部), 如 ["_7", "_8"]
        """
        suffixes = month_filter or ["_6", "_7", "_8", "_9"]
        month_labels = {"_6": "2014-06", "_7": "2014-07", "_8": "2014-08", "_9": "2014-09"}

        total_sent = 0
        start_time = time.time()

        print(f"\n{'='*50}")
        print(f"Starting customer event production...")
        print(f"Batch size: {batch_size}, Interval: {interval_seconds}s")
        print(f"Topic: {self.topic}")
        print(f"Months: {[month_labels[s] for s in suffixes]}")
        print(f"{'='*50}\n")

        try:
            while self.current_idx < self.total_rows:
                end_idx = min(self.current_idx + batch_size, self.total_rows)
                batch = self.df.iloc[self.current_idx:end_idx]

                for _, row in batch.iterrows():
                    for suffix in suffixes:
                        msg = self._build_customer_event(row, suffix, month_labels[suffix])
                        key = f"{row['mobile_number']}_{month_labels[suffix]}"

                        if self.producer:
                            future = self.producer.send(
                                self.topic, key=key, value=msg
                            )
                            try:
                                future.get(timeout=0.1)
                            except Exception:
                                pass
                        else:
                            if total_sent < 3:
                                print(f"  [DRY RUN] key={key} | "
                                      f"calls={msg['total_og_mou']:.0f}min | "
                                      f"data={msg['vol_2g_mb']+msg['vol_3g_mb']:.0f}MB | "
                                      f"ARPU={msg['arpu']:.1f}")

                self.current_idx = end_idx
                total_sent += len(batch) * len(suffixes)

                if total_sent % (batch_size * len(suffixes) * 10) == 0:
                    elapsed = time.time() - start_time
                    rate = total_sent / elapsed if elapsed > 0 else 0
                    pct = self.current_idx / self.total_rows * 100
                    print(f"  Progress: {total_sent:,} events | "
                          f"Customers: {self.current_idx:,}/{self.total_rows:,} "
                          f"({pct:.1f}%) | Rate: {rate:.0f} msg/s")

                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                print("Kafka Producer closed.")
            elapsed = time.time() - start_time
            print(f"\nTotal: {total_sent:,} messages in {elapsed:.1f}s "
                  f"({total_sent/elapsed:.0f} msg/s)")


def main():
    producer = CustomerEventProducer()
    # 发送7-8月数据(7月作为历史, 8月作为当前月, 可检测环比下降)
    producer.send_batch(batch_size=200, interval_seconds=0.1, month_filter=["_7", "_8"])


if __name__ == "__main__":
    main()
