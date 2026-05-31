"""
运营商客户实时流失检测处理器（模拟 Flink DataStream）
====================================================
消费 stream_producer.py 生成的 events.jsonl，
按 circle_id 分区 + 5 分钟滚动窗口聚合，
检测异常行为并输出分级告警。

可用性: 无需 Flink/PyFlink，纯 Python + pandas + numpy。
真实 Flink 版本见 flink_processor.py（需 PyFlink + Flink 集群）。

用法:
    # 先启动 producer（另一个终端）
    python stream_producer.py --speed 1000

    # 再启动 processor
    python stream_processor.py
    python stream_processor.py --window-size 300  # 5 分钟窗口（秒）
"""
import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime

import numpy as np

from config import OUTPUT_DIR, KPI_THRESHOLDS

# ── 处理配置 ──────────────────────────────────────────────────
DEFAULT_WINDOW_SECONDS = 300   # 5 分钟滚动窗口
POLL_INTERVAL = 0.5            # 每 0.5 秒检查一次新事件
ALERT_LEVELS = ["INFO", "WARNING", "CRITICAL"]


class TumblingWindowAggregator:
    """
    滚动窗口聚合器 —— 模拟 Flink TumblingEventTimeWindows(5 min)

    算法:
    1. 将事件按 event_time 分配到对应窗口
    2. 窗口关闭后，对该窗口内每个 circle 的所有事件进行聚合
    3. 计算各项风险指标并触发告警
    """

    def __init__(self, window_seconds=DEFAULT_WINDOW_SECONDS):
        self.window_ms = window_seconds * 1000
        self.buffer = defaultdict(list)     # window_key -> [events]
        self.processed_windows = set()

    @staticmethod
    def get_window_key(event_time_str):
        """将事件时间映射到所属窗口（向下取整到 window 边界）"""
        t = datetime.fromisoformat(event_time_str)
        epoch_ms = int(t.timestamp() * 1000)
        return epoch_ms  # 在外层做取整

    def add_event(self, event):
        """将事件加入缓冲区"""
        t = datetime.fromisoformat(event["event_time"])
        epoch_ms = int(t.timestamp() * 1000)
        window_key = (epoch_ms // self.window_ms) * self.window_ms
        self.buffer[window_key].append(event)

    def get_closed_windows(self, current_time_str):
        """返回所有已关闭（当前时间已超出窗口范围）的窗口数据"""
        t = datetime.fromisoformat(current_time_str)
        current_ms = int(t.timestamp() * 1000)
        closed = {}

        for wk in sorted(self.buffer.keys()):
            # 窗口关闭条件: current_time > window_end
            if wk + self.window_ms < current_ms:
                if wk not in self.processed_windows:
                    closed[wk] = self.buffer[wk]
                    self.processed_windows.add(wk)

        return closed

    def pending_count(self):
        return len(self.buffer)


class ChurnRiskDetector:
    """
    流失风险检测引擎 —— 模拟 Flink ProcessWindowFunction

    告警规则:
      CRITICAL: 窗口内沉默率 > 30% 或 高价值客户沉默
      WARNING:  零通话率 > 20% 或 零数据率 > 30%
    """

    def __init__(self):
        self.alerts = []
        self.stats_history = []

    def analyze_window(self, window_key, events):
        """对单个窗口内的所有事件进行聚合分析和告警判定"""
        if not events:
            return None

        # 按 circle 分区
        circle_events = defaultdict(list)
        for e in events:
            circle_events[e["circle_id"]].append(e)

        window_alerts = []
        window_start = datetime.fromtimestamp(window_key / 1000.0)
        window_end = datetime.fromtimestamp((window_key + 300000) / 1000.0)

        for circle_id, circle_evts in circle_events.items():
            result = self._analyze_circle(
                circle_id, circle_evts,
                window_start.isoformat(), window_end.isoformat()
            )
            if result and result["alert_level"] != "INFO":
                window_alerts.append(result)

        # 窗口汇总统计
        stats = {
            "window": window_start.isoformat(),
            "total_events": len(events),
            "circles_analyzed": len(circle_events),
            "alerts_triggered": len(window_alerts),
            "by_level": {
                level: len([a for a in window_alerts if a["alert_level"] == level])
                for level in ALERT_LEVELS
            },
        }
        self.stats_history.append(stats)

        return window_alerts

    def _analyze_circle(self, circle_id, events, window_start, window_end):
        """分析单个 circle 在窗口内的风险"""
        n = len(events)
        if n == 0:
            return None

        total_calls = [e["total_calls"] for e in events]
        total_data = [e["total_data_mb"] for e in events]
        arpus = [e["arpu"] for e in events]

        avg_calls = np.mean(total_calls)
        avg_data = np.mean(total_data)
        avg_arpu = np.mean(arpus)

        # 沉默客户（零通话 且 零流量）
        silent_count = sum(1 for c, d in zip(total_calls, total_data)
                          if c == 0 and d == 0)
        silent_rate = silent_count / n

        # 零通话率
        zero_call_count = sum(1 for c in total_calls if c == 0)
        zero_call_rate = zero_call_count / n

        # 零数据率
        zero_data_count = sum(1 for d in total_data if d == 0)
        zero_data_rate = zero_data_count / n

        # 高价值客户（ARPU > 500）中沉默的比例
        high_value = [(a, c, d) for a, c, d in zip(arpus, total_calls, total_data)
                      if a >= KPI_THRESHOLDS["arpu"]["high"]]
        hv_silent = sum(1 for a, c, d in high_value if c == 0 and d == 0)
        hv_count = len(high_value)
        hv_silent_rate = hv_silent / hv_count if hv_count > 0 else 0

        # ── 告警判定 ──
        alert_level = "INFO"
        reasons = []

        if silent_rate > 0.3:
            alert_level = "CRITICAL"
            reasons.append(f"高沉默率: {silent_rate:.1%}")
        elif silent_rate > 0.15:
            alert_level = "WARNING"
            reasons.append(f"沉默率偏高: {silent_rate:.1%}")

        if zero_call_rate > 0.2:
            if alert_level == "INFO":
                alert_level = "WARNING"
            reasons.append(f"零通话率: {zero_call_rate:.1%}")

        if zero_data_rate > 0.3:
            if alert_level == "INFO":
                alert_level = "WARNING"
            reasons.append(f"零数据率: {zero_data_rate:.1%}")

        if hv_silent_rate > 0.1 and hv_count > 5:
            alert_level = "CRITICAL"
            reasons.append(f"高价值客户沉默: {hv_silent}/{hv_count}")

        return {
            "circle_id": circle_id,
            "window_start": window_start,
            "window_end": window_end,
            "customer_count": n,
            "avg_calls": round(avg_calls, 2),
            "avg_data_mb": round(avg_data, 2),
            "avg_arpu": round(avg_arpu, 2),
            "silent_count": silent_count,
            "silent_rate": round(silent_rate, 3),
            "zero_call_rate": round(zero_call_rate, 3),
            "zero_data_rate": round(zero_data_rate, 3),
            "alert_level": alert_level,
            "alert_reason": " | ".join(reasons) if reasons else "Normal",
        }


def monitor_stream(events_path, window_seconds=DEFAULT_WINDOW_SECONDS,
                   max_idle_seconds=10):
    """
    监控 events.jsonl 文件，模拟实时流处理。

    工作方式:
    1. 每 0.5 秒检查文件是否有新行
    2. 将新事件加入窗口缓冲区
    3. 窗口关闭后执行聚合 + 告警检测
    4. 告警写入 alerts.jsonl

    Args:
        events_path: events.jsonl 文件路径
        window_seconds: 窗口大小（秒）
        max_idle_seconds: 无新事件最大等待秒数，超时强制处理
    """
    print(f"\n{'='*55}")
    print(f"[Processor] Starting real-time churn detection ...")
    print(f"[Processor]   Source: {events_path}")
    print(f"[Processor]   Window: {window_seconds}s tumbling")
    print(f"[Processor]   Output: {os.path.join(OUTPUT_DIR, 'alerts.jsonl')}")
    print(f"{'='*55}\n")

    aggregator = TumblingWindowAggregator(window_seconds)
    detector = ChurnRiskDetector()
    alerts_path = os.path.join(OUTPUT_DIR, "alerts.jsonl")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 清空旧的告警文件
    open(alerts_path, "w").close()

    last_line = 0
    idle_start = time.time()
    total_events = 0
    total_alerts = 0

    try:
        while True:
            # 读取新行
            with open(events_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            new_lines = lines[last_line:]
            last_line = len(lines)

            if new_lines:
                idle_start = time.time()
                for line in new_lines:
                    event = json.loads(line.strip())
                    aggregator.add_event(event)
                    total_events += 1

                # 处理已关闭的窗口
                now = datetime.now().isoformat()
                closed = aggregator.get_closed_windows(now)

                for wk, evts in closed.items():
                    window_alerts = detector.analyze_window(wk, evts)

                    if window_alerts:
                        with open(alerts_path, "a", encoding="utf-8") as af:
                            for alert in window_alerts:
                                af.write(json.dumps(alert, ensure_ascii=False) + "\n")
                                total_alerts += 1

                        # 打印告警摘要
                        for a in window_alerts[:3]:  # 最多显示 3 条
                            icon = "🔴" if a["alert_level"] == "CRITICAL" else "🟡"
                            print(f"  {icon} [{a['alert_level']:9s}] "
                                  f"Circle={a['circle_id']} | "
                                  f"customers={a['customer_count']} | "
                                  f"silent={a['silent_rate']:.1%} | "
                                  f"{a['alert_reason']}")

                    # 清理已处理的缓冲区
                    del aggregator.buffer[wk]

                # 打印窗口统计
                if detector.stats_history:
                    last_stat = detector.stats_history[-1]
                    print(f"  ✓ Window complete | events={last_stat['total_events']} "
                          f"| circles={last_stat['circles_analyzed']} "
                          f"| alerts={last_stat['alerts_triggered']} "
                          f"(C:{last_stat['by_level']['CRITICAL']} "
                          f"W:{last_stat['by_level']['WARNING']})")

            else:
                # 无新事件，检查是否超时
                if time.time() - idle_start > max_idle_seconds:
                    print(f"\n[Processor] No new events for {max_idle_seconds}s, "
                          f"assuming stream ended.")
                    break

                time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n[Processor] Interrupted by user.")

    # ── 流结束后，强制处理所有剩余窗口 ──
    remaining = sorted(aggregator.buffer.keys())
    if remaining:
        print(f"\n[Processor] Stream ended. Processing {len(remaining)} "
              f"remaining window(s) ...")
        for wk in remaining:
            evts = aggregator.buffer[wk]
            window_alerts = detector.analyze_window(wk, evts)
            if window_alerts:
                with open(alerts_path, "a", encoding="utf-8") as af:
                    for alert in window_alerts:
                        af.write(json.dumps(alert, ensure_ascii=False) + "\n")
                        total_alerts += 1
                for a in window_alerts[:5]:
                    icon = "CRIT" if a["alert_level"] == "CRITICAL" else "WARN"
                    print(f"  [{icon}] Circle={a['circle_id']} | "
                          f"silent={a['silent_rate']:.1%} | "
                          f"zero_call={a['zero_call_rate']:.1%} | "
                          f"{a['alert_reason']}")

    # ── 最终报告 ──
    print(f"\n{'='*55}")
    print(f"[Processor] Stream processing complete!")
    print(f"[Processor]   Total events processed: {total_events:,}")
    print(f"[Processor]   Total alerts generated: {total_alerts}")
    print(f"[Processor]   Windows analyzed:        "
          f"{len(detector.stats_history)}")
    print(f"[Processor]   Alerts saved to:         {alerts_path}")
    print(f"{'='*55}")

    # 按告警级别汇总
    level_counts = defaultdict(int)
    for stat in detector.stats_history:
        for level, count in stat["by_level"].items():
            level_counts[level] += count
    print(f"\n  Alert Summary:")
    print(f"    CRITICAL: {level_counts['CRITICAL']}")
    print(f"    WARNING:  {level_counts['WARNING']}")
    print(f"    INFO:     {level_counts['INFO']}")


def main():
    parser = argparse.ArgumentParser(
        description="实时流失风险检测处理器"
    )
    parser.add_argument(
        "--events", type=str,
        default=os.path.join(OUTPUT_DIR, "events.jsonl"),
        help="事件文件路径（stream_producer.py 的输出）"
    )
    parser.add_argument(
        "--window-size", type=int, default=DEFAULT_WINDOW_SECONDS,
        help="滚动窗口大小（秒），默认 300（5 分钟）"
    )
    parser.add_argument(
        "--max-idle", type=int, default=10,
        help="最大空闲秒数，超时自动结束（默认 10）"
    )
    args = parser.parse_args()

    if not os.path.exists(args.events):
        print(f"Error: Events file not found: {args.events}")
        print("Please run:  python stream_producer.py  first.")
        return

    monitor_stream(args.events, args.window_size, args.max_idle)


if __name__ == "__main__":
    main()
