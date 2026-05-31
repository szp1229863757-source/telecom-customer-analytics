"""
运营商客户行为可视化看板
基于Plotly生成交互式图表: ARPU趋势/Circle对比/用户分布/流失风险
"""
import os
import numpy as np
import pandas as pd
from config import OUTPUT_DIR, KPI_THRESHOLDS, DATA_DIR, DATA_CONFIG

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False
    print("plotly not installed. Install: pip install plotly")
    print("This script will run in dry mode.")


def load_data():
    """加载ETL产出数据(或原始CSV做demo)"""
    ranking_path = os.path.join(OUTPUT_DIR, "circle_ranking.csv")
    csv_path = DATA_CONFIG["csv_file"]

    data = {}
    if os.path.exists(ranking_path):
        data["circle_ranking"] = pd.read_csv(ranking_path)

    # 从原始CSV采样做demo可视化
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)

        # 提取月度数据做长表(简化版, 只取关键指标)
        months = []
        for suffix, label in [("_6", "2014-06"), ("_7", "2014-07"),
                               ("_8", "2014-08"), ("_9", "2014-09")]:
            month_df = pd.DataFrame({
                "mobile_number": df["mobile_number"],
                "circle_id": df["circle_id"].astype(str),
                "report_month": label,
                "arpu": df.get(f"arpu{suffix}", 0),
                "total_og_mou": df.get(f"total_og_mou{suffix}", 0),
                "total_ic_mou": df.get(f"total_ic_mou{suffix}", 0),
                "total_rech_amt": df.get(f"total_rech_amt{suffix}", 0),
                "total_rech_num": df.get(f"total_rech_num{suffix}", 0),
                "vol_2g_mb": df.get(f"vol_2g_mb{suffix}", 0),
                "vol_3g_mb": df.get(f"vol_3g_mb{suffix}", 0),
            })
            month_df["total_calls"] = month_df["total_og_mou"] + month_df["total_ic_mou"]
            month_df["total_data"] = month_df["vol_2g_mb"] + month_df["vol_3g_mb"]
            months.append(month_df)

        data["long_df"] = pd.concat(months, ignore_index=True)

        # Circle月度汇总
        data["circle_monthly"] = data["long_df"].groupby(
            ["circle_id", "report_month"]
        ).agg(
            avg_arpu=("arpu", "mean"),
            total_customers=("mobile_number", "count"),
            avg_calls=("total_calls", "mean"),
            avg_data=("total_data", "mean"),
            total_revenue=("total_rech_amt", "sum"),
        ).reset_index()

        # 全量月度趋势
        data["monthly_trend"] = data["long_df"].groupby("report_month").agg(
            avg_arpu=("arpu", "mean"),
            total_revenue=("total_rech_amt", "sum"),
            active_users=("total_calls", lambda x: (x > 0).sum()),
            total_calls_avg=("total_calls", "mean"),
            total_data_avg=("total_data", "mean"),
        ).reset_index()

    return data


def create_dashboard(data, output_html=None):
    """
    生成四合一可视化看板:
    1. 月度ARPU&收入趋势
    2. Circle地区对比(ARPU气泡图)
    3. 用户通话vs数据分布散点
    4. Circle综合排名柱状图
    """
    if not HAS_PLOTLY:
        print("[Demo] Plotly not available, skipping charts.")
        return

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "<b>Monthly ARPU & Revenue Trend</b>",
            "<b>Circle Comparison</b>: ARPU vs Active Users",
            "<b>User Distribution</b>: Calls vs Data Usage",
            "<b>Circle Composite Score</b>: Performance Ranking",
        ),
        specs=[
            [{"secondary_y": True}, {"type": "scatter"}],
            [{"type": "scatter"}, {"type": "bar"}],
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )

    # --- Chart 1: 月度ARPU趋势 + 总收入 ---
    if "monthly_trend" in data:
        mt = data["monthly_trend"]
        fig.add_trace(
            go.Scatter(
                x=mt["report_month"], y=mt["avg_arpu"],
                mode="lines+markers",
                line=dict(color="#00205B", width=2.5),
                marker=dict(size=8),
                name="Avg ARPU",
            ),
            row=1, col=1, secondary_y=False,
        )
        fig.add_trace(
            go.Bar(
                x=mt["report_month"], y=mt["total_revenue"],
                name="Total Revenue",
                marker_color="rgba(37, 99, 235, 0.3)",
            ),
            row=1, col=1, secondary_y=True,
        )
        fig.update_yaxes(title_text="ARPU (INR)", row=1, col=1, secondary_y=False)
        fig.update_yaxes(title_text="Total Revenue (INR)", row=1, col=1, secondary_y=True)
        fig.update_xaxes(title_text="Month", row=1, col=1)

    # --- Chart 2: Circle对比气泡图 ---
    if "circle_monthly" in data:
        cm = data["circle_monthly"]
        latest = cm[cm["report_month"] == cm["report_month"].max()]
        fig.add_trace(
            go.Scatter(
                x=latest["avg_arpu"], y=latest["active_rate"] if "active_rate" in latest.columns else latest["avg_calls"],
                mode="markers+text",
                marker=dict(
                    size=latest["total_customers"] / latest["total_customers"].max() * 50,
                    color=latest["avg_arpu"],
                    colorscale="Blues",
                    showscale=True,
                    colorbar=dict(title="ARPU", x=1.02, y=0.7, len=0.35),
                ),
                text=latest["circle_id"],
                textposition="top center",
                name="Circles",
                hovertemplate="Circle %{text}<br>ARPU: %{x:.1f}<br>Users: %{marker.size:.0f}<extra></extra>",
            ),
            row=1, col=2,
        )
        fig.update_xaxes(title_text="Avg ARPU (INR)", row=1, col=2)
        fig.update_yaxes(title_text="Avg Calls (min)", row=1, col=2)

    # --- Chart 3: 用户通话 vs 数据分布 ---
    if "long_df" in data:
        sample = data["long_df"].sample(min(5000, len(data["long_df"])), random_state=42)
        colors = sample["circle_id"].astype(str)
        fig.add_trace(
            go.Scatter(
                x=sample["total_calls"].clip(upper=2000),
                y=sample["total_data"].clip(upper=5000),
                mode="markers",
                marker=dict(
                    size=4, opacity=0.4,
                    color=sample["arpu"],
                    colorscale="RdYlGn",
                    cmin=0, cmax=800,
                    colorbar=dict(title="ARPU", x=1.02, y=0.15, len=0.35),
                ),
                text=[f"Circle {c}<br>Calls: {v:.0f}min<br>Data: {d:.0f}MB<br>ARPU: {a:.1f}"
                      for c, v, d, a in zip(sample["circle_id"], sample["total_calls"],
                                            sample["total_data"], sample["arpu"])],
                hovertemplate="%{text}",
                name="Users",
            ),
            row=2, col=1,
        )
        fig.add_vline(x=500, line_dash="dash", line_color="gray",
                      opacity=0.5, row=2, col=1,
                      annotation_text="Heavy Caller(500min)", annotation_position="top")
        fig.update_xaxes(title_text="Total Calls (min)", row=2, col=1)
        fig.update_yaxes(title_text="Total Data (MB)", row=2, col=1)

    # --- Chart 4: Circle综合排名 ---
    if "circle_ranking" in data:
        cr = data["circle_ranking"]
        latest_rank = cr[cr["report_month"] == cr["report_month"].max()]
        if len(latest_rank) > 0:
            latest_rank = latest_rank.sort_values("composite_score", ascending=True)
            fig.add_trace(
                go.Bar(
                    y=latest_rank["circle_id"].astype(str),
                    x=latest_rank["composite_score"],
                    orientation="h",
                    marker=dict(
                        color=latest_rank["composite_score"],
                        colorscale="Blues",
                        showscale=False,
                    ),
                    text=latest_rank["composite_score"].round(1),
                    textposition="auto",
                    name="Score",
                ),
                row=2, col=2,
            )
            fig.update_xaxes(title_text="Composite Score", row=2, col=2)
            fig.update_yaxes(title_text="Circle ID", row=2, col=2)

    # --- 布局 ---
    fig.update_layout(
        height=900,
        template="plotly_white",
        showlegend=True,
        title={
            "text": "<b>Telecom Customer Behavior Analytics Dashboard</b><br>"
                    "<sub>Data: India Prepaid Telecom  |  "
                    "99,999 Customers × 226 Features × 4 Months (Jun-Sep 2014)  |  "
                    "ETL: PySpark  |  Realtime: Kafka+Flink</sub>",
            "x": 0.5,
            "xanchor": "center",
        },
        margin=dict(l=40, r=60, t=80, b=40),
    )

    if output_html is None:
        output_html = os.path.join(OUTPUT_DIR, "telecom_dashboard.html")

    fig.write_html(output_html, full_html=True, include_plotlyjs="cdn")
    print(f"Dashboard saved to: {output_html}")
    return fig


def main():
    print("Loading data for visualization...")
    data = load_data()
    if data:
        create_dashboard(data)
    else:
        print("No data found. Run spark_batch_etl.py first to generate output.")
        print(f"Or ensure {DATA_CONFIG['csv_file']} exists.")


if __name__ == "__main__":
    main()
