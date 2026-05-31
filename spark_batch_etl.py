"""
运营商客户行为大数据分析 - PySpark 批量ETL管道
核心: 226列宽表 → 长表(UNPIVOT) → DWS汇总 → ADS应用报表

数据源: Telecom Customer Churn Case Study (Kaggle)
99,999 印度预付费客户 × 226列 × 4个月(2014.06-09)

宽表结构: {metric}_{month} 如 arpu_6, total_og_mou_7, vol_3g_mb_8
          → 转换为长表: 每客户每月一行
"""
import os
import sys
from config import DATA_DIR, OUTPUT_DIR, SPARK_CONFIG, KPI_THRESHOLDS, DATA_CONFIG

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, avg, max, min, stddev, count, sum as spark_sum,
        lit, when, round as spark_round, row_number, desc,
        coalesce, lag, expr, broadcast, collect_list, concat_ws,
        greatest, least, abs as spark_abs
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType
except ImportError:
    print("PySpark not installed. Install via: pip install pyspark")
    print("This script demonstrates the ETL logic; use local mode for testing.")
    sys.exit(0)


class TelecomCustomerETL:
    """运营商客户行为数据 ETL 管道
    流程: CSV宽表 → DWD长表 → DWS客户/Circle汇总 → ADS排名/告警/沉默
    """

    def __init__(self, input_csv=None):
        self.spark = SparkSession.builder \
            .appName(SPARK_CONFIG["app_name"]) \
            .master("local[*]") \
            .config("spark.executor.memory", SPARK_CONFIG["executor_memory"]) \
            .config("spark.driver.memory", SPARK_CONFIG["driver_memory"]) \
            .config("spark.sql.shuffle.partitions", SPARK_CONFIG["shuffle_partitions"]) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        self.input_csv = input_csv or DATA_CONFIG["csv_file"]
        self.month_suffixes = DATA_CONFIG["monthly_suffixes"]
        self.month_map = {
            "_6": "2014-06", "_7": "2014-07",
            "_8": "2014-08", "_9": "2014-09"
        }

    # ================================================================
    # Step 1: ODS - 加载原始CSV宽表
    # ================================================================
    def load_raw_data(self):
        print(f"[ODS] Loading raw wide table from {self.input_csv}...")
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(self.input_csv)
        print(f"[ODS] Loaded {df.count():,} rows, {len(df.columns)} columns")
        return df

    # ================================================================
    # Step 2: DWD - 宽表转长表(核心UNPIVOT)
    # ================================================================
    def wide_to_long(self, wide_df):
        """
        将 226 列宽表转换为长格式: 每客户每月一行

        原理: 原始列如 arpu_6, arpu_7, arpu_8, arpu_9
              → 对每个月份后缀, 提取该月的所有指标列, 去除后缀作为列名
              → 4个月份的DataFrame UNION在一起
              → 得到 (customer_id, month, arpu, total_og_mou, ...) 长表

        这是整个ETL最核心的一步, 也是面试中最值得讲的点
        """
        print("[DWD] Converting wide table to long format...")
        print(f"      Source: {len(wide_df.columns)} columns (wide)")
        print(f"      Target: 1 row per customer per month (long)")

        # 定义需要UNPIVOT的指标组(去掉_6/_7/_8/_9后缀后的列名)
        # 这些是完整的指标列表, 自动根据实际CSV列名提取
        metric_bases = self._extract_metric_bases(wide_df.columns)

        # 非月度列(每个客户只有一个值, 不随月份变化)
        static_cols = ["mobile_number", "circle_id", "aon"]

        month_dfs = []
        for suffix, month_label in self.month_map.items():
            # 构建该月的SELECT表达式列表
            select_cols = [col(c) for c in static_cols if c in wide_df.columns]

            for base in metric_bases:
                wide_col_name = base + suffix
                if wide_col_name in wide_df.columns:
                    select_cols.append(
                        col(wide_col_name).alias(base)
                    )

            # 添加月份列
            select_cols.append(lit(month_label).alias("report_month"))

            month_df = wide_df.select(*select_cols)
            month_dfs.append(month_df)
            print(f"      Month {month_label}: {len(month_df.columns)} columns extracted")

        # UNION 四个月份的数据
        long_df = month_dfs[0]
        for i in range(1, len(month_dfs)):
            long_df = long_df.unionByName(month_dfs[i])

        # 计算派生字段
        long_df = long_df \
            .withColumn("total_data_mb",
                coalesce(col("vol_2g_mb"), lit(0)) + coalesce(col("vol_3g_mb"), lit(0))) \
            .withColumn("is_active_call",
                when(coalesce(col("total_og_mou"), lit(0)) + coalesce(col("total_ic_mou"), lit(0)) > 0, 1).otherwise(0)) \
            .withColumn("is_active_data",
                when(col("total_data_mb") > 0, 1).otherwise(0)) \
            .withColumn("is_active",
                when((col("is_active_call") == 1) | (col("is_active_data") == 1), 1).otherwise(0))

        result_count = long_df.count()
        print(f"[DWD] Long format: {result_count:,} rows "
              f"(expected: ~{99999 * 4:,})")
        return long_df

    def _extract_metric_bases(self, all_columns):
        """从宽表列名中提取指标基础名(去掉 _6/_7/_8/_9 后缀后去重)"""
        bases = set()
        skip_prefixes = {"last_date_of_month"}  # 日期列不需要unpivot

        for c in all_columns:
            for suffix in self.month_suffixes:
                if c.endswith(suffix):
                    base = c[:-len(suffix)]
                    if base not in skip_prefixes:
                        bases.add(base)
                    break
        return sorted(bases)

    # ================================================================
    # Step 3: DWS - 客户月度综合KPI
    # ================================================================
    def aggregate_customer_kpi(self, dwd_df):
        print("[DWS] Computing customer monthly KPIs...")

        kpi_df = dwd_df \
            .withColumn("total_calls_mou",
                coalesce(col("total_og_mou"), lit(0)) + coalesce(col("total_ic_mou"), lit(0))) \
            .withColumn("avg_rech_amt",
                when(coalesce(col("total_rech_num"), lit(0)) > 0,
                     col("total_rech_amt") / col("total_rech_num")).otherwise(0)) \
            .withColumn("is_heavy_caller",
                when(col("total_calls_mou") > 500, 1).otherwise(0)) \
            .withColumn("is_heavy_data_user",
                when(col("total_data_mb") > 1024, 1).otherwise(0)) \
            .withColumn("is_high_value",
                when(col("arpu") >= KPI_THRESHOLDS["arpu"]["high"], 1).otherwise(0)) \
            .withColumn("is_roamer",
                when(coalesce(col("roam_og_mou"), lit(0)) + coalesce(col("roam_ic_mou"), lit(0)) > 0, 1).otherwise(0)) \
            .withColumn("is_data_user",
                when(col("total_data_mb") > 0, 1).otherwise(0)) \
            .withColumn("is_2g_user",
                when(coalesce(col("vol_2g_mb"), lit(0)) > 0, 1).otherwise(0)) \
            .withColumn("is_3g_user",
                when(coalesce(col("vol_3g_mb"), lit(0)) > 0, 1).otherwise(0)) \
            .select(
                "mobile_number", "circle_id", "aon", "report_month",
                "total_calls_mou", "total_rech_amt", "total_data_mb",
                "arpu", "avg_rech_amt",
                "is_heavy_caller", "is_heavy_data_user", "is_high_value",
                "is_roamer", "is_data_user", "is_2g_user", "is_3g_user"
            )

        print(f"[DWS] Customer KPI: {kpi_df.count():,} rows")
        return kpi_df

    # ================================================================
    # Step 4: DWS - Circle月度汇总
    # ================================================================
    def aggregate_circle_kpi(self, dwd_df):
        print("[DWS] Computing circle monthly KPIs...")

        # 基于DWD明细计算, 保证聚合灵活性
        circle_df = dwd_df.groupBy("circle_id", "report_month").agg(
            count("mobile_number").alias("total_customers"),
            spark_sum("is_active").alias("active_customers"),
            avg("aon").alias("avg_tenure_days"),
            spark_sum(coalesce(col("total_og_mou"), lit(0))).alias("total_og_mou"),
            spark_sum(coalesce(col("total_ic_mou"), lit(0))).alias("total_ic_mou"),
            spark_sum(coalesce(col("total_rech_amt"), lit(0))).alias("total_recharge_amt"),
            avg("arpu").alias("avg_arpu"),
            spark_sum(coalesce(col("vol_2g_mb"), lit(0))).alias("total_2g_mb"),
            spark_sum(coalesce(col("vol_3g_mb"), lit(0))).alias("total_3g_mb"),
        )

        circle_df = circle_df \
            .withColumn("active_rate",
                spark_round(col("active_customers") / col("total_customers"), 4)) \
            .withColumn("avg_mou_per_user",
                spark_round((col("total_og_mou") + col("total_ic_mou")) / col("total_customers"), 2)) \
            .withColumn("avg_rech_per_user",
                spark_round(col("total_recharge_amt") / col("total_customers"), 2)) \
            .withColumn("avg_data_mb_per_user",
                spark_round((col("total_2g_mb") + col("total_3g_mb")) / col("total_customers"), 2)) \
            .withColumn("inactive_customers",
                col("total_customers") - col("active_customers")) \
            .withColumn("inactive_rate",
                spark_round(col("inactive_customers") / col("total_customers"), 4))

        # 补充需要明细才有的聚合字段
        detail_aggs = dwd_df.groupBy("circle_id", "report_month").agg(
            spark_sum(when(
                coalesce(col("total_og_mou"), lit(0)) + coalesce(col("total_ic_mou"), lit(0)) > 500, 1
            ).otherwise(0)).alias("heavy_caller_count"),
            spark_sum(when(col("total_data_mb") > 0, 1).otherwise(0)).alias("data_user_count"),
            spark_sum(when(col("arpu") >= KPI_THRESHOLDS["arpu"]["high"], 1).otherwise(0)).alias("high_value_count"),
            spark_sum(when(
                coalesce(col("roam_og_mou"), lit(0)) + coalesce(col("roam_ic_mou"), lit(0)) > 0, 1
            ).otherwise(0)).alias("roamer_count"),
            spark_sum(when(col("is_active_call") == 0, 1).otherwise(0)).alias("zero_call_customers"),
            spark_sum(when(col("is_active_data") == 0, 1).otherwise(0)).alias("zero_data_customers"),
        )

        circle_df = circle_df.join(detail_aggs, ["circle_id", "report_month"], "left") \
            .withColumn("heavy_caller_pct",
                spark_round(col("heavy_caller_count") / col("total_customers"), 4)) \
            .withColumn("data_user_pct",
                spark_round(col("data_user_count") / col("total_customers"), 4)) \
            .withColumn("high_value_pct",
                spark_round(col("high_value_count") / col("total_customers"), 4)) \
            .withColumn("roamer_pct",
                spark_round(col("roamer_count") / col("total_customers"), 4))

        # 选最终列
        circle_df = circle_df.select(
            "circle_id", "report_month",
            "total_customers", "active_customers", "active_rate", "avg_tenure_days",
            "total_og_mou", "total_ic_mou", "avg_mou_per_user",
            "total_recharge_amt", "avg_arpu", "avg_rech_per_user",
            "total_2g_mb", "total_3g_mb", "avg_data_mb_per_user",
            "heavy_caller_pct", "data_user_pct", "high_value_pct", "roamer_pct",
            "inactive_customers", "inactive_rate",
            "zero_call_customers", "zero_data_customers",
        )

        print(f"[DWS] Circle KPI: {circle_df.count():,} rows")
        return circle_df

    # ================================================================
    # Step 5: ADS - Circle业绩排名
    # ================================================================
    def generate_circle_ranking(self, circle_kpi_df):
        print("[ADS] Generating circle performance ranking...")

        # 计算各维度的标准化得分(Min-Max归一化 × 100)
        def calc_score(col_name):
            c = col(col_name)
            w = Window.partitionBy("report_month")
            min_val = min(c).over(w)
            max_val = max(c).over(w)
            return when(max_val == min_val, lit(50.0)).otherwise(
                (c - min_val) / (max_val - min_val) * 100)

        ranking = circle_kpi_df \
            .withColumn("arpu_score", calc_score("avg_arpu")) \
            .withColumn("activity_score", calc_score("active_rate")) \
            .withColumn("data_score", calc_score("avg_data_mb_per_user")) \
            .withColumn("composite_score",
                spark_round(
                    col("arpu_score") * 0.4 +
                    col("activity_score") * 0.35 +
                    col("data_score") * 0.25, 2
                ))

        # 排名
        window_spec = Window.partitionBy("report_month").orderBy(desc("composite_score"))
        ranking = ranking \
            .withColumn("rank", row_number().over(window_spec)) \
            .select(
                "circle_id", "report_month", "total_customers",
                "avg_arpu", "avg_mou_per_user", "avg_data_mb_per_user",
                "active_rate", "inactive_rate",
                "arpu_score", "activity_score", "data_score",
                "composite_score", "rank"
            ) \
            .orderBy("report_month", "rank")

        print(f"[ADS] Circle ranking: {ranking.count()} rows")
        return ranking

    # ================================================================
    # Step 6: ADS - 用量骤降告警(环比下降>50%)
    # ================================================================
    def generate_usage_decline_alert(self, dwd_df):
        print("[ADS] Generating usage decline alerts...")

        # 按客户+月份排序, 用LAG取上月数据
        window_spec = Window.partitionBy("mobile_number").orderBy("report_month")

        with_lag = dwd_df \
            .withColumn("prev_og_mou", lag("total_og_mou").over(window_spec)) \
            .withColumn("prev_ic_mou", lag("total_ic_mou").over(window_spec)) \
            .withColumn("prev_data_mb", lag("total_data_mb").over(window_spec)) \
            .withColumn("prev_rech_amt", lag("total_rech_amt").over(window_spec)) \
            .withColumn("prev_month", lag("report_month").over(window_spec))

        alerts = with_lag \
            .filter(col("prev_og_mou").isNotNull()) \
            .withColumn("curr_calls_mou",
                coalesce(col("total_og_mou"), lit(0)) + coalesce(col("total_ic_mou"), lit(0))) \
            .withColumn("prev_calls_mou",
                coalesce(col("prev_og_mou"), lit(0)) + coalesce(col("prev_ic_mou"), lit(0))) \
            .withColumn("calls_decline_pct",
                when(col("prev_calls_mou") > 0,
                     spark_round(1.0 - col("curr_calls_mou") / col("prev_calls_mou"), 4))
                .otherwise(0.0)) \
            .withColumn("data_decline_pct",
                when(col("prev_data_mb") > 0,
                     spark_round(1.0 - col("total_data_mb") / col("prev_data_mb"), 4))
                .otherwise(0.0)) \
            .withColumn("recharge_decline_pct",
                when(col("prev_rech_amt") > 0,
                     spark_round(1.0 - col("total_rech_amt") / col("prev_rech_amt"), 4))
                .otherwise(0.0))

        # 筛选满足条件的告警: 通话或流量下降 > 50%
        threshold = KPI_THRESHOLDS["usage_decline_pct"]
        alerts = alerts.filter(
            (col("calls_decline_pct") > threshold) | (col("data_decline_pct") > threshold)
        )

        # 告警分级
        alerts = alerts \
            .withColumn("alert_level",
                when((col("calls_decline_pct") > 0.8) | (col("data_decline_pct") > 0.8), "CRITICAL")
                .otherwise("WARNING")) \
            .withColumn("alert_reason",
                when(col("calls_decline_pct") > threshold,
                     concat_ws("", lit("通话下降"), spark_round(col("calls_decline_pct") * 100, 1), lit("%")))
                .otherwise(
                    concat_ws("", lit("流量下降"), spark_round(col("data_decline_pct") * 100, 1), lit("%"))))

        alerts = alerts.select(
            "mobile_number", "circle_id", "report_month", "prev_month",
            col("curr_calls_mou"), col("total_data_mb").alias("curr_data_mb"),
            col("total_rech_amt").alias("curr_rech_amt"),
            "prev_calls_mou", "prev_data_mb", "prev_rech_amt",
            "calls_decline_pct", "data_decline_pct", "recharge_decline_pct",
            "alert_level", "alert_reason"
        ).orderBy(desc("calls_decline_pct"))

        print(f"[ADS] Usage decline alerts: {alerts.count()} rows")
        return alerts

    # ================================================================
    # Step 7: ADS - 沉默客户报告(当月零通话零流量)
    # ================================================================
    def generate_silent_customer_report(self, dwd_df):
        print("[ADS] Generating silent customer report...")

        # 筛选本月沉默客户
        silent = dwd_df.filter(
            (col("is_active_call") == 0) & (col("is_active_data") == 0)
        )

        # 关联上月数据来评估流失风险
        window_spec = Window.partitionBy("mobile_number").orderBy("report_month")
        with_prev = dwd_df \
            .withColumn("prev_arpu", lag("arpu").over(window_spec)) \
            .withColumn("prev_og_mou", lag("total_og_mou").over(window_spec)) \
            .withColumn("prev_ic_mou", lag("total_ic_mou").over(window_spec)) \
            .withColumn("prev_data", lag("total_data_mb").over(window_spec)) \
            .withColumn("prev_rech", lag("total_rech_amt").over(window_spec))

        # JOIN回沉默客户
        silent_enriched = silent.join(
            with_prev.select("mobile_number", "report_month",
                             "prev_arpu", "prev_og_mou", "prev_ic_mou",
                             "prev_data", "prev_rech"),
            ["mobile_number", "report_month"], "left"
        )

        silent_enriched = silent_enriched \
            .withColumn("prev_total_calls",
                coalesce(col("prev_og_mou"), lit(0)) + coalesce(col("prev_ic_mou"), lit(0))) \
            .withColumn("risk_level",
                when(col("prev_arpu") >= KPI_THRESHOLDS["arpu"]["high"], "HIGH")
                .when(col("prev_arpu") >= KPI_THRESHOLDS["arpu"]["medium"], "MEDIUM")
                .otherwise("LOW")) \
            .withColumn("recommendation",
                when(col("prev_arpu") >= KPI_THRESHOLDS["arpu"]["high"],
                     "高价值客户沉默, 建议客服48小时内回访")
                .when(col("prev_arpu") >= KPI_THRESHOLDS["arpu"]["medium"],
                     "中等价值客户, 建议推送优惠活动召回")
                .otherwise("低价值客户, 建议短信营销触达"))

        silent_enriched = silent_enriched.select(
            "mobile_number", "circle_id", "aon", "report_month",
            "prev_arpu", "prev_total_calls", "prev_data", "prev_rech",
            "risk_level", "recommendation"
        ).orderBy(desc("prev_arpu"))

        print(f"[ADS] Silent customers: {silent_enriched.count()} rows")
        return silent_enriched

    # ================================================================
    # 执行完整ETL管道
    # ================================================================
    def run_pipeline(self):
        print("=" * 60)
        print("Telecom Customer Behavior ETL Pipeline Starting...")
        print(f"Source: {self.input_csv}")
        print(f"Architecture: CSV → ODS(wide) → DWD(long) → DWS → ADS")
        print("=" * 60)

        # ODS: 加载宽表
        wide_df = self.load_raw_data()

        # DWD: 宽表转长表 (核心步骤)
        long_df = self.wide_to_long(wide_df)

        # DWS: 客户月度KPI
        customer_kpi = self.aggregate_customer_kpi(long_df)

        # DWS: Circle月度KPI
        circle_kpi = self.aggregate_circle_kpi(long_df)

        # ADS: Circle业绩排名
        circle_ranking = self.generate_circle_ranking(circle_kpi)

        # ADS: 用量骤降告警
        decline_alerts = self.generate_usage_decline_alert(long_df)

        # ADS: 沉默客户报告
        silent_report = self.generate_silent_customer_report(long_df)

        # 写入本地输出
        print("\n" + "=" * 60)
        print("Writing results to output/ ...")
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        self._save_csv(circle_ranking, "circle_ranking.csv")
        self._save_csv(decline_alerts.limit(500), "decline_alerts.csv")
        self._save_csv(silent_report.limit(500), "silent_customers.csv")

        # 打印 Pipeline 总结
        print("\n" + "=" * 60)
        print("ETL Pipeline Complete!")
        print("=" * 60)
        print(f"  ODS Raw rows:      {wide_df.count():,} (226-column wide table)")
        print(f"  DWD Long rows:     {long_df.count():,} (per-customer-per-month)")
        print(f"  DWS Customer KPI:  {customer_kpi.count():,}")
        print(f"  DWS Circle KPI:    {circle_kpi.count()}")
        print(f"  ADS Circle Rank:   {circle_ranking.count()}")
        print(f"  ADS Alerts:        {decline_alerts.count()}")
        print(f"  ADS Silent:        {silent_report.count()}")
        print(f"  Output directory:  {OUTPUT_DIR}")

        # 样例展示
        print("\n--- Circle Performance Ranking (2014-09) ---")
        top5 = circle_ranking \
            .filter(col("report_month") == "2014-09") \
            .orderBy("rank").limit(5)
        top5.show(truncate=False)

        self.spark.stop()
        return {
            "circle_ranking": circle_ranking,
            "decline_alerts": decline_alerts,
            "silent_report": silent_report,
        }

    def _save_csv(self, df, filename):
        path = os.path.join(OUTPUT_DIR, filename)
        df.toPandas().to_csv(path, index=False)
        print(f"  Saved: {path}")


def main():
    etl = TelecomCustomerETL()
    return etl.run_pipeline()


if __name__ == "__main__":
    main()
