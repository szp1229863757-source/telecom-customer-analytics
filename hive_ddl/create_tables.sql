-- ============================================================
-- 运营商客户行为大数据分析系统 - Hive 数据仓库建表脚本
-- 分层架构：ODS → DWD → DWS → ADS
-- 数据源：Telecom Customer Churn Case Study (Kaggle)
-- 99,999 印度预付费客户 × 226列 × 4个月(2014.06-09)
-- ============================================================

CREATE DATABASE IF NOT EXISTS telecom_customer_dw
COMMENT '运营商客户行为数据仓库';
USE telecom_customer_dw;

-- ============================================================
-- ODS 层：贴源层（原始宽表 226列）
-- 保持原始数据格式，按circle_id分区，不做任何加工
-- ============================================================
DROP TABLE IF EXISTS ods_telecom_raw;
CREATE EXTERNAL TABLE ods_telecom_raw (
    mobile_number               BIGINT          COMMENT '客户手机号(唯一标识)',
    circle_id                   STRING          COMMENT '运营商地区ID',
    aon                         INT             COMMENT '在网天数(Age on Network)',
    loc_og_t2o_mou              DOUBLE          COMMENT '本地去电-运营商外-通话时长',
    std_og_t2o_mou              DOUBLE          COMMENT '长途去电-运营商外-通话时长',
    loc_ic_t2o_mou              DOUBLE          COMMENT '本地来电-运营商外-通话时长',
    -- 月度指标(原始宽表以 _6/_7/_8/_9 后缀区分月份) --
    last_date_of_month_6        STRING          COMMENT '6月最后日期',
    last_date_of_month_7        STRING          COMMENT '7月最后日期',
    last_date_of_month_8        STRING          COMMENT '8月最后日期',
    last_date_of_month_9        STRING          COMMENT '9月最后日期',
    arpu_6                      DOUBLE          COMMENT '6月ARPU',
    arpu_7                      DOUBLE          COMMENT '7月ARPU',
    arpu_8                      DOUBLE          COMMENT '8月ARPU',
    arpu_9                      DOUBLE          COMMENT '9月ARPU',
    onnet_mou_6                 DOUBLE          COMMENT '6月网内通话时长',
    onnet_mou_7                 DOUBLE          COMMENT '7月网内通话时长',
    onnet_mou_8                 DOUBLE          COMMENT '8月网内通话时长',
    onnet_mou_9                 DOUBLE          COMMENT '9月网内通话时长',
    offnet_mou_6                DOUBLE          COMMENT '6月网外通话时长',
    offnet_mou_7                DOUBLE          COMMENT '7月网外通话时长',
    offnet_mou_8                DOUBLE          COMMENT '8月网外通话时长',
    offnet_mou_9                DOUBLE          COMMENT '9月网外通话时长',
    roam_og_mou_6               DOUBLE          COMMENT '6月漫游去电时长',
    roam_og_mou_7               DOUBLE          COMMENT '7月漫游去电时长',
    roam_og_mou_8               DOUBLE          COMMENT '8月漫游去电时长',
    roam_og_mou_9               DOUBLE          COMMENT '9月漫游去电时长',
    roam_ic_mou_6               DOUBLE          COMMENT '6月漫游来电时长',
    roam_ic_mou_7               DOUBLE          COMMENT '7月漫游来电时长',
    roam_ic_mou_8               DOUBLE          COMMENT '8月漫游来电时长',
    roam_ic_mou_9               DOUBLE          COMMENT '9月漫游来电时长',
    loc_og_mou_6                DOUBLE          COMMENT '6月本地去电时长',
    loc_og_mou_7                DOUBLE          COMMENT '7月本地去电时长',
    loc_og_mou_8                DOUBLE          COMMENT '8月本地去电时长',
    loc_og_mou_9                DOUBLE          COMMENT '9月本地去电时长',
    std_og_mou_6                DOUBLE          COMMENT '6月长途去电时长',
    std_og_mou_7                DOUBLE          COMMENT '7月长途去电时长',
    std_og_mou_8                DOUBLE          COMMENT '8月长途去电时长',
    std_og_mou_9                DOUBLE          COMMENT '9月长途去电时长',
    isd_og_mou_6                DOUBLE          COMMENT '6月国际去电时长',
    isd_og_mou_7                DOUBLE          COMMENT '7月国际去电时长',
    isd_og_mou_8                DOUBLE          COMMENT '8月国际去电时长',
    isd_og_mou_9                DOUBLE          COMMENT '9月国际去电时长',
    total_og_mou_6              DOUBLE          COMMENT '6月总去电时长',
    total_og_mou_7              DOUBLE          COMMENT '7月总去电时长',
    total_og_mou_8              DOUBLE          COMMENT '8月总去电时长',
    total_og_mou_9              DOUBLE          COMMENT '9月总去电时长',
    loc_ic_mou_6                DOUBLE          COMMENT '6月本地来电时长',
    loc_ic_mou_7                DOUBLE          COMMENT '7月本地来电时长',
    loc_ic_mou_8                DOUBLE          COMMENT '8月本地来电时长',
    loc_ic_mou_9                DOUBLE          COMMENT '9月本地来电时长',
    std_ic_mou_6                DOUBLE          COMMENT '6月长途来电时长',
    std_ic_mou_7                DOUBLE          COMMENT '7月长途来电时长',
    std_ic_mou_8                DOUBLE          COMMENT '8月长途来电时长',
    std_ic_mou_9                DOUBLE          COMMENT '9月长途来电时长',
    isd_ic_mou_6                DOUBLE          COMMENT '6月国际来电时长',
    isd_ic_mou_7                DOUBLE          COMMENT '7月国际来电时长',
    isd_ic_mou_8                DOUBLE          COMMENT '8月国际来电时长',
    isd_ic_mou_9                DOUBLE          COMMENT '9月国际来电时长',
    total_ic_mou_6              DOUBLE          COMMENT '6月总来电时长',
    total_ic_mou_7              DOUBLE          COMMENT '7月总来电时长',
    total_ic_mou_8              DOUBLE          COMMENT '8月总来电时长',
    total_ic_mou_9              DOUBLE          COMMENT '9月总来电时长',
    total_rech_num_6            DOUBLE          COMMENT '6月充值次数',
    total_rech_num_7            DOUBLE          COMMENT '7月充值次数',
    total_rech_num_8            DOUBLE          COMMENT '8月充值次数',
    total_rech_num_9            DOUBLE          COMMENT '9月充值次数',
    total_rech_amt_6            DOUBLE          COMMENT '6月充值金额',
    total_rech_amt_7            DOUBLE          COMMENT '7月充值金额',
    total_rech_amt_8            DOUBLE          COMMENT '8月充值金额',
    total_rech_amt_9            DOUBLE          COMMENT '9月充值金额',
    max_rech_amt_6              DOUBLE          COMMENT '6月单笔最大充值',
    max_rech_amt_7              DOUBLE          COMMENT '7月单笔最大充值',
    max_rech_amt_8              DOUBLE          COMMENT '8月单笔最大充值',
    max_rech_amt_9              DOUBLE          COMMENT '9月单笔最大充值',
    vol_2g_mb_6                 DOUBLE          COMMENT '6月2G流量(MB)',
    vol_2g_mb_7                 DOUBLE          COMMENT '7月2G流量(MB)',
    vol_2g_mb_8                 DOUBLE          COMMENT '8月2G流量(MB)',
    vol_2g_mb_9                 DOUBLE          COMMENT '9月2G流量(MB)',
    vol_3g_mb_6                 DOUBLE          COMMENT '6月3G流量(MB)',
    vol_3g_mb_7                 DOUBLE          COMMENT '7月3G流量(MB)',
    vol_3g_mb_8                 DOUBLE          COMMENT '8月3G流量(MB)',
    vol_3g_mb_9                 DOUBLE          COMMENT '9月3G流量(MB)',
    monthly_2g_6                INT             COMMENT '6月2G月包用户',
    monthly_2g_7                INT             COMMENT '7月2G月包用户',
    monthly_2g_8                INT             COMMENT '8月2G月包用户',
    monthly_2g_9                INT             COMMENT '9月2G月包用户',
    monthly_3g_6                INT             COMMENT '6月3G月包用户',
    monthly_3g_7                INT             COMMENT '7月3G月包用户',
    monthly_3g_8                INT             COMMENT '8月3G月包用户',
    monthly_3g_9                INT             COMMENT '9月3G月包用户',
    sachet_2g_6                 INT             COMMENT '6月2G小包用户',
    sachet_2g_7                 INT             COMMENT '7月2G小包用户',
    sachet_2g_8                 INT             COMMENT '8月2G小包用户',
    sachet_2g_9                 INT             COMMENT '9月2G小包用户',
    sachet_3g_6                 INT             COMMENT '6月3G小包用户',
    sachet_3g_7                 INT             COMMENT '7月3G小包用户',
    sachet_3g_8                 INT             COMMENT '8月3G小包用户',
    sachet_3g_9                 INT             COMMENT '9月3G小包用户',
    night_pck_user_6            INT             COMMENT '6月夜间包用户',
    night_pck_user_7            INT             COMMENT '7月夜间包用户',
    night_pck_user_8            INT             COMMENT '8月夜间包用户',
    night_pck_user_9            INT             COMMENT '9月夜间包用户',
    fb_user_6                   INT             COMMENT '6月Facebook用户',
    fb_user_7                   INT             COMMENT '7月Facebook用户',
    fb_user_8                   INT             COMMENT '8月Facebook用户',
    fb_user_9                   INT             COMMENT '9月Facebook用户',
    jun_vbc_3g                  INT             COMMENT '6月3G VBC',
    jul_vbc_3g                  INT             COMMENT '7月3G VBC',
    aug_vbc_3g                  INT             COMMENT '8月3G VBC',
    sep_vbc_3g                  INT             COMMENT '9月3G VBC'
)
COMMENT '印度运营商预付费客户原始宽表 - ODS贴源层'
PARTITIONED BY (circle_id STRING COMMENT '运营商地区ID')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'creator'='telecom-customer-analytics'
);


-- ============================================================
-- DWD 层：明细层（宽表 → 长表, 每客户每自然月一行）
-- 核心ETL: 将 _6/_7/_8/_9 四组列 UNPIVOT 为长格式
-- 粒度: customer_id + report_month
-- ============================================================
DROP TABLE IF EXISTS dwd_customer_monthly;
CREATE EXTERNAL TABLE dwd_customer_monthly (
    mobile_number               BIGINT          COMMENT '客户手机号',
    circle_id                   STRING          COMMENT '运营商地区ID',
    aon                         INT             COMMENT '在网天数',
    report_month                STRING          COMMENT '报告月份(2014-06/07/08/09)',
    -- 通话指标 --
    total_og_mou                DOUBLE          COMMENT '总去电分钟数',
    total_ic_mou                DOUBLE          COMMENT '总来电分钟数',
    loc_og_mou                  DOUBLE          COMMENT '本地去电分钟',
    std_og_mou                  DOUBLE          COMMENT '长途去电分钟',
    isd_og_mou                  DOUBLE          COMMENT '国际去电分钟',
    loc_ic_mou                  DOUBLE          COMMENT '本地来电分钟',
    std_ic_mou                  DOUBLE          COMMENT '长途来电分钟',
    isd_ic_mou                  DOUBLE          COMMENT '国际来电分钟',
    onnet_mou                   DOUBLE          COMMENT '网内通话分钟',
    offnet_mou                  DOUBLE          COMMENT '网外通话分钟',
    roam_og_mou                 DOUBLE          COMMENT '漫游去电分钟',
    roam_ic_mou                 DOUBLE          COMMENT '漫游来电分钟',
    -- 充值指标 --
    total_rech_num              DOUBLE          COMMENT '总充值次数',
    total_rech_amt              DOUBLE          COMMENT '总充值金额',
    max_rech_amt                DOUBLE          COMMENT '单笔最大充值金额',
    -- 数据用量 --
    vol_2g_mb                   DOUBLE          COMMENT '2G流量(MB)',
    vol_3g_mb                   DOUBLE          COMMENT '3G流量(MB)',
    total_data_mb               DOUBLE          COMMENT '总流量(MB) = 2G+3G',
    -- 套餐/包使用 --
    monthly_2g                  INT             COMMENT '是否2G月包用户',
    monthly_3g                  INT             COMMENT '是否3G月包用户',
    sachet_2g                   INT             COMMENT '是否2G小包用户',
    sachet_3g                   INT             COMMENT '是否3G小包用户',
    night_pck_user              INT             COMMENT '是否夜间包用户',
    fb_user                     INT             COMMENT '是否Facebook用户',
    -- 活跃标志 --
    is_active_call              INT             COMMENT '当月有通话=1',
    is_active_data              INT             COMMENT '当月有流量=1',
    is_active                   INT             COMMENT '当月活跃(通话或流量)=1'
)
COMMENT '客户月度明细宽表 - DWD明细层(长格式, 每客户每月一行)'
PARTITIONED BY (report_month STRING COMMENT '报告月份 yyyy-MM')
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ETL 逻辑(在PySpark中实现):
-- 1. 读取 ods_telecom_raw 宽表
-- 2. 对每个月份后缀(_6/_7/_8/_9), 提取该月所有指标列, 去除后缀后 UNION
-- 3. 计算派生字段: total_data_mb = vol_2g_mb + vol_3g_mb
-- 4. 计算活跃标志: is_active_call / is_active_data / is_active
-- 5. 写入 dwd_customer_monthly 按 report_month 分区


-- ============================================================
-- DWS 层：汇总层 1 - 每客户每月综合KPI
-- 粒度: customer_id + report_month
-- ============================================================
DROP TABLE IF EXISTS dws_customer_monthly_kpi;
CREATE EXTERNAL TABLE dws_customer_monthly_kpi (
    mobile_number               BIGINT          COMMENT '客户手机号',
    circle_id                   STRING          COMMENT '运营商地区ID',
    aon                         INT             COMMENT '在网天数',
    report_month                STRING          COMMENT '报告月份',
    -- 综合KPI --
    total_calls_mou             DOUBLE          COMMENT '总通话分钟(来+去)',
    total_recharge_amt          DOUBLE          COMMENT '总充值金额',
    total_data_mb               DOUBLE          COMMENT '总流量(MB)',
    arpu                        DOUBLE          COMMENT 'ARPU(月均收入)',
    avg_rech_amt                DOUBLE          COMMENT '平均单次充值金额',
    -- 行为标签 --
    is_heavy_caller             INT             COMMENT '重度通话用户(通话>500min)',
    is_heavy_data_user          INT             COMMENT '重度数据用户(流量>1GB)',
    is_high_value               INT             COMMENT '高价值客户(ARPU>500)',
    is_roamer                   INT             COMMENT '漫游用户',
    is_data_user                INT             COMMENT '数据用户(有流量)',
    is_2g_user                  INT             COMMENT '2G流量用户',
    is_3g_user                  INT             COMMENT '3G流量用户'
)
COMMENT '客户月度综合KPI - DWS汇总层'
PARTITIONED BY (report_month STRING COMMENT '报告月份 yyyy-MM')
STORED AS PARQUET;


-- ============================================================
-- DWS 层：汇总层 2 - 每个circle每月汇总
-- 粒度: circle_id + report_month
-- ============================================================
DROP TABLE IF EXISTS dws_circle_monthly_kpi;
CREATE EXTERNAL TABLE dws_circle_monthly_kpi (
    circle_id                   STRING          COMMENT '运营商地区ID',
    report_month                STRING          COMMENT '报告月份',
    -- 客户统计 --
    total_customers             BIGINT          COMMENT '总客户数',
    active_customers            BIGINT          COMMENT '活跃客户数',
    active_rate                 DOUBLE          COMMENT '活跃率',
    avg_tenure_days             DOUBLE          COMMENT '平均在网天数',
    -- 通话 --
    total_og_mou                DOUBLE          COMMENT '总去电分钟',
    total_ic_mou                DOUBLE          COMMENT '总来电分钟',
    avg_mou_per_user            DOUBLE          COMMENT '人均通话分钟',
    -- 收入 --
    total_recharge_amt          DOUBLE          COMMENT '总充值金额',
    avg_arpu                    DOUBLE          COMMENT '平均ARPU',
    avg_rech_per_user           DOUBLE          COMMENT '人均充值金额',
    -- 数据 --
    total_2g_mb                 DOUBLE          COMMENT '总2G流量',
    total_3g_mb                 DOUBLE          COMMENT '总3G流量',
    avg_data_mb_per_user        DOUBLE          COMMENT '人均流量(MB)',
    -- 用户构成 --
    heavy_caller_pct            DOUBLE          COMMENT '重度通话占比',
    data_user_pct               DOUBLE          COMMENT '数据用户占比',
    high_value_pct              DOUBLE          COMMENT '高价值客户占比',
    roamer_pct                  DOUBLE          COMMENT '漫游用户占比',
    -- 流失相关 --
    inactive_customers          BIGINT          COMMENT '当月不活跃客户数',
    inactive_rate               DOUBLE          COMMENT '不活跃率(潜在流失)',
    zero_call_customers         BIGINT          COMMENT '零通话客户数',
    zero_data_customers         BIGINT          COMMENT '零数据客户数'
)
COMMENT 'Circle(地区)月度汇总KPI - DWS汇总层'
PARTITIONED BY (report_month STRING COMMENT '报告月份 yyyy-MM')
STORED AS PARQUET;


-- ============================================================
-- ADS 层：应用层 1 - Circle(地区)业绩排名
-- 按ARPU/通话/数据/充值多维度综合打分排名
-- ============================================================
DROP TABLE IF EXISTS ads_circle_performance_ranking;
CREATE EXTERNAL TABLE ads_circle_performance_ranking (
    circle_id                   STRING          COMMENT '运营商地区ID',
    report_month                STRING          COMMENT '报告月份',
    total_customers             BIGINT          COMMENT '客户数',
    avg_arpu                    DOUBLE          COMMENT '平均ARPU',
    avg_mou_per_user            DOUBLE          COMMENT '人均通话分钟',
    avg_data_mb_per_user        DOUBLE          COMMENT '人均流量MB',
    active_rate                 DOUBLE          COMMENT '活跃率',
    inactive_rate               DOUBLE          COMMENT '不活跃率',
    arpu_score                  DOUBLE          COMMENT 'ARPU得分(0-100)',
    activity_score              DOUBLE          COMMENT '活跃度得分(0-100)',
    data_score                  DOUBLE          COMMENT '数据使用得分(0-100)',
    composite_score             DOUBLE          COMMENT '综合得分',
    rank                        INT             COMMENT '综合排名'
)
COMMENT 'Circle业绩排名 - ADS应用层'
PARTITIONED BY (report_month STRING)
STORED AS PARQUET;


-- ============================================================
-- ADS 层：应用层 2 - 用量骤降告警
-- 环比通话或数据下降>50%的客户, 运营可跟进挽回
-- ============================================================
DROP TABLE IF EXISTS ads_usage_decline_alert;
CREATE EXTERNAL TABLE ads_usage_decline_alert (
    mobile_number               BIGINT          COMMENT '客户手机号',
    circle_id                   STRING          COMMENT '运营商地区ID',
    report_month                STRING          COMMENT '当前报告月份',
    prev_month                  STRING          COMMENT '环比月份',
    -- 当前月 --
    curr_calls_mou              DOUBLE          COMMENT '当月通话分钟',
    curr_data_mb                DOUBLE          COMMENT '当月流量MB',
    curr_recharge_amt           DOUBLE          COMMENT '当月充值金额',
    -- 上个月 --
    prev_calls_mou              DOUBLE          COMMENT '上月通话分钟',
    prev_data_mb                DOUBLE          COMMENT '上月流量MB',
    prev_recharge_amt           DOUBLE          COMMENT '上月充值金额',
    -- 环比变化 --
    calls_decline_pct           DOUBLE          COMMENT '通话下降比例',
    data_decline_pct            DOUBLE          COMMENT '流量下降比例',
    recharge_decline_pct        DOUBLE          COMMENT '充值下降比例',
    -- 告警 --
    alert_level                 STRING          COMMENT '告警级别(CRITICAL/WARNING)',
    alert_reason                STRING          COMMENT '告警原因'
)
COMMENT '用量骤降告警 - ADS应用层(环比下降>50%)'
PARTITIONED BY (report_month STRING)
STORED AS PARQUET;


-- ============================================================
-- ADS 层：应用层 3 - 沉默客户报告
-- 当月无通话且无数据的客户, 高流失风险
-- ============================================================
DROP TABLE IF EXISTS ads_silent_customer_report;
CREATE EXTERNAL TABLE ads_silent_customer_report (
    mobile_number               BIGINT          COMMENT '客户手机号',
    circle_id                   STRING          COMMENT '运营商地区ID',
    aon                         INT             COMMENT '在网天数',
    report_month                STRING          COMMENT '沉默月份',
    prev_arpu                   DOUBLE          COMMENT '上月ARPU',
    prev_total_calls            DOUBLE          COMMENT '上月总通话',
    prev_total_data             DOUBLE          COMMENT '上月总流量',
    prev_total_recharge         DOUBLE          COMMENT '上月总充值',
    risk_level                  STRING          COMMENT '流失风险(HIGH/MEDIUM/LOW)',
    recommendation              STRING          COMMENT '建议措施'
)
COMMENT '沉默客户报告 - ADS应用层(当月零通话零流量)'
PARTITIONED BY (report_month STRING)
STORED AS PARQUET;


-- ============================================================
-- 示例查询：检验数据仓库分层效果
-- ============================================================

-- 1. 查看某月各Circle业绩排名(TOP 5)
-- SELECT circle_id, avg_arpu, avg_mou_per_user, active_rate,
--        composite_score, rank
-- FROM ads_circle_performance_ranking
-- WHERE report_month='2014-09'
-- ORDER BY rank
-- LIMIT 5;

-- 2. 查看某月用量骤降的高危客户
-- SELECT mobile_number, circle_id,
--        ROUND(calls_decline_pct*100,1) as calls_down_pct,
--        ROUND(data_decline_pct*100,1) as data_down_pct,
--        alert_level, alert_reason
-- FROM ads_usage_decline_alert
-- WHERE report_month='2014-09' AND alert_level='CRITICAL'
-- LIMIT 20;

-- 3. 查看某Circle四个月的趋势
-- SELECT report_month, total_customers, active_customers,
--        ROUND(avg_arpu,2) as avg_arpu,
--        ROUND(active_rate*100,1) as active_pct
-- FROM dws_circle_monthly_kpi
-- WHERE circle_id='1'
-- ORDER BY report_month;

-- 4. 沉默客户的高危群体(上月高ARPU但本月沉默)
-- SELECT mobile_number, circle_id, aon,
--        prev_arpu, prev_total_calls, risk_level
-- FROM ads_silent_customer_report
-- WHERE report_month='2014-09'
--   AND risk_level='HIGH'
-- ORDER BY prev_arpu DESC
-- LIMIT 50;
