
-- ═══════════════════════════════════════════════════════════════════════════
-- DAX Measures Library — Financial Data Platform
-- ═══════════════════════════════════════════════════════════════════════════
-- Organized by dashboard. Create a "Measures" table in Power BI and add these.
-- ═══════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 1: REVENUE MEASURES
-- ═══════════════════════════════════════════════════════════════════════════

-- Total Revenue (ZAR)
Total Revenue =
    SUM(fact_revenue[total_revenue_zar])

-- Total Transactions
Total Transactions =
    SUM(fact_revenue[transaction_count])

-- Average Transaction Value
Avg Transaction Value =
    DIVIDE(
        [Total Revenue],
        [Total Transactions],
        0
    )

-- Unique Customers
Unique Customers =
    SUM(fact_revenue[unique_customers])

-- Revenue Previous Month
Revenue Prev Month =
    CALCULATE(
        [Total Revenue],
        DATEADD(dim_date[date_key], -1, MONTH)
    )

-- Revenue Month-over-Month Growth %
Revenue MoM Growth % =
    VAR CurrentMonth = [Total Revenue]
    VAR PrevMonth = [Revenue Prev Month]
    RETURN
        DIVIDE(
            CurrentMonth - PrevMonth,
            PrevMonth,
            0
        )

-- Revenue Previous Year (Same Period)
Revenue Prev Year =
    CALCULATE(
        [Total Revenue],
        DATEADD(dim_date[date_key], -1, YEAR)
    )

-- Revenue Year-over-Year Growth %
Revenue YoY Growth % =
    VAR CurrentPeriod = [Total Revenue]
    VAR PrevYear = [Revenue Prev Year]
    RETURN
        DIVIDE(
            CurrentPeriod - PrevYear,
            PrevYear,
            0
        )

-- Revenue Running Total (YTD)
Revenue YTD =
    TOTALYTD(
        [Total Revenue],
        dim_date[date_key]
    )

-- Revenue Running Total (QTD)
Revenue QTD =
    TOTALQTD(
        [Total Revenue],
        dim_date[date_key]
    )

-- Revenue 3-Month Moving Average
Revenue 3M Avg =
    AVERAGEX(
        DATESINPERIOD(
            dim_date[date_key],
            MAX(dim_date[date_key]),
            -3,
            MONTH
        ),
        [Total Revenue]
    )

-- Debit Revenue
Debit Revenue =
    SUM(fact_revenue[debit_revenue_zar])

-- Credit Revenue
Credit Revenue =
    SUM(fact_revenue[credit_revenue_zar])

-- Fee Revenue
Fee Revenue =
    SUM(fact_revenue[fee_revenue_zar])

-- Fee Revenue Contribution %
Fee Revenue % =
    DIVIDE(
        [Fee Revenue],
        [Total Revenue],
        0
    )

-- Weekend Transaction %
Weekend Transaction % =
    DIVIDE(
        SUM(fact_revenue[weekend_transactions]),
        [Total Transactions],
        0
    )

-- Business Hours Transaction %
Business Hours % =
    DIVIDE(
        SUM(fact_revenue[business_hours_transactions]),
        [Total Transactions],
        0
    )


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 2: P&L MEASURES
-- ═══════════════════════════════════════════════════════════════════════════

-- Gross Revenue
Gross Revenue =
    SUM(fact_pnl[gross_revenue_zar])

-- Total Costs
Total Costs =
    SUM(fact_pnl[total_costs_zar])

-- Net P&L
Net PnL =
    [Gross Revenue] - [Total Costs]

-- Profit Margin %
Profit Margin % =
    DIVIDE(
        [Net PnL],
        [Gross Revenue],
        0
    )

-- Fee Income
Fee Income =
    SUM(fact_pnl[fee_income_zar])

-- Interest Expense
Interest Expense =
    SUM(fact_pnl[interest_expense_zar])

-- Reversal Losses
Reversal Losses =
    SUM(fact_pnl[reversal_losses_zar])

-- Profitable Segments Count
Profitable Segments =
    COUNTROWS(
        FILTER(fact_pnl, fact_pnl[is_profitable] = TRUE)
    )

-- Loss-Making Segments Count
Loss Making Segments =
    COUNTROWS(
        FILTER(fact_pnl, fact_pnl[is_profitable] = FALSE)
    )

-- P&L Previous Month
PnL Prev Month =
    CALCULATE(
        [Net PnL],
        DATEADD(dim_date[date_key], -1, MONTH)
    )

-- P&L MoM Change
PnL MoM Change =
    [Net PnL] - [PnL Prev Month]

-- Cost-to-Revenue Ratio
Cost to Revenue Ratio =
    DIVIDE(
        [Total Costs],
        [Gross Revenue],
        0
    )


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 3: PRODUCT PERFORMANCE MEASURES
-- ═══════════════════════════════════════════════════════════════════════════

-- Product Volume
Product Volume =
    SUM(fact_product[total_volume_zar])

-- Completion Rate
Completion Rate =
    DIVIDE(
        SUM(fact_product[completed_count]),
        SUM(fact_product[total_transactions]),
        0
    )

-- Failure Rate
Failure Rate =
    DIVIDE(
        SUM(fact_product[failed_count]),
        SUM(fact_product[total_transactions]),
        0
    )

-- Flag Rate
Flag Rate =
    DIVIDE(
        SUM(fact_product[flagged_count]),
        SUM(fact_product[total_transactions]),
        0
    )

-- Digital Adoption Rate
Digital Adoption Rate =
    DIVIDE(
        SUM(fact_product[mobile_transactions]) + SUM(fact_product[online_transactions]),
        SUM(fact_product[total_transactions]),
        0
    )

-- Mobile Share
Mobile Share =
    DIVIDE(
        SUM(fact_product[mobile_transactions]),
        SUM(fact_product[total_transactions]),
        0
    )

-- Revenue Concentration (Top 3 Products %)
Top 3 Product Revenue % =
    VAR TopProducts =
        TOPN(
            3,
            SUMMARIZE(
                fact_product,
                fact_product[product_name],
                "ProductRevenue", SUM(fact_product[total_volume_zar])
            ),
            [ProductRevenue],
            DESC
        )
    VAR TopRevenue = SUMX(TopProducts, [ProductRevenue])
    VAR TotalRevenue = [Product Volume]
    RETURN
        DIVIDE(TopRevenue, TotalRevenue, 0)


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 4: ANOMALY DETECTION MEASURES
-- ═══════════════════════════════════════════════════════════════════════════

-- Total Anomalies
Total Anomalies =
    COUNTROWS(fact_anomaly)

-- Critical Anomalies
Critical Anomalies =
    COUNTROWS(
        FILTER(fact_anomaly, fact_anomaly[risk_level] = "critical")
    )

-- High Risk Anomalies
High Risk Anomalies =
    COUNTROWS(
        FILTER(fact_anomaly, fact_anomaly[risk_level] = "high")
    )

-- Anomaly Amount (Total ZAR at risk)
Anomaly Amount =
    SUM(fact_anomaly[amount_in_zar])

-- Anomaly Rate (vs total transactions)
Anomaly Rate =
    DIVIDE(
        [Total Anomalies],
        [Total Transactions],
        0
    )

-- Z-Score Outlier Count
ZScore Outliers =
    COUNTROWS(
        FILTER(fact_anomaly, fact_anomaly[is_zscore_outlier] = TRUE)
    )

-- Off-Hours High Value Count
Off Hours High Value =
    COUNTROWS(
        FILTER(fact_anomaly, fact_anomaly[is_offhours_high_value] = TRUE)
    )

-- Avg Anomaly Score
Avg Anomaly Score =
    AVERAGE(fact_anomaly[anomaly_score])

-- Anomalies Trend (vs Previous Month)
Anomalies Prev Month =
    CALCULATE(
        [Total Anomalies],
        DATEADD(dim_date[date_key], -1, MONTH)
    )

Anomaly MoM Change % =
    DIVIDE(
        [Total Anomalies] - [Anomalies Prev Month],
        [Anomalies Prev Month],
        0
    )


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 5: DATA QUALITY MEASURES
-- ═══════════════════════════════════════════════════════════════════════════

-- Overall Quality Score
Quality Score =
    AVERAGE(fact_quality[actual_value])

-- Quality Pass Rate
Quality Pass Rate =
    DIVIDE(
        COUNTROWS(FILTER(fact_quality, fact_quality[passed] = TRUE)),
        COUNTROWS(fact_quality),
        0
    )

-- Critical Failures
Critical Failures =
    COUNTROWS(
        FILTER(
            fact_quality,
            fact_quality[passed] = FALSE && fact_quality[severity] = "critical"
        )
    )

-- Warning Failures
Warning Failures =
    COUNTROWS(
        FILTER(
            fact_quality,
            fact_quality[passed] = FALSE && fact_quality[severity] = "warning"
        )
    )

-- Pipeline Health Status
Pipeline Health =
    SWITCH(
        TRUE(),
        [Critical Failures] = 0, "🟢 Healthy",
        [Critical Failures] <= 2, "🟡 Degraded",
        "🔴 Critical"
    )

-- Recovery Rate
Recovery Rate =
    AVERAGE(fact_lineage[recovery_rate])

-- Quarantine Rate
Quarantine Rate =
    AVERAGE(fact_lineage[quarantine_rate])

-- Data Freshness (Hours since last pipeline run)
Data Freshness Hours =
    VAR LastRun = MAX(fact_lineage[run_timestamp])
    RETURN
        DATEDIFF(LastRun, NOW(), HOUR)

-- Total Records Processed (Latest Run)
Latest Run Records =
    CALCULATE(
        MAX(fact_lineage[bronze_count]),
        LASTDATE(fact_lineage[date_key])
    )


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 6: FORMATTING MEASURES (Conditional Formatting Helpers)
-- ═══════════════════════════════════════════════════════════════════════════

-- Revenue MoM Color
Revenue MoM Color =
    IF([Revenue MoM Growth %] >= 0, "#2E86AB", "#C73E1D")

-- Profit Margin Color
Profit Margin Color =
    SWITCH(
        TRUE(),
        [Profit Margin %] >= 0.20, "#2E86AB",
        [Profit Margin %] >= 0.10, "#F18F01",
        [Profit Margin %] >= 0, "#FFB347",
        "#C73E1D"
    )

-- Quality Score Color
Quality Score Color =
    SWITCH(
        TRUE(),
        [Quality Pass Rate] >= 0.95, "#2E86AB",
        [Quality Pass Rate] >= 0.85, "#F18F01",
        "#C73E1D"
    )

-- Anomaly Severity Color
Anomaly Severity Color =
    SWITCH(
        SELECTEDVALUE(fact_anomaly[risk_level]),
        "critical", "#C73E1D",
        "high", "#F18F01",
        "medium", "#FFB347",
        "#2E86AB"
    )

