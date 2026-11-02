
-- ═══════════════════════════════════════════════════════════════════════════
-- Gold Layer SQL Views for Power BI Consumption
-- ═══════════════════════════════════════════════════════════════════════════
-- These views sit on top of Gold Delta tables and provide a clean,
-- optimized interface for Power BI DirectQuery or Import mode.
-- ═══════════════════════════════════════════════════════════════════════════

-- ─── Create Gold Database ───────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS financial_platform.gold;
USE financial_platform.gold;

-- ═══════════════════════════════════════════════════════════════════════════
-- DIMENSION VIEWS
-- ═══════════════════════════════════════════════════════════════════════════

-- ─── dim_date: Calendar dimension for time intelligence ─────────────────

CREATE OR REPLACE VIEW vw_dim_date AS
WITH date_spine AS (
    SELECT explode(sequence(
        DATE '2024-01-01',
        DATE '2027-12-31',
        INTERVAL 1 DAY
    )) AS date_key
)
SELECT
    date_key,
    YEAR(date_key)                                  AS year,
    MONTH(date_key)                                 AS month_num,
    DAY(date_key)                                   AS day_num,
    DATE_FORMAT(date_key, 'MMMM')                   AS month_name,
    DATE_FORMAT(date_key, 'MMM')                     AS month_short,
    DATE_FORMAT(date_key, 'EEEE')                    AS day_name,
    DATE_FORMAT(date_key, 'EEE')                     AS day_short,
    QUARTER(date_key)                                AS quarter_num,
    CONCAT('Q', QUARTER(date_key))                   AS quarter_label,
    CONCAT(YEAR(date_key), '-Q', QUARTER(date_key))  AS year_quarter,
    CONCAT(YEAR(date_key), '-', LPAD(MONTH(date_key), 2, '0')) AS year_month,
    WEEKOFYEAR(date_key)                             AS week_of_year,
    DAYOFWEEK(date_key)                              AS day_of_week_num,
    CASE WHEN DAYOFWEEK(date_key) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN DAY(LAST_DAY(date_key)) = DAY(date_key) THEN TRUE ELSE FALSE END AS is_month_end,
    CASE WHEN MONTH(date_key) IN (1, 4, 7, 10) AND DAY(date_key) = 1 THEN TRUE ELSE FALSE END AS is_quarter_start,
    CONCAT('FY', CASE WHEN MONTH(date_key) >= 4 THEN YEAR(date_key) + 1 ELSE YEAR(date_key) END) AS fiscal_year
FROM date_spine;

-- ─── dim_region: South African provinces + International ────────────────

CREATE OR REPLACE VIEW vw_dim_region AS
SELECT DISTINCT
    region_code,
    region_name,
    CASE region_code
        WHEN 'GP' THEN 'Gauteng'
        WHEN 'WC' THEN 'Western Cape'
        WHEN 'KZN' THEN 'KwaZulu-Natal'
        WHEN 'EC' THEN 'Eastern Cape'
        WHEN 'FS' THEN 'Free State'
        WHEN 'LP' THEN 'Limpopo'
        WHEN 'MP' THEN 'Mpumalanga'
        WHEN 'NW' THEN 'North West'
        WHEN 'NC' THEN 'Northern Cape'
        WHEN 'INT' THEN 'International'
        ELSE 'Unknown'
    END AS region_full_name,
    CASE region_code
        WHEN 'INT' THEN 'International'
        ELSE 'Domestic'
    END AS region_type,
    CASE region_code
        WHEN 'GP' THEN -26.2041
        WHEN 'WC' THEN -33.9249
        WHEN 'KZN' THEN -29.8587
        WHEN 'EC' THEN -33.0292
        WHEN 'FS' THEN -29.0852
        WHEN 'LP' THEN -23.4013
        WHEN 'MP' THEN -25.4753
        WHEN 'NW' THEN -26.6639
        WHEN 'NC' THEN -28.7282
        ELSE 0.0
    END AS latitude,
    CASE region_code
        WHEN 'GP' THEN 28.0473
        WHEN 'WC' THEN 18.4241
        WHEN 'KZN' THEN 31.0218
        WHEN 'EC' THEN 27.8546
        WHEN 'FS' THEN 26.1596
        WHEN 'LP' THEN 29.4179
        WHEN 'MP' THEN 30.9694
        WHEN 'NW' THEN 25.2838
        WHEN 'NC' THEN 24.7499
        ELSE 0.0
    END AS longitude
FROM delta.`s3://financial-data-platform-dev-gold/revenue_summary`;

-- ─── dim_product: Product hierarchy ─────────────────────────────────────

CREATE OR REPLACE VIEW vw_dim_product AS
SELECT DISTINCT
    product_name,
    product_category,
    CASE product_category
        WHEN 'savings' THEN 1
        WHEN 'investments' THEN 2
        WHEN 'lending' THEN 3
        WHEN 'insurance' THEN 4
        WHEN 'payments' THEN 5
        WHEN 'forex' THEN 6
        ELSE 99
    END AS category_sort_order,
    CASE product_category
        WHEN 'savings' THEN '#2E86AB'
        WHEN 'investments' THEN '#A23B72'
        WHEN 'lending' THEN '#F18F01'
        WHEN 'insurance' THEN '#C73E1D'
        WHEN 'payments' THEN '#3B1F2B'
        WHEN 'forex' THEN '#44BBA4'
        ELSE '#999999'
    END AS category_color
FROM delta.`s3://financial-data-platform-dev-gold/revenue_summary`;

-- ─── dim_customer: Customer dimension from Silver ───────────────────────

CREATE OR REPLACE VIEW vw_dim_customer AS
SELECT
    customer_id,
    customer_name,
    primary_account_id,
    num_accounts,
    total_transactions,
    total_amount_zar,
    avg_transaction_zar,
    first_transaction_date,
    last_transaction_date,
    customer_tenure_days,
    risk_tier,
    value_tier,
    primary_region AS region_code,
    primary_region_name AS region_name,
    failure_rate,
    flag_rate,
    num_product_categories,
    num_channels_used
FROM delta.`s3://financial-data-platform-dev-silver/customers`;

-- ═══════════════════════════════════════════════════════════════════════════
-- FACT VIEWS
-- ═══════════════════════════════════════════════════════════════════════════

-- ─── fact_revenue: Monthly revenue aggregates ───────────────────────────

CREATE OR REPLACE VIEW vw_fact_revenue AS
SELECT
    CONCAT(transaction_year, '-', LPAD(transaction_month, 2, '0'), '-01') AS date_key,
    transaction_year,
    transaction_month,
    product_category,
    product_name,
    region_code,
    region_name,
    channel,
    currency,
    transaction_count,
    total_revenue_zar,
    avg_transaction_zar,
    min_transaction_zar,
    max_transaction_zar,
    stddev_transaction_zar,
    unique_customers,
    unique_accounts,
    debit_count,
    credit_count,
    fee_count,
    debit_revenue_zar,
    credit_revenue_zar,
    fee_revenue_zar,
    weekend_transactions,
    business_hours_transactions,
    ROUND(weekend_transactions * 100.0 / NULLIF(transaction_count, 0), 2) AS weekend_pct,
    ROUND(business_hours_transactions * 100.0 / NULLIF(transaction_count, 0), 2) AS business_hours_pct
FROM delta.`s3://financial-data-platform-dev-gold/revenue_summary`;

-- ─── fact_pnl: Regional profit & loss ───────────────────────────────────

CREATE OR REPLACE VIEW vw_fact_pnl AS
SELECT
    CONCAT(transaction_year, '-', LPAD(transaction_month, 2, '0'), '-01') AS date_key,
    transaction_year,
    transaction_month,
    region_code,
    region_name,
    product_category,
    product_name,
    gross_revenue_zar,
    total_costs_zar,
    net_pnl_zar,
    profit_margin,
    is_profitable,
    transaction_count,
    unique_customers,
    fee_income_zar,
    interest_expense_zar,
    reversal_losses_zar,
    ROUND(fee_income_zar * 100.0 / NULLIF(gross_revenue_zar, 0), 2) AS fee_income_pct,
    ROUND(reversal_losses_zar * 100.0 / NULLIF(total_costs_zar, 0), 2) AS reversal_loss_pct
FROM delta.`s3://financial-data-platform-dev-gold/regional_pnl`;

-- ─── fact_product: Product performance metrics ──────────────────────────

CREATE OR REPLACE VIEW vw_fact_product AS
SELECT
    CONCAT(transaction_year, '-', LPAD(transaction_month, 2, '0'), '-01') AS date_key,
    transaction_year,
    transaction_month,
    product_category,
    product_name,
    total_transactions,
    unique_customers,
    total_volume_zar,
    avg_transaction_zar,
    median_transaction_zar,
    completed_count,
    failed_count,
    flagged_count,
    pending_count,
    completion_rate,
    failure_rate,
    flag_rate,
    mobile_transactions,
    online_transactions,
    branch_transactions,
    atm_transactions,
    digital_adoption_rate,
    revenue_contribution_pct,
    micro_count,
    enterprise_count
FROM delta.`s3://financial-data-platform-dev-gold/product_performance`;

-- ─── fact_anomaly: Anomaly detection results ────────────────────────────

CREATE OR REPLACE VIEW vw_fact_anomaly AS
SELECT
    transaction_id,
    customer_id,
    transaction_date,
    DATE(transaction_date) AS date_key,
    product_category,
    product_name,
    region_code,
    channel,
    amount_in_zar,
    anomaly_type,
    anomaly_score,
    is_zscore_outlier,
    is_iqr_outlier,
    is_offhours_high_value,
    is_weekend_enterprise,
    risk_level,
    CASE
        WHEN risk_level = 'critical' THEN 1
        WHEN risk_level = 'high' THEN 2
        WHEN risk_level = 'medium' THEN 3
        ELSE 4
    END AS risk_sort_order
FROM delta.`s3://financial-data-platform-dev-gold/anomaly_flags`
WHERE anomaly_type IS NOT NULL;

-- ─── fact_quality: Data quality metrics ─────────────────────────────────

CREATE OR REPLACE VIEW vw_fact_quality AS
SELECT
    run_id,
    DATE(check_timestamp) AS date_key,
    check_timestamp,
    rule_id,
    rule_name,
    layer,
    severity,
    check_type,
    column AS checked_column,
    threshold,
    actual_value,
    passed,
    record_count,
    details,
    description,
    ROUND(actual_value * 100, 2) AS score_pct,
    CASE
        WHEN passed = TRUE THEN 'Pass'
        ELSE 'Fail'
    END AS status_label,
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        WHEN 'info' THEN 3
        ELSE 4
    END AS severity_sort_order
FROM delta.`s3://financial-data-platform-dev-gold/quality_metrics`;

-- ─── fact_lineage: Pipeline execution metrics ───────────────────────────

CREATE OR REPLACE VIEW vw_fact_lineage AS
SELECT
    run_id,
    DATE(run_timestamp) AS date_key,
    run_timestamp,
    bronze_count,
    silver_count,
    quarantine_count,
    gold_revenue_count,
    gold_pnl_count,
    gold_product_count,
    gold_anomaly_count,
    recovery_rate,
    quarantine_rate,
    total_size_mb,
    bronze_count + quarantine_count AS total_ingested,
    ROUND(silver_count * 100.0 / NULLIF(bronze_count, 0), 2) AS silver_yield_pct
FROM delta.`s3://financial-data-platform-dev-gold/pipeline_lineage`;

