
// ═══════════════════════════════════════════════════════════════════════════
// Power Query M — Databricks Connection Templates
// ═══════════════════════════════════════════════════════════════════════════
// Use these in Power BI Desktop → Transform Data → Advanced Editor
// ═══════════════════════════════════════════════════════════════════════════


// ─── Parameters (Create these first in Power BI) ────────────────────────
// Name: DatabricksHost     | Type: Text | Default: <workspace>.cloud.databricks.com
// Name: SQLWarehousePath   | Type: Text | Default: /sql/1.0/warehouses/<id>
// Name: Catalog            | Type: Text | Default: financial_platform
// Name: Schema             | Type: Text | Default: gold


// ─── fact_revenue ───────────────────────────────────────────────────────

// let
//     Source = Databricks.Catalogs(DatabricksHost, SQLWarehousePath, [Catalog=Catalog]),
//     gold_Schema = Source{[Name=Schema]}[Data],
//     vw_fact_revenue = gold_Schema{[Name="vw_fact_revenue"]}[Data],
//
//     // Type casting
//     TypedColumns = Table.TransformColumnTypes(vw_fact_revenue, {
//         {"date_key", type date},
//         {"transaction_year", Int64.Type},
//         {"transaction_month", Int64.Type},
//         {"total_revenue_zar", type number},
//         {"avg_transaction_zar", type number},
//         {"transaction_count", Int64.Type},
//         {"unique_customers", Int64.Type}
//     }),
//
//     // Add calculated columns
//     WithYearMonth = Table.AddColumn(TypedColumns, "Year-Month",
//         each Date.ToText([date_key], "yyyy-MM"), type text),
//
//     WithSortKey = Table.AddColumn(WithYearMonth, "MonthSortKey",
//         each [transaction_year] * 100 + [transaction_month], Int64.Type)
// in
//     WithSortKey


// ─── fact_pnl ───────────────────────────────────────────────────────────

// let
//     Source = Databricks.Catalogs(DatabricksHost, SQLWarehousePath, [Catalog=Catalog]),
//     gold_Schema = Source{[Name=Schema]}[Data],
//     vw_fact_pnl = gold_Schema{[Name="vw_fact_pnl"]}[Data],
//
//     TypedColumns = Table.TransformColumnTypes(vw_fact_pnl, {
//         {"date_key", type date},
//         {"gross_revenue_zar", type number},
//         {"total_costs_zar", type number},
//         {"net_pnl_zar", type number},
//         {"profit_margin", type number},
//         {"is_profitable", type logical}
//     })
// in
//     TypedColumns


// ─── fact_product ───────────────────────────────────────────────────────

// let
//     Source = Databricks.Catalogs(DatabricksHost, SQLWarehousePath, [Catalog=Catalog]),
//     gold_Schema = Source{[Name=Schema]}[Data],
//     vw_fact_product = gold_Schema{[Name="vw_fact_product"]}[Data],
//
//     TypedColumns = Table.TransformColumnTypes(vw_fact_product, {
//         {"date_key", type date},
//         {"total_volume_zar", type number},
//         {"completion_rate", type number},
//         {"failure_rate", type number},
//         {"digital_adoption_rate", type number},
//         {"total_transactions", Int64.Type}
//     })
// in
//     TypedColumns


// ─── fact_anomaly ───────────────────────────────────────────────────────

// let
//     Source = Databricks.Catalogs(DatabricksHost, SQLWarehousePath, [Catalog=Catalog]),
//     gold_Schema = Source{[Name=Schema]}[Data],
//     vw_fact_anomaly = gold_Schema{[Name="vw_fact_anomaly"]}[Data],
//
//     TypedColumns = Table.TransformColumnTypes(vw_fact_anomaly, {
//         {"date_key", type date},
//         {"amount_in_zar", type number},
//         {"anomaly_score", type number},
//         {"is_zscore_outlier", type logical},
//         {"is_iqr_outlier", type logical},
//         {"is_offhours_high_value", type logical}
//     }),
//
//     // Add risk level sort
//     WithRiskSort = Table.AddColumn(TypedColumns, "RiskSortOrder",
//         each if [risk_level] = "critical" then 1
//              else if [risk_level] = "high" then 2
//              else if [risk_level] = "medium" then 3
//              else 4, Int64.Type)
// in
//     WithRiskSort


// ─── fact_quality ───────────────────────────────────────────────────────

// let
//     Source = Databricks.Catalogs(DatabricksHost, SQLWarehousePath, [Catalog=Catalog]),
//     gold_Schema = Source{[Name=Schema]}[Data],
//     vw_fact_quality = gold_Schema{[Name="vw_fact_quality"]}[Data],
//
//     TypedColumns = Table.TransformColumnTypes(vw_fact_quality, {
//         {"date_key", type date},
//         {"threshold", type number},
//         {"actual_value", type number},
//         {"passed", type logical},
//         {"record_count", Int64.Type},
//         {"score_pct", type number}
//     })
// in
//     TypedColumns


// ─── dim_date ───────────────────────────────────────────────────────────

// let
//     Source = Databricks.Catalogs(DatabricksHost, SQLWarehousePath, [Catalog=Catalog]),
//     gold_Schema = Source{[Name=Schema]}[Data],
//     vw_dim_date = gold_Schema{[Name="vw_dim_date"]}[Data],
//
//     TypedColumns = Table.TransformColumnTypes(vw_dim_date, {
//         {"date_key", type date},
//         {"year", Int64.Type},
//         {"month_num", Int64.Type},
//         {"quarter_num", Int64.Type},
//         {"is_weekend", type logical},
//         {"is_month_end", type logical}
//     }),
//
//     // Mark as date table
//     MarkedAsDateTable = TypedColumns
// in
//     MarkedAsDateTable


// ─── dim_region ─────────────────────────────────────────────────────────

// let
//     Source = Databricks.Catalogs(DatabricksHost, SQLWarehousePath, [Catalog=Catalog]),
//     gold_Schema = Source{[Name=Schema]}[Data],
//     vw_dim_region = gold_Schema{[Name="vw_dim_region"]}[Data],
//
//     TypedColumns = Table.TransformColumnTypes(vw_dim_region, {
//         {"latitude", type number},
//         {"longitude", type number}
//     })
// in
//     TypedColumns


// ─── dim_customer ───────────────────────────────────────────────────────

// let
//     Source = Databricks.Catalogs(DatabricksHost, SQLWarehousePath, [Catalog=Catalog]),
//     gold_Schema = Source{[Name=Schema]}[Data],
//     vw_dim_customer = gold_Schema{[Name="vw_dim_customer"]}[Data],
//
//     TypedColumns = Table.TransformColumnTypes(vw_dim_customer, {
//         {"total_amount_zar", type number},
//         {"avg_transaction_zar", type number},
//         {"total_transactions", Int64.Type},
//         {"customer_tenure_days", Int64.Type},
//         {"failure_rate", type number},
//         {"flag_rate", type number}
//     })
// in
//     TypedColumns

