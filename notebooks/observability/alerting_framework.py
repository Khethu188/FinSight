
# Databricks notebook source
# MAGIC %md
# MAGIC # 12 — Alerting Framework
# MAGIC
# MAGIC **Purpose:** Send alerts via AWS SNS when data quality issues are detected.
# MAGIC Integrates with the quality monitor (notebook 08) and pipeline lineage (notebook 11).
# MAGIC
# MAGIC **Alert Channels:**
# MAGIC - AWS SNS → Email notifications
# MAGIC - CloudWatch Custom Metrics → Dashboard integration
# MAGIC - Databricks SQL Alerts → In-platform notifications

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

import json
import boto3
from datetime import datetime
from pyspark.sql import functions as F

# ─── Configuration ───────────────────────────────────────────────────────────

SNS_TOPIC_ARN = "arn:aws:sns:af-south-1:ACCOUNT_ID:financial-data-platform-dev-alerts"
CLOUDWATCH_NAMESPACE = "FinancialDataPlatform/DataQuality"

dbutils.widgets.text("run_id", datetime.now().strftime("%Y%m%d_%H%M%S"), "Pipeline Run ID")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Retrieve Quality Alert Payload

# COMMAND ----------

try:
    alert_json = dbutils.jobs.taskValues.get(
        taskKey="quality_monitor",
        key="quality_alert",
        default="null",
    )
    alert_payload = json.loads(alert_json) if alert_json != "null" else None
except Exception as e:
    print(f"No alert payload from quality monitor: {e}")
    alert_payload = None

if alert_payload:
    print(f"⚠ Alert detected: {alert_payload['message']}")
else:
    print("✓ No quality alerts — pipeline is healthy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Send SNS Notification

# COMMAND ----------

def send_sns_alert(topic_arn: str, subject: str, message: dict):
    """Send a structured alert to AWS SNS."""
    try:
        sns_client = boto3.client("sns", region_name="af-south-1")

        formatted_message = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  🚨 FINANCIAL DATA PLATFORM — DATA QUALITY ALERT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Severity    : {message.get('severity', 'unknown').upper()}
  Pipeline Run: {message.get('pipeline_run_id', 'unknown')}
  Timestamp   : {message.get('timestamp', 'unknown')}

  Summary:
  {message.get('message', 'No details available')}

  Critical Failures:
"""
        for failure in message.get("failures", []):
            formatted_message += f"    • {failure['rule']}: {failure['actual']:.4f} (threshold: {failure['threshold']})\n"

        formatted_message += f"""
  Action Required:
  {message.get('action_required', 'Review pipeline logs')}

  Dashboard: https://your-databricks-workspace.cloud.databricks.com/sql/dashboards/quality
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

        response = sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject[:100],  # SNS subject limit
            Message=formatted_message,
            MessageAttributes={
                "severity": {
                    "DataType": "String",
                    "StringValue": message.get("severity", "warning"),
                },
                "source": {
                    "DataType": "String",
                    "StringValue": "data-quality-monitor",
                },
            },
        )

        print(f"✓ SNS alert sent (MessageId: {response['MessageId']})")
        return response

    except Exception as e:
        print(f"✗ Failed to send SNS alert: {e}")
        return None

# COMMAND ----------

if alert_payload:
    severity = alert_payload.get("severity", "warning").upper()
    send_sns_alert(
        topic_arn=SNS_TOPIC_ARN,
        subject=f"[{severity}] Financial Data Platform — Data Quality Alert",
        message=alert_payload,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Publish CloudWatch Custom Metrics

# COMMAND ----------

def publish_cloudwatch_metrics(namespace: str, metrics: list[dict]):
    """Publish custom metrics to CloudWatch for dashboard integration."""
    try:
        cw_client = boto3.client("cloudwatch", region_name="af-south-1")

        metric_data = []
        for m in metrics:
            metric_data.append({
                "MetricName": m["name"],
                "Value": m["value"],
                "Unit": m.get("unit", "None"),
                "Dimensions": [
                    {"Name": "Environment", "Value": "dev"},
                    {"Name": "Pipeline", "Value": "financial-data-platform"},
                    {"Name": "Layer", "Value": m.get("layer", "all")},
                
