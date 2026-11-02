
# ─────────────────────────────────────────────────────────────────────────────
# Monitoring Module — CloudWatch Alarms, SNS Alerts, AWS Budgets
# ─────────────────────────────────────────────────────────────────────────────

# ─── SNS Topic for All Alerts ────────────────────────────────────────────────

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"

  tags = {
    Component = "monitoring"
  }
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ─── AWS Budget (Monthly Spend Limit) ───────────────────────────────────────

resource "aws_budgets_budget" "monthly" {
  name         = "${var.name_prefix}-monthly-budget"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_limit
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:Project$${var.name_prefix}"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

# ─── CloudWatch Alarms: S3 Bucket Size Monitoring ───────────────────────────

resource "aws_cloudwatch_metric_alarm" "landing_bucket_size" {
  alarm_name          = "${var.name_prefix}-landing-bucket-size"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "BucketSizeBytes"
  namespace           = "AWS/S3"
  period              = 86400  # Daily check
  statistic           = "Average"
  threshold           = 5368709120  # 5 GB
  alarm_description   = "Landing zone bucket exceeds 5GB — check if lifecycle policies are working"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    BucketName  = var.bucket_names["landing"]
    StorageType = "StandardStorage"
  }

  tags = {
    Component = "monitoring"
  }
}

# ─── CloudWatch Alarm: S3 4xx Errors (Access Issues) ────────────────────────

resource "aws_cloudwatch_metric_alarm" "s3_4xx_errors" {
  alarm_name          = "${var.name_prefix}-s3-4xx-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "4xxErrors"
  namespace           = "AWS/S3"
  period              = 300  # 5 minutes
  statistic           = "Sum"
  threshold           = 50
  alarm_description   = "High rate of S3 4xx errors — possible permission or config issue"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    BucketName = var.bucket_names["landing"]
    FilterId   = "AllRequests"
  }

  tags = {
    Component = "monitoring"
  }
}

# ─── CloudWatch Alarm: S3 5xx Errors (Service Issues) ───────────────────────

resource "aws_cloudwatch_metric_alarm" "s3_5xx_errors" {
  alarm_name          = "${var.name_prefix}-s3-5xx-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "5xxErrors"
  namespace           = "AWS/S3"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "S3 service errors detected — possible AWS issue"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    BucketName = var.bucket_names["landing"]
    FilterId   = "AllRequests"
  }

  tags = {
    Component = "monitoring"
  }
}

