
<div align="center">

# 🏦 Financial Data Platform

### Enterprise-Grade Data Engineering on AWS + Databricks

[![CI — Lint, Test & Validate](https://github.com/Khethu188/financial-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Khethu188/financial-data-platform/actions/workflows/ci.yml)
[![CD — Deploy to Dev](https://github.com/Khethu188/financial-data-platform/actions/workflows/cd.yml/badge.svg)](https://github.com/Khethu188/financial-data-platform/actions/workflows/cd.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Terraform](https://img.shields.io/badge/terraform-1.7-purple.svg)](https://www.terraform.io/)
[![Databricks](https://img.shields.io/badge/databricks-14.3-red.svg)](https://www.databricks.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen.svg)]()

---

**A production-ready data platform processing thousands of South African financial transactions daily through a Medallion Architecture (Bronze → Silver → Gold), with automated data quality monitoring, anomaly detection, self-healing pipelines, and executive dashboards.**

[Architecture](#-architecture) •
[Features](#-features) •
[Quick Start](#-quick-start) •
[Pipeline](#-pipeline-flow) •
[Dashboards](#-dashboards) •
[Documentation](#-documentation)

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Pipeline Flow](#-pipeline-flow)
- [Data Quality](#-data-quality)
- [Dashboards](#-dashboards)
- [CI/CD](#-cicd)
- [Cost Optimization](#-cost-optimization)
- [Documentation](#-documentation)
- [Author](#-author)

---

## 🔍 Overview

### The Problem

A growing South African financial services company processes thousands of transactions daily across 9 provinces and international markets. Raw data arrives with **~15% defect rates** — missing fields, invalid currencies, duplicate records, and suspicious transactions. Manual data cleaning is unsustainable, quality issues erode trust in reporting, and anomalies go undetected.

### The Solution

This platform automates the entire data lifecycle:

| Challenge | Solution |
|-----------|----------|
| Messy raw data (15% defects) | Automated cleansing with quarantine & recovery |
| No data quality visibility | 20+ quality rules with historical tracking |
| Manual anomaly detection | Statistical anomaly detection (Z-score, IQR) |
| Fragile infrastructure | Terraform IaC with CI/CD automation |
| Expensive compute | SPOT instances, autoscaling, right-sized clusters |
| No executive visibility | 5 Power BI dashboards with RLS |

### Key Metrics

| Metric | Value |
|--------|-------|
| **Data Recovery Rate** | 90%+ of quarantined records auto-recovered |
| **Quality Pass Rate** | 95%+ across all Medallion layers |
| **Pipeline Runtime** | ~45 minutes (full daily run) |
| **Infrastructure Cost** | ~60-80% savings via SPOT instances |
| **Quality Rules** | 20+ automated checks across 4 categories |
| **Anomaly Detection** | 4 detection methods (Z-score, IQR, off-hours, weekend) |

---

## 🏗 Architecture

