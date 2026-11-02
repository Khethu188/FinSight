
#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Deploy Databricks Workflows via CLI
# ─────────────────────────────────────────────────────────────────────────────
# Prerequisites:
#   - Databricks CLI installed and configured
#   - DATABRICKS_HOST and DATABRICKS_TOKEN set
#
# Usage:
#   ./deploy_workflows.sh [dev|staging|prod]
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKFLOWS_DIR="${SCRIPT_DIR}/../databricks/workflows"
ENVIRONMENT="${1:-dev}"

echo "============================================================"
echo "  Deploying Databricks Workflows — ${ENVIRONMENT^^}"
echo "============================================================"

# Validate Databricks CLI
if ! command -v databricks &> /dev/null; then
    echo "✗ Databricks CLI not found. Install: pip install databricks-cli"
    exit 1
fi

# Verify connection
echo "→ Verifying Databricks connection..."
databricks workspace ls / > /dev/null 2>&1 || {
    echo "✗ Cannot connect to Databricks. Check DATABRICKS_HOST and DATABRICKS_TOKEN."
    exit 1
}
echo "  ✓ Connected to Databricks"

# ─── Deploy Each Workflow ────────────────────────────────────────────────────

deploy_job() {
    local job_file="$1"
    local job_name=$(python3 -c "import json; print(json.load(open('$job_file'))['name'])")

    echo ""
    echo "→ Deploying: ${job_name}"

    # Check if job already exists
    local existing_job_id=$(databricks jobs list --output JSON 2>/dev/null | \
        python3 -c "
import json, sys
jobs = json.load(sys.stdin).get('jobs', [])
for j in jobs:
    if j['settings']['name'] == '${job_name}':
        print(j['job_id'])
        break
" 2>/dev/null || echo "")

    if [[ -n "$existing_job_id" ]]; then
        echo "  Updating existing job (ID: ${existing_job_id})..."
        databricks jobs reset --job-id "$existing_job_id" --json-file "$job_file"
        echo "  ✓ Updated job: ${job_name} (ID: ${existing_job_id})"
    else
        echo "  Creating new job..."
        local new_job_id=$(databricks jobs create --json-file "$job_file" | \
            python3 -c "import json, sys; print(json.load(sys.stdin)['job_id'])")
        echo "  ✓ Created job: ${job_name} (ID: ${new_job_id})"
    fi
}

# Deploy all workflow definitions
for job_file in "${WORKFLOWS_DIR}"/*.json; do
    if [[ -f "$job_file" ]]; then
        deploy_job "$job_file"
    fi
done

# ─── Sync Notebooks to Databricks Repos ─────────────────────────────────────

echo ""
echo "→ Syncing notebooks via Databricks Repos..."

REPO_PATH="/Repos/financial-data-platform"

# Check if repo exists, create if not
databricks repos get --path "$REPO_PATH" > /dev/null 2>&1 || {
    echo "  Creating Databricks Repo..."
    databricks repos create \
        --url "https://github.com/Khethu188/financial-data-platform" \
        --provider "gitHub" \
        --path "$REPO_PATH"
}

# Pull latest from main branch
echo "  Pulling latest from main branch..."
databricks repos update --path "$REPO_PATH" --branch "main"
echo "  ✓ Notebooks synced"

# ─── Summary ─────────────────────────────────────────────────────────────────

echo ""
echo "============================================================"
echo "  ✓ Deployment Complete"
echo "============================================================"
echo "  Environment : ${ENVIRONMENT}"
echo "  Workflows   : $(ls -1 "${WORKFLOWS_DIR}"/*.json 2>/dev/null | wc -l) deployed"
echo "  Repo        : ${REPO_PATH}"
echo ""
echo "  Next steps:"
echo "    1. Verify jobs in Databricks UI → Workflows"
echo "    2. Trigger a test run: databricks jobs run-now --job-id <ID>"
echo "    3. Monitor: databricks runs list --job-id <ID>"
echo "============================================================"

