
#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Safe Deployment Script
# Runs terraform plan, shows the diff, and asks for confirmation before apply.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."
ENVIRONMENT="${1:-dev}"

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    echo "✗ Invalid environment: $ENVIRONMENT"
    echo "  Usage: ./plan_and_apply.sh [dev|staging|prod]"
    exit 1
fi

TFVARS_FILE="${PROJECT_DIR}/environments/${ENVIRONMENT}.tfvars"

if [[ ! -f "$TFVARS_FILE" ]]; then
    echo "✗ Variable file not found: $TFVARS_FILE"
    exit 1
fi

echo "============================================================"
echo "  Terraform Deployment — ${ENVIRONMENT^^}"
echo "============================================================"
echo "  Var file: $TFVARS_FILE"
echo ""

cd "$PROJECT_DIR"

# Initialize
echo "→ Initializing Terraform..."
terraform init -input=false

# Validate
echo "→ Validating configuration..."
terraform validate

# Format check
echo "→ Checking formatting..."
terraform fmt -check -recursive || {
    echo "⚠ Formatting issues found. Run 'terraform fmt -recursive' to fix."
}

# Plan
echo "→ Creating execution plan..."
terraform plan \
    -var-file="$TFVARS_FILE" \
    -out="${ENVIRONMENT}.tfplan" \
    -input=false

echo ""
echo "============================================================"
echo "  Review the plan above carefully."
echo "============================================================"

if [[ "$ENVIRONMENT" == "prod" ]]; then
    echo "  ⚠  PRODUCTION DEPLOYMENT — Extra confirmation required"
    read -p "  Type 'deploy-prod' to confirm: " confirm
    [[ "$confirm" != "deploy-prod" ]] && echo "✗ Aborted." && exit 1
else
    read -p "  Apply this plan? (yes/no): " confirm
    [[ "$confirm" != "yes" ]] && echo "✗ Aborted." && exit 1
fi

# Apply
echo "→ Applying..."
terraform apply "${ENVIRONMENT}.tfplan"

echo ""
echo "✓ Deployment complete for ${ENVIRONMENT^^}!"

# Show key outputs
echo ""
echo "─── Key Outputs ───────────────────────────────────────────"
terraform output

# Cleanup plan file
rm -f "${ENVIRONMENT}.tfplan"

