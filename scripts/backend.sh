
#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap Script: Initialize Terraform State Backend
# Run this ONCE before the first terraform init of the main project.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="${SCRIPT_DIR}/../modules/state_backend"

echo "============================================================"
echo "  Bootstrapping Terraform State Backend"
echo "============================================================"

cd "$BACKEND_DIR"

echo "→ Initializing state backend module..."
terraform init

echo "→ Planning state backend resources..."
terraform plan -out=backend.tfplan

echo ""
read -p "Apply the state backend? (yes/no): " confirm
if [[ "$confirm" == "yes" ]]; then
    terraform apply backend.tfplan
    echo ""
    echo "✓ State backend created successfully!"
    echo "  You can now run 'terraform init' in the root project directory."
else
    echo "✗ Aborted."
    exit 1
fi

# Cleanup plan file
rm -f backend.tfplan

