
#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Trigger Pipeline Run (Manual or CI/CD)
# ─────────────────────────────────────────────────────────────────────────────
# Usage:
#   ./trigger_pipeline.sh daily                    # Trigger daily pipeline
#   ./trigger_pipeline.sh daily --wait             # Trigger and wait for completion
#   ./trigger_pipeline.sh reprocessing             # Trigger quarantine reprocessing
#   ./trigger_pipeline.sh maintenance              # Trigger table maintenance
#   ./trigger_pipeline.sh daily --date 2026-11-01  # Backfill specific date
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

PIPELINE_TYPE="${1:-daily}"
WAIT_FLAG=""
PROCESS_DATE=""
RUN_ID="manual_$(date +%Y%m%d_%H%M%S)"

# Parse arguments
shift || true
while [[ $# -gt 0 ]]; do
    case "$1" in
        --wait) WAIT_FLAG="true"; shift ;;
        --date) PROCESS_DATE="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Map pipeline type to job name
case "$PIPELINE_TYPE" in
    daily)         JOB_NAME="fdp-daily-pipeline" ;;
    reprocessing)  JOB_NAME="fdp-weekly-quarantine-reprocessing" ;;
    maintenance)   JOB_NAME="fdp-weekly-maintenance" ;;
    *)
        echo "✗ Unknown pipeline type: $PIPELINE_TYPE"
        echo "  Options: daily, reprocessing, maintenance"
        exit 1
        ;;
esac

echo "============================================================"
echo "  Triggering: ${JOB_NAME}"
echo "  Run ID    : ${RUN_ID}"
[[ -n "$PROCESS_DATE" ]] && echo "  Date      : ${PROCESS_DATE}"
echo "============================================================"

# Find job ID
JOB_ID=$(databricks jobs list --output JSON | \
    python3 -c "
import json, sys
jobs = json.load(sys.stdin).get('jobs', [])
for j in jobs:
    if j['settings']['name'] == '${JOB_NAME}':
        print(j['job_id'])
        break
" 2>/dev/null)

if [[ -z "$JOB_ID" ]]; then
    echo "✗ Job not found: ${JOB_NAME}"
    echo "  Run deploy_workflows.sh first."
    exit 1
fi

echo "→ Found job ID: ${JOB_ID}"

# Build parameters
PARAMS="{\"run_id\": \"${RUN_ID}\""
[[ -n "$PROCESS_DATE" ]] && PARAMS="${PARAMS}, \"process_date\": \"${PROCESS_DATE}\""
PARAMS="${PARAMS}}"

# Trigger run
echo "→ Triggering run..."
RUN_RESPONSE=$(databricks jobs run-now \
    --job-id "$JOB_ID" \
    --notebook-params "$PARAMS")

RUN_ID_ACTUAL=$(echo "$RUN_RESPONSE" | python3 -c "import json, sys; print(json.load(sys.stdin)['run_id'])")
echo "  ✓ Run triggered (Run ID: ${RUN_ID_ACTUAL})"

# Optionally wait for completion
if [[ "$WAIT_FLAG" == "true" ]]; then
    echo ""
    echo "→ Waiting for completion..."

    while true; do
        STATUS=$(databricks runs get --run-id "$RUN_ID_ACTUAL" --output JSON | \
            python3 -c "import json, sys; r=json.load(sys.stdin); print(r['state']['life_cycle_state'])")

        case "$STATUS" in
            TERMINATED)
                RESULT=$(databricks runs get --run-id "$RUN_ID_ACTUAL" --output JSON | \
                    python3 -c "import json, sys; r=json.load(sys.stdin); print(r['state']['result_state'])")
                echo ""
                if [[ "$RESULT" == "SUCCESS" ]]; then
                    echo "  ✓ Pipeline completed successfully!"
                else
                    echo "  ✗ Pipeline failed with result: ${RESULT}"
                    exit 1
                fi
                break
                ;;
            INTERNAL_ERROR)
                echo ""
                echo "  ✗ Pipeline encountered an internal error"
                exit 1
                ;;
            *)
                printf "."
                sleep 30
                ;;
        esac
    done
fi

echo ""
echo "  Monitor: databricks runs get --run-id ${RUN_ID_ACTUAL}"
echo "  UI: https://<workspace-url>/#job/${JOB_ID}/run/${RUN_ID_ACTUAL}"

