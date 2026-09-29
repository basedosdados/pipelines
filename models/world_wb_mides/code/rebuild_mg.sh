#!/bin/zsh
# Rebuild every world_wb_mides table that MG feeds, in cost order.
#
# ORDER IS DELIBERATE. The BigQuery daily quota (`QueryUsagePerDay`) is a hard
# byte ceiling that resets at midnight Pacific. If it trips mid-way, everything
# already built persists -- so the tables that matter most go first:
#
#   1. spend      -- empenho / liquidacao / pagamento. These are PUBLISHED tables
#                    and the dev copies currently hold the pre-UTF-8-fix build.
#                    Their four staging mirrors are complete, so they are
#                    unblocked regardless of anything else.
#   2. mg         -- the 43 MG-only tables (~250 GiB).
#   3. procurement-- licitacao / licitacao_item / licitacao_participante, which
#                    depend on the MG state models being correct but are cheap.
#
# A model that reports ERROR on a quota trip may still have BUILT -- BigQuery
# fails dbt's post-step after the CREATE TABLE succeeded. Check `last_modified`
# and the column TYPES before re-running anything; see the session notes.
set -u
cd "$(dirname "$0")/../../.."
export BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/staging.json"
LOG=~/Downloads/world_wb_mides_data

run() {
  local name=$1; shift
  echo "=== $name  $(date '+%H:%M:%S') ==="
  uv run dbt run --select "$@" --threads 6 --target-path /tmp/dbt_rebuild 2>&1 \
    | grep -vE "RequestsDependency|warnings.warn|BigQuery adapter: http|WARNING" \
    | tee -a "$LOG/rebuild_$name.log" \
    | grep -E "OK created|ERROR creating|Completed with|Done\."
}

run spend       world_wb_mides__empenho world_wb_mides__liquidacao world_wb_mides__pagamento
run mg          "path:models/world_wb_mides/mg"
run procurement world_wb_mides__licitacao world_wb_mides__licitacao_item world_wb_mides__licitacao_participante
echo "=== rebuild finished $(date '+%H:%M:%S') ==="
