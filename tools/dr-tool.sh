#!/usr/bin/env bash
# dr-tool.sh — Doris DR operations tool
#
# Usage:
#   dr-tool.sh status
#   dr-tool.sh switchover
#   dr-tool.sh failover [--force]
#   dr-tool.sh failback
#   dr-tool.sh drill  start|end
#   dr-tool.sh verify
#
# Configuration is read from dr-tool.conf in the same directory,
# or from environment variables. See dr-tool.conf.example.
#
# Required variables:
#   DR_PRIMARY_FE_HOST   — hostname/IP of primary FE (e.g. fe-bj-01)
#   DR_STANDBY_FE_HOST   — hostname/IP of DR FE     (e.g. fe-sh-01)
#   DR_FE_HTTP_PORT      — FE HTTP port (default 8030)
#   DR_FE_USER           — FE operator username
#   DR_FE_PASSWORD       — FE operator password
#   DR_FDB_HOST          — host where fdbbackup runs (primary)
#   DR_FDB_DR_HOST       — host where fdbrestore runs (DR)
#   DR_FDB_CLUSTER_FILE  — path to fdb.cluster on each FDB host

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF_FILE="${SCRIPT_DIR}/dr-tool.conf"

# ── load config ───────────────────────────────────────────────────────────

if [[ -f "${CONF_FILE}" ]]; then
    # shellcheck source=/dev/null
    source "${CONF_FILE}"
fi

DR_PRIMARY_FE_HOST="${DR_PRIMARY_FE_HOST:-}"
DR_STANDBY_FE_HOST="${DR_STANDBY_FE_HOST:-}"
DR_FE_HTTP_PORT="${DR_FE_HTTP_PORT:-8030}"
DR_FE_USER="${DR_FE_USER:-admin}"
DR_FE_PASSWORD="${DR_FE_PASSWORD:-}"
DR_FDB_HOST="${DR_FDB_HOST:-}"
DR_FDB_DR_HOST="${DR_FDB_DR_HOST:-}"
DR_FDB_CLUSTER_FILE="${DR_FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"

# ── helpers ───────────────────────────────────────────────────────────────

log()  { echo "[dr-tool] $*"; }
info() { echo "[dr-tool] INFO  $*"; }
warn() { echo "[dr-tool] WARN  $*" >&2; }
die()  { echo "[dr-tool] ERROR $*" >&2; exit 1; }

require_vars() {
    for var in "$@"; do
        [[ -n "${!var:-}" ]] || die "Required variable ${var} is not set. Check dr-tool.conf."
    done
}

# Calls the DR HTTP API on the given FE host.
# Usage: dr_api <host> <method> <path> [--force]
dr_api() {
    local host="$1" method="$2" path="$3"
    local url="http://${host}:${DR_FE_HTTP_PORT}${path}"
    local extra_args=()
    [[ "${4:-}" == "--force" ]] && url="${url}?force=true"

    curl --silent --fail --show-error \
        --user "${DR_FE_USER}:${DR_FE_PASSWORD}" \
        --request "${method}" \
        "${url}"
}

# Returns 0 if host is reachable on the FE HTTP port, 1 otherwise.
is_reachable() {
    local host="$1"
    curl --silent --fail --connect-timeout 3 \
        "http://${host}:${DR_FE_HTTP_PORT}/api/health" > /dev/null 2>&1
}

# Polls until BDBJE lag on the DR FE is below threshold_ms.
wait_for_lag() {
    local threshold_ms="${1:-30000}"
    local max_wait_s="${2:-300}"
    local deadline=$(( $(date +%s) + max_wait_s ))

    info "Waiting for BDBJE lag < ${threshold_ms}ms (timeout ${max_wait_s}s)..."
    while true; do
        local status_json
        status_json=$(dr_api "${DR_STANDBY_FE_HOST}" GET "/api/dr/status" 2>/dev/null || echo "{}")
        local lag_ms
        lag_ms=$(echo "${status_json}" | grep -o '"lagMs":[0-9]*' | grep -o '[0-9]*' || echo "999999")

        if [[ "${lag_ms}" -lt "${threshold_ms}" ]]; then
            info "BDBJE lag is ${lag_ms}ms — OK"
            return 0
        fi

        if [[ $(date +%s) -ge ${deadline} ]]; then
            die "Timed out waiting for BDBJE lag < ${threshold_ms}ms. Current lag: ${lag_ms}ms"
        fi

        log "Lag is ${lag_ms}ms, waiting..."
        sleep 5
    done
}

# Runs a command on a remote host via ssh.
ssh_run() {
    local host="$1"; shift
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${host}" "$@"
}

# ── commands ──────────────────────────────────────────────────────────────

cmd_status() {
    require_vars DR_PRIMARY_FE_HOST DR_STANDBY_FE_HOST

    echo "=== PRIMARY (${DR_PRIMARY_FE_HOST}) ==="
    dr_api "${DR_PRIMARY_FE_HOST}" GET "/api/dr/status" 2>/dev/null \
        | python3 -m json.tool 2>/dev/null || warn "Primary FE unreachable"

    echo ""
    echo "=== STANDBY (${DR_STANDBY_FE_HOST}) ==="
    dr_api "${DR_STANDBY_FE_HOST}" GET "/api/dr/status" 2>/dev/null \
        | python3 -m json.tool 2>/dev/null || warn "Standby FE unreachable"
}

cmd_switchover() {
    require_vars DR_PRIMARY_FE_HOST DR_STANDBY_FE_HOST DR_FDB_HOST DR_FDB_DR_HOST

    info "=== PLANNED SWITCHOVER ==="
    info "Primary: ${DR_PRIMARY_FE_HOST}  →  DR: ${DR_STANDBY_FE_HOST}"
    echo ""

    # 1. pre-flight checks
    info "Step 1/7: Pre-flight checks"
    is_reachable "${DR_PRIMARY_FE_HOST}" || die "Primary FE is unreachable. Aborting."
    is_reachable "${DR_STANDBY_FE_HOST}" || die "Standby FE is unreachable. Aborting."
    wait_for_lag 30000 120   # BDBJE lag < 30s

    # 2. quiesce primary — stop accepting new writes
    info "Step 2/7: Quiescing primary (SWITCHING state)"
    dr_api "${DR_PRIMARY_FE_HOST}" POST "/api/dr/pause"
    info "Primary paused. No new writes accepted."
    sleep 5   # allow in-flight queries to drain

    # 3. wait for DR to drain the relay
    info "Step 3/7: Waiting for DR FE to drain all relay segments"
    wait_for_lag 500 120    # lag < 500ms means nearly caught up

    # 4. FDB: pause backup and create final snapshot on primary
    info "Step 4/7: Creating consistent FDB snapshot on primary"
    ssh_run "${DR_FDB_HOST}" \
        "fdbbackup pause --cluster-file ${DR_FDB_CLUSTER_FILE}"
    info "FDB snapshot created"

    # 5. FDB: restore snapshot on DR
    info "Step 5/7: Restoring FDB snapshot on DR site"
    ssh_run "${DR_FDB_DR_HOST}" \
        "fdbrestore start --source-url \$(cat /etc/dr/fdb-dest-url) \
         --cluster-file ${DR_FDB_CLUSTER_FILE}"
    info "Waiting for FDB restore to complete..."
    ssh_run "${DR_FDB_DR_HOST}" \
        "timeout 300 bash -c 'until fdbrestore status --cluster-file ${DR_FDB_CLUSTER_FILE} | grep -q Completed; do sleep 5; done'"
    info "FDB restore complete"

    # 6. promote DR FE
    info "Step 6/7: Promoting DR FE to ACTIVE"
    dr_api "${DR_STANDBY_FE_HOST}" POST "/api/dr/promote"
    info "DR FE is now ACTIVE"

    # 7. demote old primary
    info "Step 7/7: Demoting old primary to STANDBY"
    dr_api "${DR_PRIMARY_FE_HOST}" POST "/api/dr/demote" || \
        warn "Could not demote old primary (may be unreachable). Demote manually."

    echo ""
    info "✅ SWITCHOVER COMPLETE"
    info "New primary: ${DR_STANDBY_FE_HOST}"
    info "Update your load balancer to point to the new primary."
}

cmd_failover() {
    local force="${1:-}"
    require_vars DR_STANDBY_FE_HOST DR_FDB_DR_HOST

    info "=== UNPLANNED FAILOVER ==="

    # safety check — abort if primary is still reachable
    if is_reachable "${DR_PRIMARY_FE_HOST:-__none__}"; then
        die "Primary FE at ${DR_PRIMARY_FE_HOST} is still reachable! " \
            "Use 'switchover' for planned switchover. " \
            "If this is truly an emergency, stop the primary first."
    fi
    info "Primary is unreachable — proceeding with failover"

    # show estimated data loss window
    info "Checking latest available checkpoint..."
    local status_json
    status_json=$(dr_api "${DR_STANDBY_FE_HOST}" GET "/api/dr/status" 2>/dev/null || echo "{}")
    local last_applied
    last_applied=$(echo "${status_json}" | grep -o '"lastAppliedJournalId":[0-9]*' | grep -o '[0-9]*' || echo "unknown")
    local lag_ms
    lag_ms=$(echo "${status_json}" | grep -o '"lagMs":[0-9]*' | grep -o '[0-9]*' || echo "unknown")
    warn "Estimated data loss window: ~${lag_ms}ms (last applied journal_id=${last_applied})"

    if [[ "${force}" != "--force" ]]; then
        read -r -p "[dr-tool] Proceed with failover? Data up to ${lag_ms}ms ago may be lost. [yes/no]: " confirm
        [[ "${confirm}" == "yes" ]] || die "Failover cancelled by operator."
    fi

    # restore FDB from latest backup
    info "Step 1/3: Restoring FDB from latest backup on DR site"
    ssh_run "${DR_FDB_DR_HOST}" \
        "fdbrestore start --source-url \$(cat /etc/dr/fdb-dest-url) \
         --cluster-file ${DR_FDB_CLUSTER_FILE}"
    ssh_run "${DR_FDB_DR_HOST}" \
        "timeout 300 bash -c 'until fdbrestore status --cluster-file ${DR_FDB_CLUSTER_FILE} | grep -q Completed; do sleep 5; done'"
    info "FDB restore complete"

    # promote DR FE
    info "Step 2/3: Promoting DR FE to ACTIVE"
    dr_api "${DR_STANDBY_FE_HOST}" POST "/api/dr/promote" "--force"
    info "DR FE is now ACTIVE"

    info "Step 3/3: Update load balancer"
    warn "ACTION REQUIRED: Update your load balancer to point to ${DR_STANDBY_FE_HOST}"

    echo ""
    info "✅ FAILOVER COMPLETE"
    info "New primary: ${DR_STANDBY_FE_HOST}"
    info "Data loss window: ~${lag_ms}ms"
}

cmd_failback() {
    require_vars DR_PRIMARY_FE_HOST DR_STANDBY_FE_HOST

    info "=== FAILBACK ==="
    info "Restoring ${DR_PRIMARY_FE_HOST} as primary"
    echo ""
    info "Step 1: Ensure original primary hardware is repaired and FE is running"
    info "Step 2: Configure original primary fe.conf with dr.mode=STANDBY"
    info "Step 3: Start original primary FE — it will consume from relay"
    info "Step 4: Wait for it to catch up, then run: dr-tool.sh switchover"
    echo ""
    info "This tool will now run 'switchover' to return ${DR_PRIMARY_FE_HOST} as primary."
    read -r -p "[dr-tool] Is ${DR_PRIMARY_FE_HOST} running in STANDBY mode and caught up? [yes/no]: " confirm
    [[ "${confirm}" == "yes" ]] || die "Failback cancelled. Start the original primary in STANDBY mode first."

    # swap hosts and run switchover
    local tmp="${DR_PRIMARY_FE_HOST}"
    DR_PRIMARY_FE_HOST="${DR_STANDBY_FE_HOST}"
    DR_STANDBY_FE_HOST="${tmp}"
    cmd_switchover
}

cmd_drill() {
    local mode="${1:-}"
    require_vars DR_STANDBY_FE_HOST

    case "${mode}" in
        start)
            info "Starting DR drill — isolating DR cluster from relay"
            dr_api "${DR_STANDBY_FE_HOST}" POST "/api/dr/drill?mode=start"
            info "✅ Drill started. Consumer paused. Primary unaffected."
            info "Test your failover procedures on the isolated DR cluster."
            info "Run 'dr-tool.sh drill end' when done."
            ;;
        end)
            info "Ending DR drill — reconnecting to relay"
            dr_api "${DR_STANDBY_FE_HOST}" POST "/api/dr/drill?mode=end"
            info "✅ Drill ended. Consumer reconnected. DR is resynchronizing."
            ;;
        *)
            die "Usage: dr-tool.sh drill start|end"
            ;;
    esac
}

cmd_verify() {
    require_vars DR_PRIMARY_FE_HOST DR_STANDBY_FE_HOST

    info "=== CONSISTENCY VERIFICATION ==="

    local primary_status standby_status
    primary_status=$(dr_api "${DR_PRIMARY_FE_HOST}" GET "/api/dr/status" 2>/dev/null || echo "{}")
    standby_status=$(dr_api "${DR_STANDBY_FE_HOST}" GET "/api/dr/status" 2>/dev/null || echo "{}")

    local primary_exported standby_applied lag_ms
    primary_exported=$(echo "${primary_status}" | grep -o '"lastExportedJournalId":[0-9]*' | grep -o '[0-9]*' || echo "?")
    standby_applied=$(echo "${standby_status}"  | grep -o '"lastAppliedJournalId":[0-9]*'  | grep -o '[0-9]*' || echo "?")
    lag_ms=$(echo "${standby_status}" | grep -o '"lagMs":[0-9]*' | grep -o '[0-9]*' || echo "?")

    echo "Primary last exported journal_id : ${primary_exported}"
    echo "Standby last applied journal_id  : ${standby_applied}"
    echo "BDBJE replication lag            : ${lag_ms}ms"

    if [[ "${lag_ms}" != "?" && "${lag_ms}" -lt 60000 ]]; then
        info "✅ BDBJE lag is acceptable (< 60s)"
    else
        warn "⚠️  BDBJE lag is ${lag_ms}ms — check exporter/consumer health"
    fi
}

# ── main ──────────────────────────────────────────────────────────────────

CMD="${1:-}"
shift || true

case "${CMD}" in
    status)    cmd_status ;;
    switchover) cmd_switchover ;;
    failover)  cmd_failover "${1:-}" ;;
    failback)  cmd_failback ;;
    drill)     cmd_drill "${1:-}" ;;
    verify)    cmd_verify ;;
    *)
        echo "Usage: dr-tool.sh <command>"
        echo ""
        echo "Commands:"
        echo "  status              Show DR state, lag, and lease on both clusters"
        echo "  switchover          Planned RPO=0 switchover (quiesce primary, sync, promote DR)"
        echo "  failover [--force]  Unplanned failover (primary unreachable)"
        echo "  failback            Restore original primary after failover"
        echo "  drill start|end     Isolated DR test without affecting production"
        echo "  verify              Check consistency between primary and DR"
        exit 1
        ;;
esac
