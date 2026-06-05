#!/usr/bin/env bash
# run-local.sh — starts the full OMS stack locally (single node, macOS-safe).
#
# Process order:
#   1. Build all modules
#   2. Start OmsNode (Aeron Cluster node 0, embedded MediaDriver + Archive)
#   3. Wait until OmsNode's MediaDriver is ready (polls Aeron dir)
#   4. Start OmsLauncher (AlgoSorAgent)
#   5. On Ctrl-C: shut down both in reverse order
#
# Logs:
#   /tmp/oms-node.log     — OmsNode output
#   /tmp/oms-launcher.log — AlgoSorAgent output
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# ── Configuration ──────────────────────────────────────────────────────────────
export OMS_NODE_ID=0
export OMS_AERON_DIR="/tmp/oms-aeron-0"
export OMS_ARCHIVE_DIR="/tmp/oms-archive-0"
export OMS_LAUNCHER_AERON_DIR="/tmp/oms-aeron-launcher"
export OMS_MAX_NOTIONAL=10000000
export OMS_SYMBOLS="AAPL,MSFT,GOOG,AMZN"
export OMS_CLUSTER_MEMBERS="0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010"
export OMS_ARCHIVE_DELETE_ON_START=true

NODE_LOG="/tmp/oms-node.log"
LAUNCHER_LOG="/tmp/oms-launcher.log"

OMS_NODE_PID=""
OMS_LAUNCHER_PID=""

# Derive cluster port list from OMS_CLUSTER_MEMBERS once, for use in cleanup.
# Format: nodeId,host:port,host:port,... — extract every field that contains ':'
IFS=',' read -ra _MF <<< "${OMS_CLUSTER_MEMBERS}"
CLUSTER_PORTS=()
for _f in "${_MF[@]}"; do
    [[ "${_f}" == *:* ]] && CLUSTER_PORTS+=("${_f##*:}")
done
unset _MF _f

# ── Cleanup on exit ────────────────────────────────────────────────────────────
_CLEANED=false
cleanup() {
    [[ "${_CLEANED}" == "true" ]] && return
    _CLEANED=true
    echo ""
    echo "Shutting down..."

    # Phase 1 — SIGTERM all tracked processes
    local _sent_term=false
    for _pid in "${OMS_LAUNCHER_PID}" "${OMS_NODE_PID}"; do
        if [[ -n "${_pid}" ]] && kill -0 "${_pid}" 2>/dev/null; then
            kill "${_pid}" 2>/dev/null && _sent_term=true || true
        fi
    done

    # Phase 2 — give them 2 s to exit, then SIGKILL survivors
    if [[ "${_sent_term}" == "true" ]]; then
        sleep 2
    fi
    for _pid in "${OMS_LAUNCHER_PID}" "${OMS_NODE_PID}"; do
        if [[ -n "${_pid}" ]] && kill -0 "${_pid}" 2>/dev/null; then
            echo "  SIGKILL PID ${_pid} (did not stop after SIGTERM)"
            kill -9 "${_pid}" 2>/dev/null || true
        fi
    done

    # Phase 3 — kill any process still holding a cluster port
    local _holders
    for _port in "${CLUSTER_PORTS[@]}"; do
        _holders=$(lsof -ti :"${_port}" 2>/dev/null) || true
        if [[ -n "${_holders}" ]]; then
            echo "  Killing port-${_port} holder(s): ${_holders}"
            # word-split intentional: _holders is a newline-separated list of PIDs
            # shellcheck disable=SC2086
            kill -9 ${_holders} 2>/dev/null || true
        fi
    done

    # Phase 4 — remove all Aeron / archive directories this script created
    rm -rf "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}" "${OMS_LAUNCHER_AERON_DIR}"

    # Phase 5 — wait up to 10 s for all cluster ports to be confirmed free
    local _ports_free=false _dl=$(( SECONDS + 10 ))
    while [[ $SECONDS -lt $_dl ]]; do
        _ports_free=true
        for _port in "${CLUSTER_PORTS[@]}"; do
            if lsof -ti :"${_port}" &>/dev/null; then
                _ports_free=false
                break
            fi
        done
        [[ "${_ports_free}" == "true" ]] && break
        sleep 1
    done
    if [[ "${_ports_free}" == "false" ]]; then
        echo "WARNING: cluster ports still in use after 10 s — next run may fail"
    fi

    # Phase 6 — reap background jobs so the shell exits cleanly
    for _pid in "${OMS_LAUNCHER_PID}" "${OMS_NODE_PID}"; do
        [[ -n "${_pid}" ]] && wait "${_pid}" 2>/dev/null || true
    done

    echo "Shutdown complete."
}
trap cleanup EXIT INT TERM

# ── 1. Build ───────────────────────────────────────────────────────────────────
echo "Building all modules..."
./gradlew :oms-core:installDist :oms-launcher:installDist --quiet --no-daemon
echo "Build OK."

# ── 2. Clean stale Aeron dirs ─────────────────────────────────────────────────
rm -rf "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}" "${OMS_LAUNCHER_AERON_DIR}"
mkdir -p "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}" "${OMS_LAUNCHER_AERON_DIR}"

# ── 3. Start OmsNode ──────────────────────────────────────────────────────────
echo "Starting OmsNode (node ${OMS_NODE_ID})... logging to ${NODE_LOG}"
oms-core/build/install/oms-core/bin/oms-core \
    > "${NODE_LOG}" 2>&1 &
OMS_NODE_PID=$!
echo "  OmsNode PID: ${OMS_NODE_PID}"

# ── 4. Wait for OmsNode MediaDriver to be ready ───────────────────────────────
echo -n "  Waiting for Aeron MediaDriver"
WAIT_SECONDS=0
until find "${OMS_AERON_DIR}" -name "cnc.dat" 2>/dev/null | grep -q cnc.dat; do
    sleep 1
    WAIT_SECONDS=$((WAIT_SECONDS + 1))
    echo -n "."
    if [[ ${WAIT_SECONDS} -ge 30 ]]; then
        echo ""
        echo "ERROR: OmsNode did not start within 30s. Check ${NODE_LOG}"
        exit 1
    fi
    # Fail fast if OmsNode died
    if ! kill -0 "${OMS_NODE_PID}" 2>/dev/null; then
        echo ""
        echo "ERROR: OmsNode exited unexpectedly. Check ${NODE_LOG}"
        tail -20 "${NODE_LOG}"
        exit 1
    fi
done
echo " ready."

# ── 5. Start AlgoSorAgent ─────────────────────────────────────────────────────
echo "Starting AlgoSorAgent... logging to ${LAUNCHER_LOG}"
OMS_AERON_DIR="${OMS_LAUNCHER_AERON_DIR}" \
    oms-launcher/build/install/oms-launcher/bin/oms-launcher \
    > "${LAUNCHER_LOG}" 2>&1 &
OMS_LAUNCHER_PID=$!
echo "  AlgoSorAgent PID: ${OMS_LAUNCHER_PID}"

echo ""
echo "================================================================"
echo "  OMS stack is running."
echo "  Node log    : tail -f ${NODE_LOG}"
echo "  Launcher log: tail -f ${LAUNCHER_LOG}"
echo "  Press Ctrl-C to stop."
echo "================================================================"
echo ""

# Block until a child exits or the user interrupts.
# 'wait -n' requires bash 4.3+; macOS ships bash 3.2, so poll instead.
while kill -0 "${OMS_NODE_PID}" 2>/dev/null && kill -0 "${OMS_LAUNCHER_PID}" 2>/dev/null; do
    sleep 2
done
