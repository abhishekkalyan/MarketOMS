#!/usr/bin/env bash
# run-perf.sh — builds all modules, starts OmsNode + AlgoSorAgent, runs the performance
# harness (PerfHarnessLauncher), prints a consolidated results table, then shuts down.
#
# Exit code: 0 if all benchmarks pass, 1 if any fail or OmsNode fails to start.
#
# Usage:
#   ./run-perf.sh                    # default: single-node localhost
#   HARNESS_CLUSTER_INGRESS=0=host:9000 ./run-perf.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# ── Configuration ──────────────────────────────────────────────────────────────
export OMS_NODE_ID=0
export OMS_AERON_DIR="/tmp/oms-aeron-0"
export OMS_ARCHIVE_DIR="/tmp/oms-archive-0"
export OMS_MAX_NOTIONAL=10000000
export OMS_ARCHIVE_DELETE_ON_START=true
export OMS_SYMBOLS="AAPL,MSFT,GOOG,AMZN"
export OMS_CLUSTER_MEMBERS="0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010"

export HARNESS_AERON_DIR="/tmp/oms-aeron-perf"
export HARNESS_CLUSTER_INGRESS="${HARNESS_CLUSTER_INGRESS:-0=localhost:9000}"
LAUNCHER_AERON_DIR="/tmp/oms-aeron-launcher"

# Performance harness configuration — override via env vars if needed
export PERF_SAMPLE_COUNT="${PERF_SAMPLE_COUNT:-10000}"
export PERF_SEND_INTERVAL_NS="${PERF_SEND_INTERVAL_NS:-100000}"
export PERF_P99_LIMIT_US="${PERF_P99_LIMIT_US:-100000}"
export PERF_ASSERT_ZERO_GC="${PERF_ASSERT_ZERO_GC:-false}"
export PERF_MAX_OUTSTANDING="${PERF_MAX_OUTSTANDING:-5000}"
export PERF_THROUGHPUT_TIMEOUT_MS="${PERF_THROUGHPUT_TIMEOUT_MS:-5000}"
export PERF_DURATION_SECONDS="${PERF_DURATION_SECONDS:-5}"
export PERF_SUSTAINED_DURATION_S="${PERF_SUSTAINED_DURATION_S:-10}"
export PERF_PARENT_ORDER_COUNT="${PERF_PARENT_ORDER_COUNT:-100}"
export PERF_SNAPSHOT_RESUME_LIMIT_MS="${PERF_SNAPSHOT_RESUME_LIMIT_MS:-60000}"

NODE_LOG="/tmp/oms-node-perf.log"
LAUNCHER_LOG="/tmp/oms-launcher-perf.log"

OMS_NODE_PID=""
OMS_LAUNCHER_PID=""
PERF_EXIT=1

# Derive cluster port list from OMS_CLUSTER_MEMBERS once, for use in cleanup.
# Format: nodeId,host:port,host:port,... — extract every field that contains ':'
IFS=',' read -ra _MF <<< "${OMS_CLUSTER_MEMBERS}"
CLUSTER_PORTS=()
for _f in "${_MF[@]}"; do
    [[ "${_f}" == *:* ]] && CLUSTER_PORTS+=("${_f##*:}")
done
unset _MF _f

# ── Cleanup ────────────────────────────────────────────────────────────────────
_CLEANED=false
cleanup() {
    [[ "${_CLEANED}" == "true" ]] && return
    _CLEANED=true
    echo ""
    echo "Stopping OMS stack..."

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
            # shellcheck disable=SC2086
            kill -9 ${_holders} 2>/dev/null || true
        fi
    done

    # Phase 4 — remove all Aeron / archive directories this script created
    rm -rf "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}" "${HARNESS_AERON_DIR}" "${LAUNCHER_AERON_DIR}"

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

    echo "Stack stopped. Perf harness exit code: ${PERF_EXIT}"
}
trap cleanup EXIT INT TERM

# ── 1. Build ───────────────────────────────────────────────────────────────────
echo "Building all modules..."
./gradlew :oms-core:installDist :oms-launcher:installDist :oms-harness:installDist --quiet --no-daemon
echo "Build OK."

# ── 2. Clean Aeron dirs ────────────────────────────────────────────────────────
rm -rf "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}" "${HARNESS_AERON_DIR}" "${LAUNCHER_AERON_DIR}"
mkdir -p "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}" "${HARNESS_AERON_DIR}" "${LAUNCHER_AERON_DIR}"

# ── 3. Start OmsNode ──────────────────────────────────────────────────────────
echo "Starting OmsNode (node ${OMS_NODE_ID})... logging to ${NODE_LOG}"
oms-core/build/install/oms-core/bin/oms-core > "${NODE_LOG}" 2>&1 &
OMS_NODE_PID=$!
echo "  OmsNode PID: ${OMS_NODE_PID}"

# ── 4. Wait for MediaDriver to be ready ───────────────────────────────────────
echo -n "  Waiting for OmsNode MediaDriver"
WAITED=0
until find "${OMS_AERON_DIR}" -name "cnc.dat" 2>/dev/null | grep -q cnc.dat; do
    sleep 1; WAITED=$((WAITED + 1)); echo -n "."
    if [[ ${WAITED} -ge 60 ]]; then
        echo ""; echo "ERROR: OmsNode did not start in 60s. See ${NODE_LOG}"; tail -30 "${NODE_LOG}"; exit 1
    fi
    if ! kill -0 "${OMS_NODE_PID}" 2>/dev/null; then
        echo ""; echo "ERROR: OmsNode exited. See ${NODE_LOG}"; tail -30 "${NODE_LOG}"; exit 1
    fi
done
echo " ready."

# ── 4b. Wait for cluster ingress to accept connections (port 9000) ─────────────
# Aeron Cluster's startup canvass phase (default 60s) must complete before the
# ConsensusModule binds the cluster ingress. Poll until port 9000 is active.
echo -n "  Waiting for cluster ingress (port 9000)"
WAITED=0
until grep -q "New leadership term" "${NODE_LOG}" 2>/dev/null; do
    sleep 1; WAITED=$((WAITED + 1)); echo -n "."
    if [[ ${WAITED} -ge 60 ]]; then
        echo ""; echo "ERROR: Cluster never elected leader in 60s. See ${NODE_LOG}"; tail -20 "${NODE_LOG}"; exit 1
    fi
    if ! kill -0 "${OMS_NODE_PID}" 2>/dev/null; then
        echo ""; echo "ERROR: OmsNode exited. See ${NODE_LOG}"; tail -20 "${NODE_LOG}"; exit 1
    fi
done
echo " ready."

# ── 5. Start AlgoSorAgent ─────────────────────────────────────────────────────
echo "Starting AlgoSorAgent... logging to ${LAUNCHER_LOG}"
OMS_AERON_DIR="${LAUNCHER_AERON_DIR}" \
    oms-launcher/build/install/oms-launcher/bin/oms-launcher \
    > "${LAUNCHER_LOG}" 2>&1 &
OMS_LAUNCHER_PID=$!
echo "  AlgoSorAgent PID: ${OMS_LAUNCHER_PID}"

# Wait until AlgoSorAgent's AgentRunner is confirmed started before running benchmarks
echo -n "  Waiting for AlgoSorAgent"
WAITED=0
until grep -q "AlgoSorAgent started on dedicated thread" "${LAUNCHER_LOG}" 2>/dev/null; do
    sleep 1; WAITED=$((WAITED + 1)); echo -n "."
    if [[ ${WAITED} -ge 30 ]]; then
        echo ""; echo "ERROR: AlgoSorAgent did not start in 30s. See ${LAUNCHER_LOG}"; tail -20 "${LAUNCHER_LOG}"; exit 1
    fi
    if ! kill -0 "${OMS_LAUNCHER_PID}" 2>/dev/null; then
        echo ""; echo "ERROR: AlgoSorAgent exited. See ${LAUNCHER_LOG}"; tail -20 "${LAUNCHER_LOG}"; exit 1
    fi
done
echo " ready."

# ── 6. Run the performance harness ────────────────────────────────────────────
echo ""
echo "Running performance harness..."
echo "================================================================"
oms-harness/build/install/oms-harness/bin/oms-perf-harness
PERF_EXIT=$?

echo "================================================================"
if [[ ${PERF_EXIT} -eq 0 ]]; then
    echo "ALL BENCHMARKS PASSED"
else
    echo "ONE OR MORE BENCHMARKS FAILED — check output above"
    echo "Node log    : ${NODE_LOG}"
    echo "Launcher log: ${LAUNCHER_LOG}"
fi

exit "${PERF_EXIT}"
