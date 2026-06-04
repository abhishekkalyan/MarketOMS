#!/usr/bin/env bash
# run-harness.sh — builds all modules, starts OmsNode, runs the test harness, shuts down.
#
# Exit code: 0 if all scenarios pass, 1 if any fail or OmsNode fails to start.
#
# Usage:
#   ./run-harness.sh                    # default: single-node localhost
#   HARNESS_CLUSTER_INGRESS=host:9000 ./run-harness.sh   # remote node
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# ── Configuration ──────────────────────────────────────────────────────────────
export OMS_NODE_ID=0
export OMS_AERON_DIR="/tmp/oms-aeron-0"
export OMS_ARCHIVE_DIR="/tmp/oms-archive-0"
export OMS_MAX_NOTIONAL=10000000
# Wipe archive and cluster state on each restart so the harness always starts clean.
export OMS_ARCHIVE_DELETE_ON_START=true
export OMS_SYMBOLS="AAPL,MSFT,GOOG,AMZN"
export OMS_CLUSTER_MEMBERS="0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010"

export HARNESS_AERON_DIR="/tmp/oms-aeron-harness"
export HARNESS_CLUSTER_INGRESS="${HARNESS_CLUSTER_INGRESS:-0=localhost:9000}"
LAUNCHER_AERON_DIR="/tmp/oms-aeron-launcher"

NODE_LOG="/tmp/oms-node-harness.log"
LAUNCHER_LOG="/tmp/oms-launcher-harness.log"

OMS_NODE_PID=""
OMS_LAUNCHER_PID=""
HARNESS_PID=""
HARNESS_EXIT=1

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
    for _pid in "${HARNESS_PID}" "${OMS_LAUNCHER_PID}" "${OMS_NODE_PID}"; do
        if [[ -n "${_pid}" ]] && kill -0 "${_pid}" 2>/dev/null; then
            kill "${_pid}" 2>/dev/null && _sent_term=true || true
        fi
    done

    # Phase 2 — give them 2 s to exit, then SIGKILL survivors
    if [[ "${_sent_term}" == "true" ]]; then
        sleep 2
    fi
    for _pid in "${HARNESS_PID}" "${OMS_LAUNCHER_PID}" "${OMS_NODE_PID}"; do
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
    for _pid in "${HARNESS_PID}" "${OMS_LAUNCHER_PID}" "${OMS_NODE_PID}"; do
        [[ -n "${_pid}" ]] && wait "${_pid}" 2>/dev/null || true
    done

    echo "Stack stopped. Harness exit code: ${HARNESS_EXIT}"
}
trap cleanup EXIT INT TERM

# ── 1. Build ───────────────────────────────────────────────────────────────────
echo "Building all modules..."
./gradlew :oms-core:installDist :oms-launcher:installDist :oms-harness:installDist --quiet
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
# cnc.dat may sit directly in OMS_AERON_DIR or in an aeron-<pid> subdirectory depending on platform.
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

# Give the AlgoSorAgent a moment to connect before injecting orders
sleep 3

# ── 6. Run the test harness ───────────────────────────────────────────────────
echo ""
echo "Running test harness..."
echo "================================================================"
oms-harness/build/install/oms-harness/bin/oms-harness &
HARNESS_PID=$!
wait "${HARNESS_PID}"
HARNESS_EXIT=$?
HARNESS_PID=""   # already exited — no need to kill in cleanup

if [[ ${HARNESS_EXIT} -eq 0 ]]; then
    echo "================================================================"
    echo "ALL SCENARIOS PASSED"
else
    echo "================================================================"
    echo "ONE OR MORE SCENARIOS FAILED — check harness output above"
    echo "Node log    : ${NODE_LOG}"
    echo "Launcher log: ${LAUNCHER_LOG}"
fi

exit "${HARNESS_EXIT}"
