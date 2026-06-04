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

NODE_LOG="/tmp/oms-node-harness.log"
LAUNCHER_LOG="/tmp/oms-launcher-harness.log"

OMS_NODE_PID=""
OMS_LAUNCHER_PID=""
HARNESS_PID=""
HARNESS_EXIT=1

# ── Cleanup ────────────────────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "Stopping OMS stack..."
    [[ -n "${HARNESS_PID}"   ]] && kill "${HARNESS_PID}"   2>/dev/null || true
    [[ -n "${OMS_LAUNCHER_PID}" ]] && kill "${OMS_LAUNCHER_PID}" 2>/dev/null || true
    [[ -n "${OMS_NODE_PID}"  ]] && kill "${OMS_NODE_PID}"  2>/dev/null || true
    # Wait only on the PIDs we started — avoids hanging on unrelated background jobs.
    for pid in "${HARNESS_PID}" "${OMS_LAUNCHER_PID}" "${OMS_NODE_PID}"; do
        [[ -n "${pid}" ]] && wait "${pid}" 2>/dev/null || true
    done
    echo "Stack stopped. Harness exit code: ${HARNESS_EXIT}"
}
trap cleanup EXIT INT TERM

# ── 1. Build ───────────────────────────────────────────────────────────────────
echo "Building all modules..."
./gradlew :oms-core:installDist :oms-launcher:installDist :oms-harness:installDist --quiet
echo "Build OK."

# ── 2. Clean Aeron dirs ────────────────────────────────────────────────────────
rm -rf "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}" "${HARNESS_AERON_DIR}"
mkdir -p "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}" "${HARNESS_AERON_DIR}"

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
until netstat -an 2>/dev/null | grep -q "\.9000 "; do
    sleep 2; WAITED=$((WAITED + 2)); echo -n "."
    if [[ ${WAITED} -ge 90 ]]; then
        echo ""; echo "ERROR: Cluster ingress never appeared on port 9000. See ${NODE_LOG}"; tail -20 "${NODE_LOG}"; exit 1
    fi
    if ! kill -0 "${OMS_NODE_PID}" 2>/dev/null; then
        echo ""; echo "ERROR: OmsNode exited. See ${NODE_LOG}"; tail -20 "${NODE_LOG}"; exit 1
    fi
done
echo " ready."

# ── 5. Start AlgoSorAgent ─────────────────────────────────────────────────────
echo "Starting AlgoSorAgent... logging to ${LAUNCHER_LOG}"
OMS_AERON_DIR="/tmp/oms-aeron-launcher" \
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
