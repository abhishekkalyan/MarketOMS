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
export OMS_SYMBOLS="AAPL,MSFT,GOOG,AMZN"
export OMS_CLUSTER_MEMBERS="0,localhost:9000:9001:9002:0:9003"

export HARNESS_AERON_DIR="/tmp/oms-aeron-harness"
export HARNESS_CLUSTER_INGRESS="${HARNESS_CLUSTER_INGRESS:-localhost:9000}"

NODE_LOG="/tmp/oms-node-harness.log"
LAUNCHER_LOG="/tmp/oms-launcher-harness.log"

OMS_NODE_PID=""
OMS_LAUNCHER_PID=""
HARNESS_EXIT=1

# ── Cleanup ────────────────────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "Stopping OMS stack..."
    if [[ -n "${OMS_LAUNCHER_PID}" ]]; then
        kill "${OMS_LAUNCHER_PID}" 2>/dev/null || true
    fi
    if [[ -n "${OMS_NODE_PID}" ]]; then
        kill "${OMS_NODE_PID}" 2>/dev/null || true
    fi
    wait 2>/dev/null || true
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
until ls "${OMS_AERON_DIR}"/aeron-*/cnc.dat 2>/dev/null | grep -q cnc.dat; do
    sleep 1; WAITED=$((WAITED + 1)); echo -n "."
    if [[ ${WAITED} -ge 30 ]]; then
        echo ""; echo "ERROR: OmsNode did not start in 30s. See ${NODE_LOG}"; tail -30 "${NODE_LOG}"; exit 1
    fi
    if ! kill -0 "${OMS_NODE_PID}" 2>/dev/null; then
        echo ""; echo "ERROR: OmsNode exited. See ${NODE_LOG}"; tail -30 "${NODE_LOG}"; exit 1
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
if oms-harness/build/install/oms-harness/bin/oms-harness; then
    HARNESS_EXIT=0
    echo "================================================================"
    echo "ALL SCENARIOS PASSED"
else
    HARNESS_EXIT=1
    echo "================================================================"
    echo "ONE OR MORE SCENARIOS FAILED — check harness output above"
    echo "Node log   : ${NODE_LOG}"
    echo "Launcher log: ${LAUNCHER_LOG}"
fi

exit "${HARNESS_EXIT}"
