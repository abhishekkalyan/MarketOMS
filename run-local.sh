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
export OMS_CLUSTER_MEMBERS="0,localhost:9000:9001:9002:0:9003"

NODE_LOG="/tmp/oms-node.log"
LAUNCHER_LOG="/tmp/oms-launcher.log"

OMS_NODE_PID=""
OMS_LAUNCHER_PID=""

# ── Cleanup on exit ────────────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "Shutting down..."
    if [[ -n "${OMS_LAUNCHER_PID}" ]]; then
        kill "${OMS_LAUNCHER_PID}" 2>/dev/null && echo "  AlgoSorAgent stopped (PID ${OMS_LAUNCHER_PID})"
    fi
    if [[ -n "${OMS_NODE_PID}" ]]; then
        kill "${OMS_NODE_PID}" 2>/dev/null && echo "  OmsNode stopped (PID ${OMS_NODE_PID})"
    fi
    wait 2>/dev/null
    echo "Shutdown complete."
}
trap cleanup EXIT INT TERM

# ── 1. Build ───────────────────────────────────────────────────────────────────
echo "Building all modules..."
./gradlew :oms-core:installDist :oms-launcher:installDist --quiet
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
until [[ -S "${OMS_AERON_DIR}/aeron-$(id -u)/cnc.dat" ]] || \
      ls "${OMS_AERON_DIR}"/aeron-*/cnc.dat 2>/dev/null | grep -q cnc.dat; do
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

# Block until a child exits or the user interrupts
wait -n "${OMS_NODE_PID}" "${OMS_LAUNCHER_PID}" 2>/dev/null || true
