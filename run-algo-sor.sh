#!/usr/bin/env bash
# run-algo-sor.sh — starts the AlgoSorAgent (OmsLauncher) for local development.
# Must be started AFTER OmsNode is running and its MediaDriver is ready.
# Safe on macOS: uses /tmp instead of /dev/shm.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Configuration — must match OmsNode's Aeron dir ────────────────────────────
export OMS_AERON_DIR="${OMS_AERON_DIR:-/tmp/oms-aeron-launcher}"

echo "================================================================"
echo "  AlgoSorAgent (OmsLauncher)"
echo "  Aeron dir: ${OMS_AERON_DIR}"
echo "  Connecting to OmsNode on Aeron IPC streams 30 (sub) / 12 (pub)"
echo "================================================================"

rm -rf "${OMS_AERON_DIR}"
mkdir -p "${OMS_AERON_DIR}"

cd "${SCRIPT_DIR}"
./gradlew :oms-launcher:installDist --quiet --no-daemon

exec oms-launcher/build/install/oms-launcher/bin/oms-launcher
