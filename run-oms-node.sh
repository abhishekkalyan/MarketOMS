#!/usr/bin/env bash
# run-oms-node.sh — starts a single OmsNode (Aeron Cluster node 0) for local development.
# Safe on macOS: uses /tmp instead of /dev/shm.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Configuration ──────────────────────────────────────────────────────────────
export OMS_NODE_ID="${OMS_NODE_ID:-0}"
export OMS_AERON_DIR="${OMS_AERON_DIR:-/tmp/oms-aeron-${OMS_NODE_ID}}"
export OMS_ARCHIVE_DIR="${OMS_ARCHIVE_DIR:-/tmp/oms-archive-${OMS_NODE_ID}}"
export OMS_MAX_NOTIONAL="${OMS_MAX_NOTIONAL:-10000000}"
export OMS_SYMBOLS="${OMS_SYMBOLS:-AAPL,MSFT,GOOG,AMZN}"

# Single-node local cluster: only node 0, all ports on localhost.
# Format: nodeId,hostname:clientPort:memberPort:logPort:transferPort:archivePort
export OMS_CLUSTER_MEMBERS="${OMS_CLUSTER_MEMBERS:-0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010}"

echo "================================================================"
echo "  OMS Node ${OMS_NODE_ID}"
echo "  Aeron dir  : ${OMS_AERON_DIR}"
echo "  Archive dir: ${OMS_ARCHIVE_DIR}"
echo "  Symbols    : ${OMS_SYMBOLS}"
echo "  Cluster    : ${OMS_CLUSTER_MEMBERS}"
echo "================================================================"

# Clean Aeron shared memory from a previous run so the MediaDriver starts fresh.
rm -rf "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}"
mkdir -p "${OMS_AERON_DIR}" "${OMS_ARCHIVE_DIR}"

cd "${SCRIPT_DIR}"

# Build a fat jar first, then run directly via java so env vars are visible.
./gradlew :oms-core:installDist --quiet

exec oms-core/build/install/oms-core/bin/oms-core
