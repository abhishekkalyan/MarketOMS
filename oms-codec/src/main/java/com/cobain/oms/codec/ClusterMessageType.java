package com.cobain.oms.codec;

/**
 * IPC message framing constants shared between oms-core and algo-sor.
 * Kept in oms-codec so both modules can reference it without a cross-module dependency.
 *
 * Wire format for parent-order IPC messages (stream STREAM_PARENT_ORDERS):
 *   [0]  msgType : byte                       — one of the constants below
 *   [1+] payload : OrderLayout.MESSAGE_SIZE bytes — order record
 *
 * Wire format for ChildOrderIntent IPC messages (stream STREAM_CHILD_INTENTS):
 *   [0]  msgType         : byte  — CHILD_ORDER_INTENT
 *   [1]  protocolVersion : byte  — PROTOCOL_VERSION
 *   [2-7] padding        : 6 bytes (reserved)
 *   [8+] intent payload  : ChildOrderIntentFlyweight.BLOCK_LENGTH bytes
 */
public final class ClusterMessageType {

    // ── Message type constants ────────────────────────────────────────────────
    public static final byte NEW_ORDER         = 1;
    public static final byte CANCEL_ORDER      = 2;
    public static final byte REPLACE_ORDER     = 3;

    /** Routing instruction published by algo-sor → oms-core over STREAM_CHILD_INTENTS. */
    public static final byte CHILD_ORDER_INTENT = 40;

    // ── Header layout constants ───────────────────────────────────────────────
    /** Byte offset of the msgType field in all IPC messages. */
    public static final int OFFSET_MSG_TYPE = 0;

    /** Byte offset where the order payload begins (after the 1-byte type header). */
    public static final int OFFSET_PAYLOAD = 1;

    /** Protocol version written into the second byte of CHILD_ORDER_INTENT messages. */
    public static final byte PROTOCOL_VERSION = 1;

    /** Header length for CHILD_ORDER_INTENT messages: msgType + version + 6 pad bytes. */
    public static final int HEADER_LENGTH = 8;

    /** Total IPC message size for parent-order messages: 1-byte header + 128-byte order payload. */
    public static final int IPC_MESSAGE_SIZE = OFFSET_PAYLOAD + com.cobain.oms.model.OrderLayout.MESSAGE_SIZE;

    // ── Aeron IPC stream IDs ──────────────────────────────────────────────────
    /** oms-core → algo-sor: validated parent orders for routing. */
    public static final int STREAM_PARENT_ORDERS = 30;

    /** algo-sor → oms-core: ChildOrderIntent routing instructions (NEW in this architecture). */
    public static final int STREAM_CHILD_INTENTS  = 12;

    private ClusterMessageType() {}
}
