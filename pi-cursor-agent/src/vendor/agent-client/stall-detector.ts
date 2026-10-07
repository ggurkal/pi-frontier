import type { StallInfo } from "./retry-policy";

/** Cursor's `jmd`: no inbound message for this long is a stall. */
export const STALL_THRESHOLD_MS = 30_000;
/** Cursor's `qmd`: user-configured thresholds are raised to this. */
export const MIN_STALL_THRESHOLD_MS = 20_000;

export interface StallDetector {
  onServerSentHeartbeat(): void;
  onClientSentHeartbeat(): void;
  reset(
    activityType: "inbound_message" | "outbound_write",
    messageType: string,
  ): void;
  onStreamEnded(): void;
  setPaused(paused: boolean): void;
  dispose(): void;
}

export interface StallDetectorOptions {
  /** `<= 0` disables detection. */
  thresholdMs: number;
  onStall(info: StallInfo): void;
}

export function createNoopStallDetector(): StallDetector {
  return {
    onServerSentHeartbeat() {},
    onClientSentHeartbeat() {},
    reset() {},
    onStreamEnded() {},
    setPaused() {},
    dispose() {},
  };
}

/**
 * Like Cursor's: only inbound messages count as activity, server heartbeats
 * included. Our own writes, client heartbeats among them, never keep a
 * stream alive. Detection pauses while a human answers a query.
 */
export function createStallDetector(
  options: StallDetectorOptions,
): StallDetector {
  const { thresholdMs, onStall } = options;
  if (thresholdMs <= 0) return createNoopStallDetector();

  let lastActivityAt = Date.now();
  let lastServerHeartbeatAt: number | undefined;
  let lastClientHeartbeatAt: number | undefined;
  let lastInboundMessageType: string | undefined;
  let paused = false;
  let disposed = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const fire = () => {
    const now = Date.now();
    onStall({
      thresholdMs,
      lastActivityAgoMs: now - lastActivityAt,
      ...(lastServerHeartbeatAt !== undefined
        ? { lastServerHeartbeatAgoMs: now - lastServerHeartbeatAt }
        : {}),
      ...(lastClientHeartbeatAt !== undefined
        ? { lastClientHeartbeatAgoMs: now - lastClientHeartbeatAt }
        : {}),
      ...(lastInboundMessageType !== undefined
        ? { lastInboundMessageType }
        : {}),
    });
  };

  const check = () => {
    timer = undefined;
    if (disposed) return;
    if (paused) {
      timer = setTimeout(check, thresholdMs);
      return;
    }
    const remaining = thresholdMs - (Date.now() - lastActivityAt);
    if (remaining <= 0) {
      disposed = true;
      fire();
      return;
    }
    timer = setTimeout(check, remaining);
  };
  timer = setTimeout(check, thresholdMs);

  return {
    onServerSentHeartbeat() {
      lastServerHeartbeatAt = Date.now();
    },
    onClientSentHeartbeat() {
      lastClientHeartbeatAt = Date.now();
    },
    reset(activityType, messageType) {
      if (activityType !== "inbound_message") return;
      lastInboundMessageType = messageType;
      lastActivityAt = Date.now();
    },
    onStreamEnded() {},
    setPaused(value) {
      if (disposed || value === paused) return;
      paused = value;
      if (!paused) lastActivityAt = Date.now();
    },
    dispose() {
      disposed = true;
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
    },
  };
}
