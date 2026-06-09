import type { ToolExecRequest } from "../bridge/cursor-to-pi/tool-bridge";

export type StreamingBehavior = "steer" | "followUp";

export function toStreamingBehavior(
  value: unknown,
): StreamingBehavior | undefined {
  return value === "steer" || value === "followUp" ? value : undefined;
}

export type ChannelEvent =
  | { kind: "content"; data: ContentEvent }
  | { kind: "tool-exec-request"; request: ToolExecRequest }
  | { kind: "token-delta"; tokens: number }
  | { kind: "cursor-done" }
  | { kind: "cursor-error"; error: unknown };

export interface ContentEvent {
  kind: "thinking-delta" | "text-delta" | "thinking-completed";
  text: string;
}

export class LiveEventChannel {
  readonly sessionId: string;
  private events: ChannelEvent[] = [];
  private cursor = 0;
  private done = false;
  private waiters: Array<() => void> = [];

  constructor(sessionId: string) {
    this.sessionId = sessionId;
  }

  push(event: ChannelEvent): void {
    this.events.push(event);
    this.notifyWaiters();
  }

  markDone(): void {
    this.done = true;
    this.notifyWaiters();
  }

  async next(): Promise<ChannelEvent | null> {
    while (this.cursor >= this.events.length) {
      if (this.done) return null;
      await new Promise<void>((r) => this.waiters.push(r));
    }
    return this.events[this.cursor++] || null;
  }

  private notifyWaiters(): void {
    const w = this.waiters;
    this.waiters = [];
    for (const resolve of w) resolve();
  }
}

export interface LiveSession {
  channel: LiveEventChannel;
  cursorRunPromise: Promise<void>;
  flushSessionState: () => Promise<void>;
  abort: (reason?: string) => void;
  startTime: number;
  firstTokenTime?: number;
  /**
   * Send a steering message - interrupts current generation and injects message.
   * Equivalent to pressing Enter while streaming in Pi TUI.
   */
  steer: (text: string) => Promise<void>;
  /**
   * Send a follow-up message - queues until current generation completes.
   * Equivalent to pressing Alt+Enter while streaming in Pi TUI.
   */
  followUp: (text: string) => Promise<void>;
  /**
   * Internal signal used to detect request-signal aborts that are side effects
   * of a steer handoff, so we can avoid surfacing them as hard errors.
   */
  wasRecentSteerAttempt?: () => boolean;
  /**
   * Internal monotonic steer counter used for deterministic abort attribution.
   */
  getSteerEpoch?: () => number;
  /**
   * Internal signal used to mark steer intent before async dispatch begins.
   */
  markSteerIntent?: () => void;
}

let liveSessions = new Map<string, LiveSession>();

interface QueuedInputIntent {
  id: string;
  text: string;
  mode: StreamingBehavior;
  createdAt: number;
}

let queuedInputIntents = new Map<string, QueuedInputIntent[]>();
let seenContextUserMessageKeys = new Map<string, Set<string>>();

export function setLiveSession(sessionId: string, session: LiveSession): void {
  liveSessions.set(sessionId, session);
}

export function getLiveSession(sessionId: string): LiveSession | undefined {
  return liveSessions.get(sessionId);
}

export function deleteLiveSession(sessionId: string): void {
  liveSessions.delete(sessionId);
  queuedInputIntents.delete(sessionId);
  seenContextUserMessageKeys.delete(sessionId);
}

export function retainOnlyLiveSession(sessionId: string | null): void {
  const retained = sessionId ? liveSessions.get(sessionId) : undefined;
  for (const [id, session] of liveSessions) {
    if (id !== sessionId) {
      session.abort("Session ended");
    }
  }
  liveSessions =
    sessionId && retained ? new Map([[sessionId, retained]]) : new Map();
  const retainedIntents = sessionId
    ? queuedInputIntents.get(sessionId)
    : undefined;
  queuedInputIntents =
    sessionId && retainedIntents
      ? new Map([[sessionId, retainedIntents]])
      : new Map();
  const retainedSeenKeys = sessionId
    ? seenContextUserMessageKeys.get(sessionId)
    : undefined;
  seenContextUserMessageKeys =
    sessionId && retainedSeenKeys
      ? new Map([[sessionId, retainedSeenKeys]])
      : new Map();
}

export function queueInputIntent(
  sessionId: string,
  text: string,
  mode: StreamingBehavior,
): string {
  const queue = queuedInputIntents.get(sessionId) ?? [];
  const id = crypto.randomUUID();
  queue.push({ id, text, mode, createdAt: Date.now() });
  if (queue.length > 100) {
    queue.splice(0, queue.length - 100);
  }
  queuedInputIntents.set(sessionId, queue);
  return id;
}

export function consumeInputIntentForText(
  sessionId: string,
  text: string,
): StreamingBehavior | undefined {
  const queue = queuedInputIntents.get(sessionId);
  if (!queue || queue.length === 0) {
    return undefined;
  }
  const exactIdx = queue.findIndex((intent) => intent.text === text);
  const idx = exactIdx >= 0 ? exactIdx : 0;
  const [intent] = queue.splice(idx, 1);
  if (queue.length === 0) {
    queuedInputIntents.delete(sessionId);
  } else {
    queuedInputIntents.set(sessionId, queue);
  }
  return intent?.mode;
}

export function markSeenContextUserMessageKey(
  sessionId: string,
  key: string,
): void {
  const seen = seenContextUserMessageKeys.get(sessionId) ?? new Set<string>();
  seen.add(key);
  seenContextUserMessageKeys.set(sessionId, seen);
}

export function hasSeenContextUserMessageKey(
  sessionId: string,
  key: string,
): boolean {
  return seenContextUserMessageKeys.get(sessionId)?.has(key) ?? false;
}
