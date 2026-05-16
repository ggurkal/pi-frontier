import type { ToolExecRequest } from "../bridge/cursor-to-pi/tool-bridge";

export type ChannelEvent =
  | { kind: "content"; data: ContentEvent }
  | { kind: "tool-exec-request"; request: ToolExecRequest }
  | { kind: "token-delta"; tokens: number }
  | { kind: "cursor-done" };

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
}

let liveSessions = new Map<string, LiveSession>();

export function setLiveSession(sessionId: string, session: LiveSession): void {
  liveSessions.set(sessionId, session);
}

export function getLiveSession(sessionId: string): LiveSession | undefined {
  return liveSessions.get(sessionId);
}

export function deleteLiveSession(sessionId: string): void {
  liveSessions.delete(sessionId);
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
}
