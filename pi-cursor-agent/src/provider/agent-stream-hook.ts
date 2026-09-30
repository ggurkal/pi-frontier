import type { ToolExecRequest } from "../bridge/cursor-to-pi/tool-bridge";
import type { SteerDispatcher } from "./pending-messages";

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

  get isDone(): boolean {
    return this.done;
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

/**
 * One Cursor run that outlives a single Pi `streamSimple` call. Pi reattaches
 * to it after a tool batch.
 */
export interface LiveSession {
  channel: LiveEventChannel;
  cursorRunPromise: Promise<void>;
  /** Persist the store and record a snapshot entry in Pi, unless `isCurrent` turns false first. */
  flushSessionState: (isCurrent?: () => boolean) => Promise<void>;
  abort: (reason?: string) => void;
  startTime: number;
  firstTokenTime?: number;
  steers: SteerDispatcher;
  /** Keys of Pi context user messages this run already accounts for. */
  seenUserMessageKeys: Set<string>;
  /** Whether the latest checkpoint covers all output the run has produced. */
  hasCurrentCheckpoint: () => boolean;
  /** Record a conversation change the latest checkpoint may not include. */
  markCheckpointStale: () => void;
  /**
   * Terminate the run when `signal` (Pi's abort signal) aborts, for the rest
   * of the run's life, including while Pi runs a tool batch and no stream
   * observes the signal.
   */
  linkAbort: (signal: AbortSignal | undefined) => void;
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
