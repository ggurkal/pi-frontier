import { setTimeout } from "node:timers/promises";
import {
  rejectPendingExceptSession,
  rejectPendingForSession,
} from "../bridge/cursor-to-pi/tool-bridge";
import { retainOnlyAgentStore } from "../lib/agent-store";
import { evictAgentStore } from "./agent-store";
import {
  deleteLiveSession,
  getLiveSession,
  retainOnlyLiveSession,
} from "./agent-stream-hook";

const TERMINATION_WAIT_MS = 2_000;
const DEFAULT_TEARDOWN_TIMEOUT_MS = 10_000;
let teardownTimeoutMs = DEFAULT_TEARDOWN_TIMEOUT_MS;

/** Test seam: how long one teardown task may hold up the next. */
export function setTeardownTimeoutMs(ms: number | undefined): void {
  teardownTimeoutMs = ms ?? DEFAULT_TEARDOWN_TIMEOUT_MS;
}

const teardowns = new Map<string, Promise<void>>();
/** Run startups still loading, per session. Ending a session cancels them. */
const startups = new Map<string, Set<symbol>>();

const endStartups = (sessionId: string): void => {
  startups.delete(sessionId);
};

export interface SessionStartup {
  /** False once the session was terminated or dropped since `begin`. */
  stillWanted: () => boolean;
  /** Release the token once the startup has finished or failed. */
  end: () => void;
}

/** Call when a run starts loading; a run that is no longer wanted must not start. */
export function beginSessionStartup(sessionId: string): SessionStartup {
  const token = Symbol(sessionId);
  let tokens = startups.get(sessionId);
  if (!tokens) {
    tokens = new Set();
    startups.set(sessionId, tokens);
  }
  const owner = tokens;
  owner.add(token);
  return {
    stillWanted: () => startups.get(sessionId)?.has(token) ?? false,
    end: () => {
      owner.delete(token);
      if (owner.size === 0 && startups.get(sessionId) === owner) {
        startups.delete(sessionId);
      }
    },
  };
}

/**
 * Resolves once no teardown task for this session is still flushing or
 * evicting its agent store, which would clobber a newer run's store.
 */
export async function awaitSessionTeardown(sessionId: string): Promise<void> {
  while (true) {
    const pending = teardowns.get(sessionId);
    if (!pending) return;
    await pending;
  }
}

/**
 * Run work that flushes or evicts a session's agent store. Tasks for one
 * session run in order, and new runs wait for them (`awaitSessionTeardown`).
 * A task that outlives the timeout stops holding up the queue; `isCurrent`
 * then turns false and the task must skip destructive steps such as
 * dropping the store, which may belong to a newer run by then.
 */
export async function runSessionTeardown(
  sessionId: string,
  work: (isCurrent: () => boolean) => Promise<void>,
): Promise<void> {
  const previous = teardowns.get(sessionId) ?? Promise.resolve();
  const teardown = previous.then(() => runBounded(work));
  teardowns.set(sessionId, teardown);
  try {
    await teardown;
  } finally {
    if (teardowns.get(sessionId) === teardown) teardowns.delete(sessionId);
  }
}

async function runBounded(
  work: (isCurrent: () => boolean) => Promise<void>,
): Promise<void> {
  let current = true;
  let timer: ReturnType<typeof globalThis.setTimeout> | undefined;
  const timeout = new Promise<void>((resolve) => {
    timer = globalThis.setTimeout(() => {
      current = false;
      resolve();
    }, teardownTimeoutMs);
    timer.unref?.();
  });
  try {
    await Promise.race([work(() => current).catch(() => {}), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

export async function terminateSession(
  sessionId: string,
  reason: string,
): Promise<void> {
  endStartups(sessionId);
  const live = getLiveSession(sessionId);
  if (live) {
    live.abort(reason);
    deleteLiveSession(sessionId);
  }
  rejectPendingForSession(sessionId, reason);

  await runSessionTeardown(sessionId, async (isCurrent) => {
    let flushed = false;
    if (live) {
      await Promise.race([
        live.cursorRunPromise.catch(() => {}),
        setTimeout(TERMINATION_WAIT_MS),
      ]);
      try {
        await live.flushSessionState(isCurrent);
        flushed = true;
      } catch {}
    }
    await evictAgentStore(sessionId, {
      persist: !flushed,
      isCurrent,
    }).catch(() => {});
  });
}

export function retainOnlyActiveSessionMemory(
  sessionId: string | null,
  reason = "Session ended",
): void {
  for (const id of [...startups.keys()]) {
    if (id !== sessionId) endStartups(id);
  }
  rejectPendingExceptSession(sessionId, reason);
  retainOnlyLiveSession(sessionId);
  retainOnlyAgentStore(sessionId);
}
