import type { ToolResultMessage } from "@earendil-works/pi-ai";
import type { LiveEventChannel } from "../../provider/agent-stream-hook";

export function shellQuote(value: string): string {
  return `'${value.replace(/'/g, "'\"'\"'")}'`;
}

export interface ToolExecRequest {
  toolCallId: string;
  cursorExecType: string;
  piToolName: string;
  piToolArgs: Record<string, unknown>;
}

interface InflightResult {
  sessionId: string;
  channel: LiveEventChannel | null;
  promise: Promise<ToolResultMessage>;
  resolve: (result: ToolResultMessage) => void;
  reject: (error: Error) => void;
}

interface CompletedResult {
  sessionId: string;
  channel: LiveEventChannel | null;
  result: ToolResultMessage;
}

/**
 * A resumed run can request a tool call it already requested. Keyed by
 * `toolCallId`, a repeat attaches to the running call or gets its result
 * instead of running the tool again.
 */
const inflight = new Map<string, InflightResult>();
const completed = new Map<string, CompletedResult>();
const COMPLETED_RESULT_LIMIT = 256;

export function requestToolExecution(
  channel: LiveEventChannel | null,
  request: ToolExecRequest,
): Promise<ToolResultMessage> {
  if (channel?.isDone) {
    return Promise.reject(
      new Error("Tool bridge not available — run has ended"),
    );
  }
  const sessionId = channel?.sessionId ?? "";
  const done = completed.get(request.toolCallId);
  if (done?.sessionId === sessionId) return Promise.resolve(done.result);
  const pending = inflight.get(request.toolCallId);
  if (pending?.sessionId === sessionId) return pending.promise;

  if (!channel) {
    return Promise.reject(
      new Error("Tool bridge not available — no active stream"),
    );
  }
  let resolve!: (result: ToolResultMessage) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<ToolResultMessage>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  inflight.set(request.toolCallId, {
    sessionId,
    channel,
    promise,
    resolve,
    reject,
  });
  channel.push({ kind: "tool-exec-request", request });
  return promise;
}

export function resolveToolResult(result: ToolResultMessage): boolean {
  const pending = inflight.get(result.toolCallId);
  if (!pending) return false;
  inflight.delete(result.toolCallId);
  pending.resolve(result);
  completed.set(result.toolCallId, {
    sessionId: pending.sessionId,
    channel: pending.channel,
    result,
  });
  for (const id of completed.keys()) {
    if (completed.size <= COMPLETED_RESULT_LIMIT) break;
    completed.delete(id);
  }
  return true;
}

function rejectWhere(
  matches: (entry: {
    sessionId: string;
    channel: LiveEventChannel | null;
  }) => boolean,
  reason: string,
): void {
  for (const [id, pending] of inflight) {
    if (matches(pending)) {
      pending.reject(new Error(reason));
      inflight.delete(id);
    }
  }
  for (const [id, done] of completed) {
    if (matches(done)) completed.delete(id);
  }
}

export function rejectPendingForSession(
  sessionId: string,
  reason: string,
): void {
  rejectWhere((entry) => entry.sessionId === sessionId, reason);
}

/** Reject the pending tool requests of one run, identified by its channel. */
export function rejectPendingForChannel(
  channel: LiveEventChannel,
  reason: string,
): void {
  rejectWhere((entry) => entry.channel === channel, reason);
}

export function rejectPendingExceptSession(
  sessionId: string | null,
  reason: string,
): void {
  rejectWhere(
    (entry) => sessionId === null || entry.sessionId !== sessionId,
    reason,
  );
}
