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

interface PendingResult {
  sessionId: string;
  channel: LiveEventChannel | null;
  resolve: (result: ToolResultMessage) => void;
  reject: (error: Error) => void;
}

const pendingResults = new Map<string, PendingResult>();

export function requestToolExecution(
  channel: LiveEventChannel | null,
  request: ToolExecRequest,
): Promise<ToolResultMessage> {
  return new Promise<ToolResultMessage>((resolve, reject) => {
    if (channel?.isDone) {
      reject(new Error("Tool bridge not available — run has ended"));
      return;
    }
    const sessionId = channel?.sessionId ?? "";
    pendingResults.set(request.toolCallId, {
      sessionId,
      channel,
      resolve,
      reject,
    });

    if (channel) {
      channel.push({ kind: "tool-exec-request", request });
    } else {
      pendingResults.delete(request.toolCallId);
      reject(new Error("Tool bridge not available — no active stream"));
    }
  });
}

export function resolveToolResult(result: ToolResultMessage): boolean {
  const pending = pendingResults.get(result.toolCallId);
  if (!pending) return false;
  pendingResults.delete(result.toolCallId);
  pending.resolve(result);
  return true;
}

export function rejectPendingForSession(
  sessionId: string,
  reason: string,
): void {
  for (const [id, pending] of pendingResults) {
    if (pending.sessionId === sessionId) {
      pending.reject(new Error(reason));
      pendingResults.delete(id);
    }
  }
}

/** Reject the pending tool requests of one run, identified by its channel. */
export function rejectPendingForChannel(
  channel: LiveEventChannel,
  reason: string,
): void {
  for (const [id, pending] of pendingResults) {
    if (pending.channel === channel) {
      pending.reject(new Error(reason));
      pendingResults.delete(id);
    }
  }
}

export function rejectPendingExceptSession(
  sessionId: string | null,
  reason: string,
): void {
  for (const [id, pending] of pendingResults) {
    if (sessionId === null || pending.sessionId !== sessionId) {
      pending.reject(new Error(reason));
      pendingResults.delete(id);
    }
  }
}
