import fs from "node:fs/promises";
import path from "node:path";
import type { AttemptFailure } from "../vendor/agent-client/connect";
import type { StallInfo } from "../vendor/agent-client/retry-policy";
import { PI_CURSOR_AGENT_LOGS_DIR } from "./env";

const MAX_LOG_BYTES = 1024 * 1024;

export interface StreamErrorLogEntry {
  runId: string;
  attempt: number;
  error: string;
  code?: unknown;
  kind: AttemptFailure["kind"];
  decision: AttemptFailure["decision"];
  reason: AttemptFailure["reason"];
  outcome: "retried" | "completed" | "failed";
  turnEnded: boolean;
  terminalCheckpoint: boolean;
  checkpointThisAttempt: boolean;
  outputSinceCheckpoint: boolean;
  checkpointCurrent: boolean;
  steerDelivered: boolean;
  aborted: boolean;
  stall?: StallInfo;
}

interface RunFlags {
  runId: string;
  checkpointCurrent: boolean;
  steerDelivered: boolean;
  aborted: boolean;
}

export function streamErrorLogEntry(
  failure: AttemptFailure,
  flags: RunFlags,
): StreamErrorLogEntry {
  const { error, progress } = failure;
  const code = (error as { code?: unknown } | null)?.code;
  return {
    runId: flags.runId,
    attempt: failure.attempt,
    error: error instanceof Error ? error.message : String(error),
    ...(code !== undefined ? { code } : {}),
    kind: failure.kind,
    decision: failure.decision,
    reason: failure.reason,
    outcome:
      failure.decision === "complete"
        ? "completed"
        : failure.decision === "fail"
          ? "failed"
          : "retried",
    turnEnded: progress.turnEnded,
    terminalCheckpoint: progress.terminalCheckpoint,
    checkpointThisAttempt: progress.checkpointThisAttempt,
    outputSinceCheckpoint: progress.outputSinceCheckpoint,
    checkpointCurrent: flags.checkpointCurrent,
    steerDelivered: flags.steerDelivered,
    aborted: flags.aborted,
    ...(failure.stall ? { stall: failure.stall } : {}),
  };
}

export function streamErrorLogPath(sessionId: string): string {
  return path.join(PI_CURSOR_AGENT_LOGS_DIR, `${sessionId}.jsonl`);
}

/** Best-effort: never throws, and stops writing once the file is full. */
export async function logStreamError(
  sessionId: string,
  entry: StreamErrorLogEntry,
): Promise<void> {
  try {
    const file = streamErrorLogPath(sessionId);
    const size = await fs.stat(file).then(
      (stat) => stat.size,
      () => 0,
    );
    if (size >= MAX_LOG_BYTES) return;
    await fs.mkdir(PI_CURSOR_AGENT_LOGS_DIR, { recursive: true });
    const line = JSON.stringify({ time: new Date().toISOString(), ...entry });
    await fs.appendFile(file, `${line}\n`);
  } catch {}
}
