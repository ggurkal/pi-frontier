import fs from "node:fs/promises";
import path from "node:path";
import { PI_CURSOR_AGENT_LOGS_DIR } from "./env";

const MAX_LOG_BYTES = 1024 * 1024;

const TRUNCATED_STREAM_PATTERNS = [
  /missing EndStreamResponse/,
  /Premature close/,
  /promised \d+ bytes in enveloped message/,
];

/** The Run response ended before Connect read its final frame. */
export function isTruncatedStreamError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return TRUNCATED_STREAM_PATTERNS.some((pattern) => pattern.test(message));
}

export interface StreamErrorLogEntry {
  runId: string;
  error: string;
  code?: unknown;
  turnEnded: boolean;
  checkpointCurrent: boolean;
  steerDelivered: boolean;
  aborted: boolean;
  outcome: "completed" | "failed";
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
