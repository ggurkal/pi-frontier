import { LostConnection } from "./exec-controller";

export const MAX_RETRY_ATTEMPTS = 5;
/** Cursor's `ogd`: resumes that stream without a new checkpoint. */
export const NO_PROGRESS_RESUME_LIMIT = 2;
/** Cursor's `Umd`: how long consecutive stall retries may take. */
export const STALL_RETRY_BUDGET_MS = 180_000;

/** Cursor's `a4i`, matched as substrings. */
export const TRANSPORT_ERROR_PATTERNS = [
  "NGHTTP2",
  "ECONNRESET",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "EPIPE",
  "socket hang up",
  "Premature close",
  "ERR_STREAM",
  "protocol error",
  "http/2 stream",
  "ERR_HTTP2_SESSION_ERROR",
  "Session closed with error code",
  "connection aborted",
] as const;

/** Cursor's `CYs`, matched as whole codes. */
export const NETWORK_ERROR_CODES: ReadonlySet<string> = new Set([
  "ENOTFOUND",
  "EAI_AGAIN",
  "EAI_FAIL",
  "ENODATA",
  "ESERVFAIL",
  "EHOSTUNREACH",
  "ENETDOWN",
  "ENETUNREACH",
]);

const NETWORK_ERROR_CODE_PATTERN = new RegExp(
  `\\b(${[...NETWORK_ERROR_CODES].join("|")})\\b`,
);

export class StreamEndedWithoutTurnEndedError extends Error {
  constructor() {
    super(
      "Stream ended without turnEnded — connection likely dropped mid-stream",
    );
    this.name = "StreamEndedWithoutTurnEndedError";
  }
}

export interface StallInfo {
  thresholdMs: number;
  lastActivityAgoMs: number;
  lastServerHeartbeatAgoMs?: number;
  lastClientHeartbeatAgoMs?: number;
  lastInboundMessageType?: string;
}

export class ConnectionStalledError extends Error {
  readonly code = "connection_stalled";
  readonly stall: StallInfo;

  constructor(stall: StallInfo, message?: string) {
    super(
      message ??
        `Connection stalled: no data from Cursor for ${Math.round(stall.thresholdMs / 1000)}s`,
    );
    this.name = "ConnectionStalledError";
    this.stall = stall;
  }
}

export class NoResumeProgressError extends Error {
  constructor(cause: unknown) {
    super(
      "Agent turn stopped after repeated resume attempts made no progress",
      {
        cause,
      },
    );
    this.name = "NoResumeProgressError";
  }
}

/** Cursor's `o4i`: the error and its cause chain, by `name: message` and `code`. */
export function matchesTransportPattern(error: unknown): boolean {
  const seen = new Set<unknown>();
  let current = error;
  while (current instanceof Error && !seen.has(current)) {
    seen.add(current);
    const text = `${current.name}: ${current.message}`;
    if (
      TRANSPORT_ERROR_PATTERNS.some((pattern) => text.includes(pattern)) ||
      NETWORK_ERROR_CODE_PATTERN.test(text)
    ) {
      return true;
    }
    const code = (current as { code?: unknown }).code;
    if (
      typeof code === "string" &&
      (TRANSPORT_ERROR_PATTERNS.some((pattern) => code.includes(pattern)) ||
        NETWORK_ERROR_CODES.has(code))
    ) {
      return true;
    }
    current = current.cause;
  }
  return false;
}

export function isTransportError(error: unknown, turnEnded: boolean): boolean {
  return (
    error instanceof LostConnection ||
    error instanceof StreamEndedWithoutTurnEndedError ||
    error instanceof ConnectionStalledError ||
    (!turnEnded &&
      error instanceof Error &&
      error.message === "[aborted] aborted") ||
    matchesTransportPattern(error)
  );
}

export type FailureKind = "transport" | "stall";

export interface ProgressSnapshot {
  turnEnded: boolean;
  /** A checkpoint arrived after `turnEnded`. */
  terminalCheckpoint: boolean;
  checkpointThisAttempt: boolean;
  outputSinceCheckpoint: boolean;
  /** We wrote a tool result since the checkpoint (or the attempt start). */
  stateSentSinceCheckpoint: boolean;
  /** A non-heartbeat message arrived in this attempt. */
  streamed: boolean;
}

export interface RetryState {
  /** 0-based index of the failed attempt. */
  attempt: number;
  actionIsResume: boolean;
  noProgressResumes: number;
  stallRetryStartedAt: number | undefined;
  now: number;
}

export type RetryDecision =
  | {
      decision: "complete";
      reason: "terminal_checkpoint" | "clean_checkpoint_before_turn_end";
    }
  | { decision: "resume"; reason: "clean_checkpoint" }
  | { decision: "resend"; reason: "no_output" }
  | {
      decision: "fail";
      reason:
        | "retry_cap"
        | "stall_budget"
        | "output_since_checkpoint"
        | "no_progress";
    };

/**
 * What to do after a transport or stall failure. Never replays output Pi
 * already shows: a retry either resumes from a checkpoint nothing followed,
 * or resends an action that produced nothing.
 */
export function decideRetry(
  kind: FailureKind,
  progress: ProgressSnapshot,
  state: RetryState,
): RetryDecision {
  if (progress.turnEnded && progress.terminalCheckpoint) {
    return { decision: "complete", reason: "terminal_checkpoint" };
  }
  if (
    progress.turnEnded &&
    progress.checkpointThisAttempt &&
    !progress.outputSinceCheckpoint &&
    !progress.stateSentSinceCheckpoint
  ) {
    return { decision: "complete", reason: "clean_checkpoint_before_turn_end" };
  }
  if (state.attempt >= MAX_RETRY_ATTEMPTS) {
    return { decision: "fail", reason: "retry_cap" };
  }
  if (
    kind === "stall" &&
    state.stallRetryStartedAt !== undefined &&
    state.now - state.stallRetryStartedAt >= STALL_RETRY_BUDGET_MS
  ) {
    return { decision: "fail", reason: "stall_budget" };
  }
  if (progress.outputSinceCheckpoint) {
    return { decision: "fail", reason: "output_since_checkpoint" };
  }
  if (progress.checkpointThisAttempt) {
    return { decision: "resume", reason: "clean_checkpoint" };
  }
  if (
    state.actionIsResume &&
    progress.streamed &&
    state.noProgressResumes + 1 >= NO_PROGRESS_RESUME_LIMIT
  ) {
    return { decision: "fail", reason: "no_progress" };
  }
  return { decision: "resend", reason: "no_output" };
}
