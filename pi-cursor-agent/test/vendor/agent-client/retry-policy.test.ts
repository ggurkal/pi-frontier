import assert from "node:assert/strict";
import test from "node:test";
import { Code, ConnectError } from "@connectrpc/connect";
import { LostConnection } from "../../../src/vendor/agent-client/exec-controller.js";
import {
  ConnectionStalledError,
  decideRetry,
  isTransportError,
  type ProgressSnapshot,
  type RetryState,
  StreamEndedWithoutTurnEndedError,
  TRANSPORT_ERROR_PATTERNS,
} from "../../../src/vendor/agent-client/retry-policy.js";

test("every Cursor transport pattern is a transport error", () => {
  for (const pattern of TRANSPORT_ERROR_PATTERNS) {
    assert.equal(isTransportError(new Error(`x ${pattern} y`), false), true);
  }
});

test("the truncated-stream errors we saw are transport errors", () => {
  for (const error of [
    new ConnectError(
      "protocol error: missing EndStreamResponse",
      Code.InvalidArgument,
    ),
    new ConnectError(
      "protocol error: promised 19289 bytes in enveloped message, got 16592 bytes",
      Code.InvalidArgument,
    ),
    new ConnectError("Premature close", Code.Unknown),
  ]) {
    assert.equal(isTransportError(error, true), true);
  }
});

test("network error codes match through the cause chain", () => {
  const withCode = (code: string) =>
    Object.assign(new Error("failed"), { code });
  assert.equal(
    isTransportError(
      new Error("outer", { cause: withCode("ECONNRESET") }),
      false,
    ),
    true,
  );
  assert.equal(isTransportError(withCode("ENOTFOUND"), false), true);
  assert.equal(
    isTransportError(new Error("getaddrinfo EAI_AGAIN api2.cursor.sh"), false),
    true,
  );
  assert.equal(isTransportError(withCode("ENOTFOUNDX"), false), false);
});

test("a cause cycle terminates", () => {
  const a = new Error("a");
  const b = new Error("b", { cause: a });
  Object.assign(a, { cause: b });
  assert.equal(isTransportError(a, false), false);
});

test("our own transport errors are transport errors", () => {
  assert.equal(isTransportError(new LostConnection("drop"), true), true);
  assert.equal(
    isTransportError(new StreamEndedWithoutTurnEndedError(), false),
    true,
  );
  assert.equal(
    isTransportError(
      new ConnectionStalledError({ thresholdMs: 1, lastActivityAgoMs: 1 }),
      true,
    ),
    true,
  );
});

test("[aborted] aborted is a transport error only before turnEnded", () => {
  const error = new Error("[aborted] aborted");
  assert.equal(isTransportError(error, false), true);
  assert.equal(isTransportError(error, true), false);
});

test("other errors are not transport errors", () => {
  assert.equal(
    isTransportError(new ConnectError("boom", Code.Internal), false),
    false,
  );
  assert.equal(isTransportError(new Error("Request cancelled"), false), false);
});

const progress = (over: Partial<ProgressSnapshot> = {}): ProgressSnapshot => ({
  turnEnded: false,
  terminalCheckpoint: false,
  checkpointThisAttempt: false,
  outputSinceCheckpoint: false,
  stateSentSinceCheckpoint: false,
  streamed: false,
  ...over,
});

const state = (over: Partial<RetryState> = {}): RetryState => ({
  attempt: 0,
  actionIsResume: false,
  noProgressResumes: 0,
  stallRetryStartedAt: undefined,
  now: 1_000_000,
  ...over,
});

const decide = (
  p: Partial<ProgressSnapshot>,
  s: Partial<RetryState> = {},
  kind: "transport" | "stall" = "transport",
) => decideRetry(kind, progress(p), state(s));

test("decideRetry", () => {
  const clean = { checkpointThisAttempt: true, streamed: true };
  const cases: Array<[string, ReturnType<typeof decideRetry>, string]> = [
    [
      "terminal checkpoint",
      decide({ ...clean, turnEnded: true, terminalCheckpoint: true }),
      "complete/terminal_checkpoint",
    ],
    [
      "clean checkpoint before turnEnded",
      decide({ ...clean, turnEnded: true }),
      "complete/clean_checkpoint_before_turn_end",
    ],
    [
      "tool result sent blocks completion",
      decide({ ...clean, turnEnded: true, stateSentSinceCheckpoint: true }),
      "resume/clean_checkpoint",
    ],
    ["cap", decide(clean, { attempt: 5 }), "fail/retry_cap"],
    [
      "complete beats the cap",
      decide(
        { ...clean, turnEnded: true, terminalCheckpoint: true },
        { attempt: 5 },
      ),
      "complete/terminal_checkpoint",
    ],
    [
      "stall budget spent",
      decide(clean, { stallRetryStartedAt: 1_000_000 - 180_000 }, "stall"),
      "fail/stall_budget",
    ],
    [
      "stall budget not spent",
      decide(clean, { stallRetryStartedAt: 1_000_000 - 179_999 }, "stall"),
      "resume/clean_checkpoint",
    ],
    [
      "transport ignores the stall budget",
      decide(clean, { stallRetryStartedAt: 0 }),
      "resume/clean_checkpoint",
    ],
    [
      "output since checkpoint",
      decide({ ...clean, outputSinceCheckpoint: true }),
      "fail/output_since_checkpoint",
    ],
    ["clean checkpoint", decide(clean), "resume/clean_checkpoint"],
    ["nothing arrived", decide({}), "resend/no_output"],
    [
      "resume streamed twice without progress",
      decide(
        { streamed: true },
        { actionIsResume: true, noProgressResumes: 1 },
      ),
      "fail/no_progress",
    ],
    [
      "resume that streamed nothing",
      decide({}, { actionIsResume: true, noProgressResumes: 1 }),
      "resend/no_output",
    ],
    [
      "not a resume",
      decide({ streamed: true }, { noProgressResumes: 1 }),
      "resend/no_output",
    ],
  ];
  for (const [name, decision, expected] of cases) {
    assert.equal(`${decision.decision}/${decision.reason}`, expected, name);
  }
});
