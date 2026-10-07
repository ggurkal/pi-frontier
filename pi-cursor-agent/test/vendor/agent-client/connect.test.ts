import assert from "node:assert/strict";
import test from "node:test";
import { Code, ConnectError } from "@connectrpc/connect";
import {
  AgentClientMessage,
  AgentRunRequest,
  AgentServerMessage,
  ConversationAction,
  ConversationStateStructure,
  HeartbeatUpdate,
  InteractionQuery,
  InteractionUpdate,
  TextDeltaUpdate,
  TokenDeltaUpdate,
  TurnEndedUpdate,
  UserMessage,
  UserMessageAction,
} from "../../../src/__generated__/agent/v1/agent_pb.js";
import {
  AskQuestionArgs,
  AskQuestionInteractionQuery,
} from "../../../src/__generated__/agent/v1/ask_question_tool_pb.js";
import {
  AgentConnectClient,
  type AgentConnectRunOptions,
  type AgentRpcClient,
  type AttemptFailure,
} from "../../../src/vendor/agent-client/connect.js";
import {
  NoResumeProgressError,
  StreamEndedWithoutTurnEndedError,
} from "../../../src/vendor/agent-client/retry-policy.js";

function initialRequest(text = "hello"): AgentClientMessage {
  return new AgentClientMessage({
    message: {
      case: "runRequest",
      value: new AgentRunRequest({
        action: new ConversationAction({
          action: {
            case: "userMessageAction",
            value: new UserMessageAction({
              userMessage: new UserMessage({ text }),
            }),
          },
        }),
      }),
    },
  });
}

function baseRunOptions(): AgentConnectRunOptions {
  return {
    interactionListener: {
      sendUpdate: async () => {},
      query: async () => ({ approved: false, reason: "not used in this test" }),
    },
    resources: { entries: () => [] },
    blobStore: {
      getBlob: async () => undefined,
      setBlob: async () => {},
    },
    checkpointHandler: {
      handleCheckpoint: async () => {},
      getLatestCheckpoint: () => undefined,
    },
  };
}

const update = (message: InteractionUpdate["message"]) =>
  new AgentServerMessage({
    message: {
      case: "interactionUpdate",
      value: new InteractionUpdate({ message }),
    },
  });

const textDelta = (text: string) =>
  update({ case: "textDelta", value: new TextDeltaUpdate({ text }) });

const turnEnded = () =>
  update({ case: "turnEnded", value: new TurnEndedUpdate() });

const tokenDelta = () =>
  update({ case: "tokenDelta", value: new TokenDeltaUpdate({ tokens: 1 }) });

const checkpoint = (state = new ConversationStateStructure()) =>
  new AgentServerMessage({
    message: { case: "conversationCheckpointUpdate", value: state },
  });

interface ScriptedAttempt {
  messages: AgentServerMessage[];
  /** Thrown after the messages; otherwise the stream ends cleanly. */
  error?: unknown;
}

function scriptedClient(attempts: ScriptedAttempt[]) {
  const calls: Array<{
    request: AgentRunRequest;
    headers: Record<string, string> | undefined;
  }> = [];
  const resets = { count: 0 };
  const client: AgentRpcClient = {
    resetConnection() {
      resets.count++;
    },
    run(input, options) {
      const script = attempts[calls.length] ?? { messages: [turnEnded()] };
      const call = {
        request: new AgentRunRequest(),
        headers: options?.headers,
      };
      calls.push(call);
      return (async function* () {
        for await (const message of input) {
          if (message.message.case === "runRequest") {
            call.request = message.message.value;
            break;
          }
        }
        yield* script.messages;
        if (script.error !== undefined) throw script.error;
      })();
    },
  };
  return { client, calls, resets };
}

test("a checkpoint is applied after the updates that precede it", async () => {
  const order: string[] = [];
  const { client } = scriptedClient([
    { messages: [textDelta("a"), checkpoint(), turnEnded()] },
  ]);
  await new AgentConnectClient(client).run(initialRequest(), {
    ...baseRunOptions(),
    interactionListener: {
      sendUpdate: async (_ctx, coreUpdate) => {
        if (coreUpdate.type !== "text-delta") return;
        await new Promise((resolve) => setTimeout(resolve, 20));
        order.push("update");
      },
      query: async () => ({ approved: false, reason: "unused" }),
    },
    checkpointHandler: {
      handleCheckpoint: async () => {
        order.push("checkpoint");
      },
    },
  });
  assert.deepEqual(order, ["update", "checkpoint"]);
});

const truncated = () =>
  new ConnectError(
    "protocol error: missing EndStreamResponse",
    Code.InvalidArgument,
  );

/** Runs `attempts` and records failures; the checkpoint handler keeps the latest. */
async function runScripted(
  attempts: ScriptedAttempt[],
  extra: Partial<AgentConnectRunOptions> = {},
) {
  const { client, calls, resets } = scriptedClient(attempts);
  const failures: AttemptFailure[] = [];
  let latest: ConversationStateStructure | undefined;
  const promise = new AgentConnectClient(client).run(initialRequest(), {
    ...baseRunOptions(),
    backoffMs: () => 0,
    checkpointHandler: {
      handleCheckpoint: async (_ctx, state) => {
        latest = state;
      },
      getLatestCheckpoint: () => latest,
    },
    onAttemptFailed: (failure) => failures.push(failure),
    ...extra,
  });
  return { promise, calls, failures, resets };
}

const decisionOf = (failure: AttemptFailure | undefined) =>
  failure && `${failure.decision}/${failure.reason}`;

test("a cut after a clean checkpoint resumes from it", async () => {
  const state = new ConversationStateStructure({
    turns: [new Uint8Array([1])],
  });
  const { promise, calls, failures, resets } = await runScripted([
    { messages: [textDelta("a"), checkpoint(state)], error: truncated() },
    { messages: [turnEnded()] },
  ]);
  await promise;
  assert.equal(resets.count, 1);
  assert.equal(calls.length, 2);
  assert.equal(calls[1]?.request.action?.action.case, "resumeAction");
  assert.deepEqual(calls[1]?.request.conversationState, state);
  assert.equal(failures.length, 1);
  assert.equal(decisionOf(failures[0]), "resume/clean_checkpoint");
  assert.equal(failures[0]?.kind, "transport");
});

test("a cut after output that follows the checkpoint fails", async () => {
  const error = truncated();
  const { promise, calls, failures, resets } = await runScripted([
    { messages: [checkpoint(), textDelta("a")], error },
  ]);
  await assert.rejects(promise, (thrown) => thrown === error);
  assert.equal(resets.count, 1);
  assert.equal(calls.length, 1);
  assert.equal(decisionOf(failures[0]), "fail/output_since_checkpoint");
});

test("a failure before any message resends the original action", async () => {
  const { promise, calls, failures } = await runScripted([
    {
      messages: [],
      error: Object.assign(new Error("connect failed"), {
        code: "ECONNREFUSED",
      }),
    },
    { messages: [turnEnded()] },
  ]);
  await promise;
  assert.equal(calls.length, 2);
  assert.equal(calls[1]?.request.action?.action.case, "userMessageAction");
  assert.equal(decisionOf(failures[0]), "resend/no_output");
});

test("a cut after turnEnded and a terminal checkpoint completes", async () => {
  const { promise, calls, failures, resets } = await runScripted([
    {
      messages: [textDelta("a"), turnEnded(), checkpoint()],
      error: truncated(),
    },
  ]);
  await promise;
  assert.equal(resets.count, 0);
  assert.equal(calls.length, 1);
  assert.equal(decisionOf(failures[0]), "complete/terminal_checkpoint");
});

test("a cut after checkpoint then turnEnded completes", async () => {
  const { promise, failures } = await runScripted([
    {
      messages: [textDelta("a"), checkpoint(), turnEnded()],
      error: truncated(),
    },
  ]);
  await promise;
  assert.equal(
    decisionOf(failures[0]),
    "complete/clean_checkpoint_before_turn_end",
  );
});

test("a clean end without turnEnded after output fails", async () => {
  const { promise, failures } = await runScripted([
    { messages: [textDelta("a")] },
  ]);
  await assert.rejects(promise, StreamEndedWithoutTurnEndedError);
  assert.equal(decisionOf(failures[0]), "fail/output_since_checkpoint");
});

test("a non-transport error is rethrown without a decision", async () => {
  const error = new ConnectError("boom", Code.Internal);
  const { promise, failures, resets } = await runScripted([
    { messages: [checkpoint()], error },
  ]);
  await assert.rejects(promise, (thrown) => thrown === error);
  assert.equal(failures.length, 0);
  assert.equal(resets.count, 0);
});

test("a session abort is rethrown without a decision", async () => {
  const controller = new AbortController();
  const failures: AttemptFailure[] = [];
  let calls = 0;
  let resets = 0;
  const client: AgentRpcClient = {
    resetConnection() {
      resets++;
    },
    run(_input, options) {
      calls++;
      return (async function* () {
        await new Promise<never>((_resolve, reject) => {
          options?.signal?.addEventListener("abort", () =>
            reject(new ConnectError("Premature close", Code.Unknown)),
          );
        });
      })();
    },
  };
  const promise = new AgentConnectClient(client).run(initialRequest(), {
    ...baseRunOptions(),
    signal: controller.signal,
    onAttemptFailed: (failure) => failures.push(failure),
  });
  await new Promise((resolve) => setTimeout(resolve, 10));
  controller.abort();
  await assert.rejects(promise);
  assert.equal(failures.length, 0);
  assert.equal(calls, 1);
  assert.equal(resets, 0);
});

test("retries stop at the cap", async () => {
  const attempts = Array.from({ length: 7 }, () => ({
    messages: [checkpoint()],
    error: truncated(),
  }));
  const { promise, calls, failures } = await runScripted(attempts);
  await assert.rejects(promise);
  assert.equal(calls.length, 6);
  assert.equal(decisionOf(failures.at(-1)), "fail/retry_cap");
});

test("resumes that stream without a checkpoint stop", async () => {
  const { promise, calls } = await runScripted([
    { messages: [checkpoint()], error: truncated() },
    { messages: [tokenDelta()], error: truncated() },
    { messages: [tokenDelta()], error: truncated() },
  ]);
  await assert.rejects(promise, NoResumeProgressError);
  assert.equal(calls.length, 3);
});

test("a stalled attempt resumes from its clean checkpoint", async () => {
  const failures: AttemptFailure[] = [];
  const signals: AbortSignal[] = [];
  let latest: ConversationStateStructure | undefined;
  let calls = 0;
  const client: AgentRpcClient = {
    run(_input, options) {
      const attempt = calls++;
      if (options?.signal) signals.push(options.signal);
      const signal = options?.signal;
      return (async function* () {
        if (attempt === 0) {
          yield checkpoint();
          await new Promise<never>((_resolve, reject) => {
            signal?.addEventListener("abort", () => reject(signal.reason));
          });
        }
        for (let i = 0; i < 12; i++) {
          yield update({ case: "heartbeat", value: new HeartbeatUpdate() });
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        yield turnEnded();
      })();
    },
  };
  await new AgentConnectClient(client).run(initialRequest(), {
    ...baseRunOptions(),
    backoffMs: () => 0,
    stallThresholdMs: 40,
    checkpointHandler: {
      handleCheckpoint: async (_ctx, state) => {
        latest = state;
      },
      getLatestCheckpoint: () => latest,
    },
    onAttemptFailed: (failure) => failures.push(failure),
  });
  assert.equal(calls, 2);
  assert.equal(signals[0]?.aborted, true);
  assert.equal(failures.length, 1);
  assert.equal(decisionOf(failures[0]), "resume/clean_checkpoint");
  assert.equal(failures[0]?.kind, "stall");
  assert.equal(failures[0]?.stall?.thresholdMs, 40);
});

test("a human query pauses stall detection", async () => {
  const failures: AttemptFailure[] = [];
  let calls = 0;
  const client: AgentRpcClient = {
    run(_input, options) {
      calls++;
      const signal = options?.signal;
      return (async function* () {
        yield new AgentServerMessage({
          message: {
            case: "interactionQuery",
            value: new InteractionQuery({
              id: 1,
              query: {
                case: "askQuestionInteractionQuery",
                value: new AskQuestionInteractionQuery({
                  args: new AskQuestionArgs(),
                  toolCallId: "ask-1",
                }),
              },
            }),
          },
        });
        await new Promise((resolve, reject) => {
          setTimeout(resolve, 150);
          signal?.addEventListener("abort", () => reject(signal.reason));
        });
        yield turnEnded();
      })();
    },
  };
  let answered!: () => void;
  const answer = new Promise<void>((resolve) => {
    answered = resolve;
  });
  setTimeout(() => answered(), 140);
  await new AgentConnectClient(client).run(initialRequest(), {
    ...baseRunOptions(),
    stallThresholdMs: 40,
    interactionListener: {
      sendUpdate: async () => {},
      query: async () => {
        await answer;
        return { approved: false, reason: "unused" };
      },
    },
    onAttemptFailed: (failure) => failures.push(failure),
  });
  assert.equal(calls, 1);
  assert.equal(failures.length, 0);
});
