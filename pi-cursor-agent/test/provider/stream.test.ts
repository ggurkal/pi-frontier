import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { after, afterEach, before, test } from "node:test";
import { Code, ConnectError } from "@connectrpc/connect";
import type {
  AssistantMessage,
  AssistantMessageEvent,
  Context,
  ToolResultMessage,
} from "@earendil-works/pi-ai";
import {
  type AgentClientMessage,
  type AgentRunRequest,
  AgentServerMessage,
  ContextInjectionCancelled,
  ContextInjectionDelivered,
  ContextInjectionQueued,
  ContextInjectionQueuedForNextTurn,
  ContextInjectionRejected,
  ContextInjectionState,
  ContextInjectionStateUpdate,
  type ConversationAction,
  ConversationStateStructure,
  type InjectContextAction,
  InteractionUpdate,
  TextDeltaUpdate,
  TurnEndedUpdate,
} from "../../src/__generated__/agent/v1/agent_pb.js";
import { ExecServerMessage } from "../../src/__generated__/agent/v1/exec_pb.js";
import { ReadArgs } from "../../src/__generated__/agent/v1/read_exec_pb.js";
import {
  requestToolExecution,
  resolveToolResult,
} from "../../src/bridge/cursor-to-pi/tool-bridge.js";
import { createSteerDispatcher } from "../../src/provider/pending-messages.js";
import { LostConnection } from "../../src/vendor/agent-client/exec-controller.js";

type StreamModule = typeof import("../../src/provider/stream.js");
type HookModule = typeof import("../../src/provider/agent-stream-hook.js");
type StateModule = typeof import("../../src/provider/state.js");
type LifecycleModule = typeof import("../../src/provider/session-lifecycle.js");

let cacheDir: string;
let streamModule: StreamModule;
let hook: HookModule;
let stateModule: StateModule;
let lifecycle: LifecycleModule;

type AckState =
  | "queued"
  | "delivered"
  | "queuedForNextTurn"
  | "cancelled"
  | "rejected";

class FakeRun {
  readonly headers: Record<string, string>;
  readonly received: AgentClientMessage[] = [];
  aborted = false;
  private outbox: AgentServerMessage[] = [];
  private ended = false;
  private failure: unknown;
  private wake: (() => void) | undefined;
  private receivedWaiters: Array<() => void> = [];
  private execId = 0;
  /** While set, the server stops reading, so client writes block. */
  private inputGate: Promise<void> | undefined;
  private releaseInput: (() => void) | undefined;

  constructor(
    input: AsyncIterable<AgentClientMessage>,
    headers: Record<string, string> | undefined,
    signal: AbortSignal | undefined,
  ) {
    this.headers = headers ?? {};
    void (async () => {
      for await (const message of input) {
        this.received.push(message);
        for (const resolve of this.receivedWaiters.splice(0)) resolve();
        await this.inputGate;
      }
    })();
    signal?.addEventListener("abort", () => {
      this.aborted = true;
      this.releaseInput?.();
      this.fail(signal.reason ?? new Error("aborted"));
    });
  }

  get runRequest(): AgentRunRequest {
    const first = this.received[0];
    assert.equal(first?.message.case, "runRequest");
    return first.message.value as AgentRunRequest;
  }

  get runText(): string {
    const action = this.runRequest.action?.action;
    assert.equal(action?.case, "userMessageAction");
    return action.value.userMessage?.text ?? "";
  }

  stallInput(): void {
    this.inputGate = new Promise<void>((resolve) => {
      this.releaseInput = resolve;
    });
  }

  checkpoint(state: ConversationStateStructure): void {
    this.push(
      new AgentServerMessage({
        message: { case: "conversationCheckpointUpdate", value: state },
      }),
    );
  }

  injections(): InjectContextAction[] {
    return this.received.flatMap((m) => {
      if (m.message.case !== "conversationAction") return [];
      const action = (m.message.value as ConversationAction).action;
      return action.case === "injectContextAction" ? [action.value] : [];
    });
  }

  async waitForInjections(count: number): Promise<InjectContextAction[]> {
    await this.waitUntil(() => this.injections().length >= count);
    return this.injections();
  }

  async waitForExecResults(count: number): Promise<void> {
    await this.waitUntil(
      () =>
        this.received.filter((m) => m.message.case === "execClientMessage")
          .length >= count,
    );
  }

  private async waitUntil(done: () => boolean): Promise<void> {
    while (!done()) {
      await new Promise<void>((r) => this.receivedWaiters.push(r));
    }
  }

  text(text: string): void {
    this.sendUpdate(
      new InteractionUpdate({
        message: { case: "textDelta", value: new TextDeltaUpdate({ text }) },
      }),
    );
  }

  turnEnded(): void {
    this.sendUpdate(
      new InteractionUpdate({
        message: { case: "turnEnded", value: new TurnEndedUpdate() },
      }),
    );
  }

  readTool(toolCallId: string): void {
    this.execId++;
    this.push(
      new AgentServerMessage({
        message: {
          case: "execServerMessage",
          value: new ExecServerMessage({
            id: this.execId,
            execId: `exec-${this.execId}`,
            message: {
              case: "readArgs",
              value: new ReadArgs({ path: "README.md", toolCallId }),
            },
          }),
        },
      }),
    );
  }

  ack(injectionId: string, state: AckState): void {
    const value = {
      queued: () => ({ case: "queued", value: new ContextInjectionQueued() }),
      delivered: () => ({
        case: "delivered",
        value: new ContextInjectionDelivered(),
      }),
      queuedForNextTurn: () => ({
        case: "queuedForNextTurn",
        value: new ContextInjectionQueuedForNextTurn(),
      }),
      cancelled: () => ({
        case: "cancelled",
        value: new ContextInjectionCancelled(),
      }),
      rejected: () => ({
        case: "rejected",
        value: new ContextInjectionRejected(),
      }),
    }[state]() as ContextInjectionState["state"];
    this.sendUpdate(
      new InteractionUpdate({
        message: {
          case: "contextInjectionState",
          value: new ContextInjectionStateUpdate({
            injectionId,
            state: new ContextInjectionState({ state: value }),
          }),
        },
      }),
    );
  }

  end(): void {
    this.ended = true;
    this.wake?.();
  }

  fail(error: unknown): void {
    this.failure = error;
    this.wake?.();
  }

  private sendUpdate(update: InteractionUpdate): void {
    this.push(
      new AgentServerMessage({
        message: { case: "interactionUpdate", value: update },
      }),
    );
  }

  private push(message: AgentServerMessage): void {
    this.outbox.push(message);
    this.wake?.();
  }

  async *stream(): AsyncGenerator<AgentServerMessage> {
    while (true) {
      const next = this.outbox.shift();
      if (next) {
        yield next;
        continue;
      }
      if (this.failure !== undefined) throw this.failure;
      if (this.ended) return;
      await new Promise<void>((r) => {
        this.wake = r;
      });
      this.wake = undefined;
    }
  }
}

let runs: FakeRun[] = [];
/** Called from `flushSessionState` when it persists a new checkpoint. */
let onAppendEntry: (() => void) | undefined;
let runWaiters: Array<() => void> = [];

async function waitForRun(index: number): Promise<FakeRun> {
  while (runs.length <= index) {
    await new Promise<void>((r) => runWaiters.push(r));
  }
  const run = runs[index];
  assert.ok(run);
  while (run.received.length === 0) {
    await new Promise<void>((r) => setImmediate(r));
  }
  return run;
}

/** Let queued server messages reach the dispatcher. */
async function settleEvents(): Promise<void> {
  for (let i = 0; i < 10; i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
}

before(async () => {
  cacheDir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-cursor-stream-"));
  process.env["PI_CODING_AGENT_DIR"] = cacheDir;
  streamModule = await import("../../src/provider/stream.js");
  hook = await import("../../src/provider/agent-stream-hook.js");
  stateModule = await import("../../src/provider/state.js");
  lifecycle = await import("../../src/provider/session-lifecycle.js");
  streamModule.setAgentRpcClientFactory(() => ({
    run(input, options) {
      const run = new FakeRun(input, options?.headers, options?.signal);
      runs.push(run);
      for (const resolve of runWaiters.splice(0)) resolve();
      return run.stream();
    },
  }));
});

afterEach(() => {
  onAppendEntry = undefined;
  // A failed test can leave a run open; its heartbeat would keep the
  // process alive.
  hook.retainOnlyLiveSession(null);
  for (const run of runs) run.end();
});

after(async () => {
  streamModule.setAgentRpcClientFactory(undefined);
  await fs.rm(cacheDir, { recursive: true, force: true });
});

let sessionCounter = 0;
let clock = 1_000;

function user(text: string, timestamp = clock++): Context["messages"][number] {
  return { role: "user", content: text, timestamp };
}

const model = {
  id: "test-model",
  name: "Test",
  api: "cursor-agent",
  provider: "cursor-agent",
  baseUrl: "http://fake.invalid",
  reasoning: false,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 100_000,
  maxTokens: 1_000,
} as never;

function startStream(
  sessionId: string,
  messages: Context["messages"],
  signal?: AbortSignal,
) {
  const pi = {
    getActiveTools: () => ["read"],
    appendEntry: () => onAppendEntry?.(),
  } as never;
  const state = stateModule.createStateStore(() => {});
  const stream = streamModule.streamCursorAgent(
    pi,
    () => null,
    state,
    model,
    { systemPrompt: "", messages },
    { sessionId, apiKey: "test-key", ...(signal ? { signal } : {}) },
  );
  const events: AssistantMessageEvent[] = [];
  void (async () => {
    for await (const event of stream) events.push(event);
  })();
  return { result: () => stream.result() };
}

function textOf(message: AssistantMessage): string {
  return message.content
    .map((block) => (block.type === "text" ? block.text : ""))
    .join("");
}

function toolResult(toolCallId: string): ToolResultMessage {
  const result: ToolResultMessage = {
    role: "toolResult",
    toolCallId,
    toolName: "read",
    content: [{ type: "text", text: "file contents" }],
    isError: false,
    timestamp: clock++,
  };
  resolveToolResult(result);
  return result;
}

function checkpointState(turns: Uint8Array[]): ConversationStateStructure {
  return new ConversationStateStructure({
    rootPromptMessagesJson: [new Uint8Array([1])],
    turns,
  });
}

function newSessionId(): string {
  runs = [];
  runWaiters = [];
  return `stream-test-${sessionCounter++}`;
}

/**
 * Starts a run that stops at a tool call, like Pi's first turn before a
 * steer, and returns what Pi would hold after running the tool.
 */
async function runUntilToolBatch(
  sessionId: string,
  toolCallId = "call-1",
  signal?: AbortSignal,
) {
  const history: Context["messages"] = [user("hello")];
  const s1 = startStream(sessionId, history, signal);
  const run = await waitForRun(0);
  run.text("looking");
  run.readTool(toolCallId);
  const assistant = await s1.result();
  assert.equal(assistant.stopReason, "toolUse");
  assert.ok(hook.getLiveSession(sessionId));
  const result = toolResult(toolCallId);
  await run.waitForExecResults(1);
  return { run, history: [...history, assistant, result] };
}

test("a steer after a tool batch is injected with the run id, and delivered steers are not resent", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);

  const s2 = startStream(sessionId, [...history, user("focus on tests")]);
  const [inject] = await run.waitForInjections(1);
  const runId = run.headers["x-original-request-id"];
  assert.ok(runId);
  assert.equal(run.headers["x-request-id"], runId);
  assert.equal(inject?.expectedRunId, runId);
  const payload = inject?.payload;
  assert.equal(payload?.case, "userContext");
  assert.equal(payload.value.userMessage?.text, "focus on tests");

  run.ack(inject.injectionId, "queued");
  run.ack(inject.injectionId, "delivered");
  run.text("on it");
  run.end();
  const reply = await s2.result();

  assert.equal(textOf(reply), "on it");
  assert.equal(runs.length, 1);
  assert.equal(hook.getLiveSession(sessionId), undefined);
});

test("Escape during a steered turn aborts the run and the next prompt starts fresh", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const controller = new AbortController();
  const steered = [...history, user("change course")];
  const s2 = startStream(sessionId, steered, controller.signal);
  await run.waitForInjections(1);
  run.text("partial");
  await settleEvents();

  controller.abort();
  const aborted = await s2.result();

  assert.equal(aborted.stopReason, "aborted");
  assert.equal(hook.getLiveSession(sessionId), undefined);
  assert.equal(run.aborted, true);

  const s3 = startStream(sessionId, [...steered, aborted, user("new prompt")]);
  const next = await waitForRun(1);
  assert.equal(next.runText, "new prompt");
  next.text("fresh");
  next.end();

  assert.equal(textOf(await s3.result()), "fresh");
  assert.equal(runs.length, 2);
});

for (const state of ["rejected", "cancelled", "queuedForNextTurn"] as const) {
  test(`a ${state} steer is sent as the next run in the same message`, async () => {
    const sessionId = newSessionId();
    const { run, history } = await runUntilToolBatch(sessionId);
    const s2 = startStream(sessionId, [...history, user("s1")]);
    const [inject] = await run.waitForInjections(1);
    run.ack(inject?.injectionId ?? "", state);
    run.text("finishing");
    run.end();

    const next = await waitForRun(1);
    assert.equal(next.runText, "s1");
    assert.notEqual(
      next.headers["x-original-request-id"],
      run.headers["x-original-request-id"],
    );
    next.text(" reply to s1");
    next.end();

    assert.equal(textOf(await s2.result()), "finishing reply to s1");
    assert.equal(hook.getLiveSession(sessionId), undefined);
  });
}

test("an unacked steer is sent as the next run", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1")]);
  await run.waitForInjections(1);
  run.end();

  const next = await waitForRun(1);
  assert.equal(next.runText, "s1");
  next.end();
  await s2.result();
});

test("an unacked steer holds back later steers; both run next in order", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1"), user("s2")]);
  await run.waitForInjections(1);
  await settleEvents();
  run.end();

  const next = await waitForRun(1);
  assert.equal(next.runText, "s1\n\ns2");
  assert.equal(run.injections().length, 1);
  next.end();
  await s2.result();
});

test("a rejected steer is never overtaken by a later one", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1"), user("s2")]);
  const [i1] = await run.waitForInjections(1);
  run.ack(i1?.injectionId ?? "", "rejected");
  await settleEvents();
  run.end();

  const next = await waitForRun(1);
  assert.equal(next.runText, "s1\n\ns2");
  assert.equal(run.injections().length, 1);
  next.end();
  await s2.result();
});

test("after queuedForNextTurn later steers are not injected and run next", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId, "call-1");
  const withS1 = [...history, user("s1")];
  const s2 = startStream(sessionId, withS1);
  const [i1] = await run.waitForInjections(1);
  run.ack(i1?.injectionId ?? "", "queuedForNextTurn");
  run.readTool("call-2");
  const second = await s2.result();
  assert.equal(second.stopReason, "toolUse");

  const secondResult = toolResult("call-2");
  await run.waitForExecResults(2);
  const s3 = startStream(sessionId, [
    ...withS1,
    second,
    secondResult,
    user("s2"),
  ]);
  await settleEvents();
  run.end();

  const next = await waitForRun(1);
  assert.equal(next.runText, "s1\n\ns2");
  assert.equal(run.injections().length, 1);
  next.end();
  await s3.result();
});

test("identical steers with the same timestamp are both injected", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [
    ...history,
    user("again", 5),
    user("again", 5),
  ]);
  const [first] = await run.waitForInjections(1);
  run.ack(first?.injectionId ?? "", "delivered");
  const [, second] = await run.waitForInjections(2);
  run.ack(second?.injectionId ?? "", "delivered");
  run.end();
  await s2.result();

  assert.equal(runs.length, 1);
});

test("a fresh run sends every trailing user message once", async () => {
  const sessionId = newSessionId();
  const s1 = startStream(sessionId, [user("one"), user("two")]);
  const run = await waitForRun(0);

  assert.equal(run.runText, "one\n\ntwo");
  run.end();
  await s1.result();
});

test("an owed run without a current checkpoint rebuilds history with the finished output", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1")]);
  await run.waitForInjections(1);
  run.checkpoint(checkpointState([]));
  run.text("finishing");
  run.end();

  const next = await waitForRun(1);
  assert.equal(next.runText, "s1");
  // hello, the tool batch and "finishing" form one turn; s1 is only the action.
  assert.equal(next.runRequest.conversationState?.turns.length, 1);
  next.end();
  await s2.result();
});

test("an owed run after a current checkpoint continues from it", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1")]);
  await run.waitForInjections(1);
  run.text("finishing");
  const marker = new Uint8Array([7, 7, 7]);
  run.checkpoint(checkpointState([marker, marker]));
  run.end();

  const next = await waitForRun(1);
  assert.equal(next.runText, "s1");
  assert.deepEqual(next.runRequest.conversationState?.turns, [marker, marker]);
  next.end();
  await s2.result();
});

test("Escape during a tool batch ends the run before Pi reattaches", async () => {
  const sessionId = newSessionId();
  const controller = new AbortController();
  const { run } = await runUntilToolBatch(
    sessionId,
    "call-1",
    controller.signal,
  );

  controller.abort();
  await settleEvents();

  assert.equal(run.aborted, true);
  assert.equal(hook.getLiveSession(sessionId), undefined);
});

test("a finished run no longer reacts to Pi's abort signal", async () => {
  const sessionId = newSessionId();
  const controller = new AbortController();
  const { run, history } = await runUntilToolBatch(
    sessionId,
    "call-1",
    controller.signal,
  );
  const s2 = startStream(sessionId, history);
  await settleEvents();

  run.text("done");
  run.end();
  assert.equal(textOf(await s2.result()), "done");
  controller.abort();
  assert.equal(run.aborted, false);
});

test("Escape racing a tool request rejects it and does not hang cleanup", async () => {
  const sessionId = newSessionId();
  const controller = new AbortController();
  const s1 = startStream(sessionId, [user("hello")], controller.signal);
  const run = await waitForRun(0);
  run.readTool("call-race");
  controller.abort();

  const aborted = await s1.result();

  assert.equal(aborted.stopReason, "aborted");
  await settleEvents();
  assert.equal(
    resolveToolResult({
      role: "toolResult",
      toolCallId: "call-race",
      toolName: "read",
      content: [],
      isError: false,
      timestamp: clock++,
    }),
    false,
  );
});

test("a tool exchange after a checkpoint makes the owed run rebuild history", async () => {
  const sessionId = newSessionId();
  const s1 = startStream(sessionId, [user("hello")]);
  const run = await waitForRun(0);
  run.text("looking");
  run.checkpoint(checkpointState([new Uint8Array([9])]));
  run.readTool("call-1");
  const assistant = await s1.result();
  const result = toolResult("call-1");
  await run.waitForExecResults(1);

  const s2 = startStream(sessionId, [
    user("hello", 1),
    assistant,
    result,
    user("s1"),
  ]);
  const [inject] = await run.waitForInjections(1);
  run.ack(inject?.injectionId ?? "", "rejected");
  run.end();

  const next = await waitForRun(1);
  assert.equal(next.runText, "s1");
  assert.notDeepEqual(next.runRequest.conversationState?.turns, [
    new Uint8Array([9]),
  ]);
  assert.equal(next.runRequest.conversationState?.turns.length, 1);
  next.end();
  await s2.result();
});

test("a steer delivered after a checkpoint makes the owed run rebuild history", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1"), user("s2")]);
  const [i1] = await run.waitForInjections(1);
  const marker = new Uint8Array([9]);
  run.checkpoint(checkpointState([marker]));
  run.ack(i1?.injectionId ?? "", "delivered");
  const [, i2] = await run.waitForInjections(2);
  run.ack(i2?.injectionId ?? "", "rejected");
  run.end();

  const next = await waitForRun(1);
  assert.equal(next.runText, "s2");
  assert.notDeepEqual(next.runRequest.conversationState?.turns, [marker]);
  next.end();
  await s2.result();
});

test("a reconnect resends a steer delivered after the last checkpoint", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1")]);
  const [inject] = await run.waitForInjections(1);
  run.checkpoint(checkpointState([]));
  run.ack(inject?.injectionId ?? "", "delivered");
  await settleEvents();
  run.fail(new LostConnection("simulated drop"));

  const retry = await waitForRun(1);
  assert.equal(
    retry.headers["x-original-request-id"],
    run.headers["x-original-request-id"],
  );
  const [resent] = await retry.waitForInjections(1);
  const payload = resent?.payload;
  assert.equal(payload?.case, "userContext");
  assert.equal(payload.value.userMessage?.text, "s1");
  retry.ack(resent?.injectionId ?? "", "delivered");
  retry.text("reply");
  retry.end();

  assert.equal(textOf(await s2.result()), "reply");
  assert.equal(runs.length, 2);
});

test("Escape while a steer write is stalled still ends the stream and the run", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  run.stallInput();
  const controller = new AbortController();
  const s2 = startStream(
    sessionId,
    [...history, user("s1"), user("s2")],
    controller.signal,
  );
  await settleEvents();

  controller.abort();
  const aborted = await s2.result();

  assert.equal(aborted.stopReason, "aborted");
  assert.equal(run.aborted, true);
  assert.equal(hook.getLiveSession(sessionId), undefined);
});

test("Escape during the owed-run handoff starts no new run", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const controller = new AbortController();
  const s2 = startStream(
    sessionId,
    [...history, user("s1")],
    controller.signal,
  );
  const [inject] = await run.waitForInjections(1);
  run.ack(inject?.injectionId ?? "", "rejected");
  run.checkpoint(checkpointState([]));
  onAppendEntry = () => controller.abort();
  run.end();

  const aborted = await s2.result();
  await settleEvents();

  assert.equal(aborted.stopReason, "aborted");
  assert.equal(runs.length, 1);
  assert.equal(hook.getLiveSession(sessionId), undefined);
});

test("Escape while the owed-run handoff waits on a stalled teardown ends the stream", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const controller = new AbortController();
  const s2 = startStream(
    sessionId,
    [...history, user("s1")],
    controller.signal,
  );
  const [inject] = await run.waitForInjections(1);
  run.ack(inject?.injectionId ?? "", "rejected");
  void lifecycle.runSessionTeardown(sessionId, () => new Promise(() => {}));
  run.end();
  await settleEvents();

  controller.abort();
  const aborted = await s2.result();

  assert.equal(aborted.stopReason, "aborted");
  assert.equal(runs.length, 1);
});

test("termination during the owed-run handoff starts no new run", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1")]);
  const [inject] = await run.waitForInjections(1);
  run.ack(inject?.injectionId ?? "", "rejected");
  run.checkpoint(checkpointState([]));
  onAppendEntry = () => {
    void lifecycle.terminateSession(sessionId, "Session ended");
  };
  run.end();

  const result = await s2.result();
  await settleEvents();

  assert.notEqual(result.stopReason, "stop");
  assert.equal(runs.length, 1);
  assert.equal(hook.getLiveSession(sessionId), undefined);
});

test("Escape while the tool-batch flush is stalled ends the stream", async () => {
  const sessionId = newSessionId();
  const controller = new AbortController();
  const s1 = startStream(sessionId, [user("hello")], controller.signal);
  const run = await waitForRun(0);
  void lifecycle.runSessionTeardown(sessionId, () => new Promise(() => {}));
  run.readTool("call-1");
  await settleEvents();

  controller.abort();
  const aborted = await s1.result();

  assert.equal(aborted.stopReason, "aborted");
  assert.equal(run.aborted, true);
});

test("termination while a fresh run is loading starts no run", async () => {
  const sessionId = newSessionId();
  const s1 = startStream(sessionId, [user("hello")]);
  void lifecycle.terminateSession(sessionId, "Session ended");

  const result = await s1.result();
  await settleEvents();

  assert.notEqual(result.stopReason, "stop");
  assert.equal(runs.length, 0);
  assert.equal(hook.getLiveSession(sessionId), undefined);
});

test("a new prompt after a stalled teardown starts once the teardown times out", async () => {
  const sessionId = newSessionId();
  lifecycle.setTeardownTimeoutMs(30);
  try {
    void lifecycle.runSessionTeardown(sessionId, () => new Promise(() => {}));
    const s1 = startStream(sessionId, [user("hello")]);
    const run = await waitForRun(0);
    run.text("hi");
    run.end();

    assert.equal(textOf(await s1.result()), "hi");
  } finally {
    lifecycle.setTeardownTimeoutMs(undefined);
  }
});

test("a startup that fails before registering leaves a newer run alone", async () => {
  const sessionId = newSessionId();
  const s1 = startStream(sessionId, [user("hello")]);
  void lifecycle.terminateSession(sessionId, "Session ended");
  const newer: import("../../src/provider/agent-stream-hook.js").LiveSession = {
    channel: new hook.LiveEventChannel(sessionId),
    cursorRunPromise: Promise.resolve(),
    flushSessionState: async () => {},
    abort: () => {},
    startTime: Date.now(),
    steers: createSteerDispatcher({ runId: "newer" }),
    seenUserMessageKeys: new Set(),
    hasCurrentCheckpoint: () => false,
    markCheckpointStale: () => {},
    linkAbort: () => {},
  };
  hook.setLiveSession(sessionId, newer);
  const pending = requestToolExecution(newer.channel, {
    toolCallId: "call-newer",
    cursorExecType: "read",
    piToolName: "read",
    piToolArgs: { path: "README.md" },
  });

  const failed = await s1.result();
  await settleEvents();

  assert.notEqual(failed.stopReason, "stop");
  assert.equal(runs.length, 0);
  assert.equal(hook.getLiveSession(sessionId), newer);
  assert.equal(
    resolveToolResult({
      role: "toolResult",
      toolCallId: "call-newer",
      toolName: "read",
      content: [],
      isError: false,
      timestamp: clock++,
    }),
    true,
  );
  await pending;
  hook.deleteLiveSession(sessionId);
});

test("a stale flush does not record a snapshot in Pi", async () => {
  const sessionId = newSessionId();
  let appended = 0;
  onAppendEntry = () => {
    appended++;
  };
  await runUntilToolBatch(sessionId);
  const run = runs[0];
  assert.ok(run);
  run.checkpoint(checkpointState([new Uint8Array([4])]));
  await settleEvents();
  const session = hook.getLiveSession(sessionId);
  assert.ok(session);
  appended = 0;

  await session.flushSessionState(() => false);
  assert.equal(appended, 0);
  await session.flushSessionState();
  assert.equal(appended, 1);
});

const truncated = () =>
  new ConnectError(
    "protocol error: missing EndStreamResponse",
    Code.InvalidArgument,
  );

async function readStreamErrorLog(
  sessionId: string,
): Promise<Array<Record<string, unknown>>> {
  const { streamErrorLogPath } = await import(
    "../../src/provider/stream-error-log.js"
  );
  const file = streamErrorLogPath(sessionId);
  assert.ok(file.startsWith(cacheDir));
  const text = await fs.readFile(file, "utf8").catch(() => "");
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

for (const order of ["checkpoint first", "turnEnded first"] as const) {
  test(`a stream cut after turnEnded and a current checkpoint ends the turn (${order})`, async () => {
    const sessionId = newSessionId();
    const s1 = startStream(sessionId, [user("hello")]);
    const run = await waitForRun(0);
    run.text("all done");
    if (order === "checkpoint first") {
      run.checkpoint(checkpointState([]));
      run.turnEnded();
    } else {
      run.turnEnded();
      run.checkpoint(checkpointState([]));
    }
    run.fail(truncated());

    const reply = await s1.result();
    assert.equal(reply.stopReason, "stop");
    assert.equal(textOf(reply), "all done");
    const [entry] = await readStreamErrorLog(sessionId);
    assert.equal(entry?.["outcome"], "completed");
    assert.equal(entry?.["turnEnded"], true);
    assert.equal(entry?.["checkpointCurrent"], true);
    assert.equal(entry?.["runId"], run.headers["x-original-request-id"]);
  });
}

test("a stream cut after turnEnded with a stale checkpoint fails the turn", async () => {
  const sessionId = newSessionId();
  const s1 = startStream(sessionId, [user("hello")]);
  const run = await waitForRun(0);
  run.checkpoint(checkpointState([]));
  run.text("more output");
  run.turnEnded();
  run.fail(truncated());

  const reply = await s1.result();
  assert.equal(reply.stopReason, "error");
  assert.match(reply.errorMessage ?? "", /missing EndStreamResponse/);
  const [entry] = await readStreamErrorLog(sessionId);
  assert.equal(entry?.["outcome"], "failed");
  assert.equal(entry?.["checkpointCurrent"], false);
});

test("a stream cut before turnEnded fails the turn", async () => {
  const sessionId = newSessionId();
  const s1 = startStream(sessionId, [user("hello")]);
  const run = await waitForRun(0);
  run.text("partial");
  run.checkpoint(checkpointState([]));
  run.fail(truncated());

  const reply = await s1.result();
  assert.equal(reply.stopReason, "error");
  const [entry] = await readStreamErrorLog(sessionId);
  assert.equal(entry?.["outcome"], "failed");
  assert.equal(entry?.["turnEnded"], false);
});

test("an unrelated error after turnEnded still fails the turn and is not logged", async () => {
  const sessionId = newSessionId();
  const s1 = startStream(sessionId, [user("hello")]);
  const run = await waitForRun(0);
  run.text("done");
  run.checkpoint(checkpointState([]));
  run.turnEnded();
  run.fail(new ConnectError("boom", Code.Internal));

  const reply = await s1.result();
  assert.equal(reply.stopReason, "error");
  assert.deepEqual(await readStreamErrorLog(sessionId), []);
});

test("an owed steer still runs next after a stream cut that ended the turn", async () => {
  const sessionId = newSessionId();
  const { run, history } = await runUntilToolBatch(sessionId);
  const s2 = startStream(sessionId, [...history, user("s1")]);
  await run.waitForInjections(1);
  run.text("finishing");
  run.checkpoint(checkpointState([]));
  run.turnEnded();
  run.fail(truncated());

  const next = await waitForRun(1);
  assert.equal(next.runText, "s1");
  next.text(" reply to s1");
  next.end();
  assert.equal(textOf(await s2.result()), "finishing reply to s1");
});
