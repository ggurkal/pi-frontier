import assert from "node:assert/strict";
import test from "node:test";
import type { WritableIterable } from "@connectrpc/connect/protocol";
import {
  AgentClientMessage,
  AgentRunRequest,
  AgentServerMessage,
  ClientHeartbeat,
  ConversationAction,
  type InjectContextAction,
  InteractionUpdate,
  ResumeAction,
  TurnEndedUpdate,
} from "../../src/__generated__/agent/v1/agent_pb.js";
import { createSteerDispatcher } from "../../src/provider/pending-messages.js";
import {
  AgentConnectClient,
  type AgentConnectRunOptions,
  type AgentRpcClient,
} from "../../src/vendor/agent-client/connect.js";
import { LostConnection } from "../../src/vendor/agent-client/exec-controller.js";

function createMockStream() {
  const written: AgentClientMessage[] = [];
  const stream: WritableIterable<AgentClientMessage> = {
    write: async (msg: AgentClientMessage) => {
      written.push(msg);
    },
    [Symbol.asyncIterator]: () => {
      throw new Error("not used");
    },
    close: () => {},
  };
  return { stream, written };
}

function injectAction(
  msg: AgentClientMessage | undefined,
): InjectContextAction {
  assert.ok(msg, "expected an AgentClientMessage but got undefined");
  assert.equal(msg.message.case, "conversationAction");
  const action = (msg.message.value as ConversationAction).action;
  assert.equal(action.case, "injectContextAction");
  return action.value as InjectContextAction;
}

function injectedText(msg: AgentClientMessage | undefined): string {
  const payload = injectAction(msg).payload;
  assert.equal(payload.case, "userContext");
  return payload.value.userMessage?.text ?? "";
}

function idAt(written: AgentClientMessage[], index: number): string {
  return injectAction(written[index]).injectionId;
}

/** Let writes started by an ack finish. */
async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve();
}

function makeIdGen(prefix = "id"): () => string {
  let n = 0;
  return () => `${prefix}-${n++}`;
}

function createDispatcher() {
  return createSteerDispatcher({ runId: "run-1", generateId: makeIdGen() });
}

test("adopt writes one injectContextAction with the run id", async () => {
  const { stream, written } = createMockStream();
  const steers = createDispatcher();
  await steers.bind(stream);

  await steers.adopt("course-correct");

  assert.equal(written.length, 1);
  const inject = injectAction(written[0]);
  assert.equal(inject.expectedRunId, "run-1");
  assert.ok(inject.injectionId.length > 0);
  assert.equal(injectedText(written[0]), "course-correct");
});

test("adopt before the stream binds is written on bind", async () => {
  const steers = createDispatcher();
  await steers.adopt("early");

  const { stream, written } = createMockStream();
  await steers.bind(stream);

  assert.deepEqual(written.map(injectedText), ["early"]);
});

test("a delivered steer is not owed", async () => {
  const { stream, written } = createMockStream();
  const steers = createDispatcher();
  await steers.bind(stream);
  await steers.adopt("s1");

  steers.applyAck(idAt(written, 0), "queued");
  steers.applyAck(idAt(written, 0), "delivered");

  assert.deepEqual(steers.settle(), []);
});

for (const state of ["rejected", "cancelled", "queuedForNextTurn"] as const) {
  test(`a ${state} steer is owed`, async () => {
    const { stream, written } = createMockStream();
    const steers = createDispatcher();
    await steers.bind(stream);
    await steers.adopt("s1");

    steers.applyAck(idAt(written, 0), state);

    assert.deepEqual(steers.settle(), ["s1"]);
  });
}

test("one injection is in flight at a time", async () => {
  const { stream, written } = createMockStream();
  const steers = createDispatcher();
  await steers.bind(stream);
  await steers.adopt("s1");
  await steers.adopt("s2");
  assert.deepEqual(written.map(injectedText), ["s1"]);

  steers.applyAck(idAt(written, 0), "queued");
  assert.equal(written.length, 1);
  steers.applyAck(idAt(written, 0), "delivered");
  await settle();

  assert.deepEqual(written.map(injectedText), ["s1", "s2"]);
});

test("the first undelivered steer stops injection; the rest are owed in order", async () => {
  const { stream, written } = createMockStream();
  const steers = createDispatcher();
  await steers.bind(stream);
  await steers.adopt("s1");
  await steers.adopt("s2");
  await steers.adopt("s3");
  steers.applyAck(idAt(written, 0), "delivered");
  await settle();

  steers.applyAck(idAt(written, 1), "rejected");
  await settle();

  assert.equal(written.length, 2);
  assert.deepEqual(steers.settle(), ["s2", "s3"]);
});

test("queuedForNextTurn stops later injections; they are owed", async () => {
  const { stream, written } = createMockStream();
  const steers = createDispatcher();
  await steers.bind(stream);
  await steers.adopt("s1");
  steers.applyAck(idAt(written, 0), "queuedForNextTurn");

  await steers.adopt("s2");

  assert.equal(written.length, 1);
  assert.deepEqual(steers.settle(), ["s1", "s2"]);
});

test("identical texts are tracked per injection", async () => {
  const { stream, written } = createMockStream();
  const steers = createDispatcher();
  await steers.bind(stream);
  await steers.adopt("same");
  await steers.adopt("same");

  steers.applyAck(idAt(written, 0), "delivered");
  await settle();
  steers.applyAck(idAt(written, 1), "rejected");

  assert.equal(written.length, 2);
  assert.deepEqual(steers.settle(), ["same"]);
});

test("settle treats unacked injections as undelivered and runs once", async () => {
  const { stream, written } = createMockStream();
  const steers = createDispatcher();
  await steers.bind(stream);
  await steers.adopt("queued");
  await steers.adopt("waiting");
  steers.applyAck(idAt(written, 0), "queued");

  assert.deepEqual(steers.settle(), ["queued", "waiting"]);
  assert.deepEqual(steers.settle(), []);
});

test("acks and adopts after settle change nothing", async () => {
  const { stream, written } = createMockStream();
  const steers = createDispatcher();
  await steers.bind(stream);
  await steers.adopt("s1");

  assert.deepEqual(steers.settle(), ["s1"]);
  steers.applyAck(idAt(written, 0), "delivered");
  await steers.adopt("late");
  assert.equal(written.length, 1);
  assert.deepEqual(steers.settle(), []);
});

test("a rebind keeps an open injection open for a late ack", async () => {
  const a = createMockStream();
  const steers = createDispatcher();
  await steers.bind(a.stream);
  await steers.adopt("s1");
  await steers.adopt("s2");
  steers.applyAck(idAt(a.written, 0), "queued");

  const b = createMockStream();
  await steers.bind(b.stream);
  assert.equal(b.written.length, 0);

  steers.applyAck(idAt(a.written, 0), "delivered");
  await settle();
  assert.deepEqual(b.written.map(injectedText), ["s2"]);
});

test("a rebind resends steers delivered after the last checkpoint, in order", async () => {
  const a = createMockStream();
  const steers = createDispatcher();
  await steers.bind(a.stream);
  await steers.adopt("committed");
  steers.applyAck(idAt(a.written, 0), "delivered");
  steers.commit();
  await steers.adopt("uncommitted");
  steers.applyAck(idAt(a.written, 1), "delivered");
  await steers.adopt("in flight");
  await settle();
  assert.equal(a.written.length, 3);

  const b = createMockStream();
  await steers.bind(b.stream);
  assert.deepEqual(b.written.map(injectedText), ["uncommitted"]);
  steers.applyAck(idAt(a.written, 2), "delivered");
  assert.equal(b.written.length, 1);

  steers.applyAck(idAt(b.written, 0), "delivered");
  await settle();
  assert.deepEqual(b.written.map(injectedText), ["uncommitted", "in flight"]);
  steers.applyAck(idAt(b.written, 1), "delivered");
  assert.deepEqual(steers.settle(), []);
});

test("a failed write stays pending and is sent on the next bind", async () => {
  const failing: WritableIterable<AgentClientMessage> = {
    write: async () => {
      throw new Error("simulated inject failure");
    },
    [Symbol.asyncIterator]: () => {
      throw new Error("not used");
    },
    close: () => {},
  };
  const steers = createDispatcher();
  await steers.bind(failing);
  await steers.adopt("a");
  await steers.adopt("b");

  const healthy = createMockStream();
  await steers.bind(healthy.stream);
  assert.deepEqual(healthy.written.map(injectedText), ["a"]);

  steers.applyAck(idAt(healthy.written, 0), "delivered");
  await settle();
  assert.deepEqual(healthy.written.map(injectedText), ["a", "b"]);
});

test("a rebind replays a delivered steer even after a later one failed", async () => {
  const a = createMockStream();
  const steers = createDispatcher();
  await steers.bind(a.stream);
  steers.commit();
  await steers.adopt("s1");
  steers.applyAck(idAt(a.written, 0), "delivered");
  await settle();
  await steers.adopt("s2");
  steers.applyAck(idAt(a.written, 1), "rejected");

  const b = createMockStream();
  await steers.bind(b.stream);
  assert.deepEqual(b.written.map(injectedText), ["s1"]);

  steers.applyAck(idAt(b.written, 0), "delivered");
  await settle();
  assert.equal(b.written.length, 1);
  assert.deepEqual(steers.settle(), ["s2"]);
});

test("a write that fails after a rebind is resent on the new stream", async () => {
  let rejectWrite: ((error: Error) => void) | undefined;
  const stale: WritableIterable<AgentClientMessage> = {
    write: () =>
      new Promise<void>((_, reject) => {
        rejectWrite = reject;
      }),
    [Symbol.asyncIterator]: () => {
      throw new Error("not used");
    },
    close: () => {},
  };
  const steers = createDispatcher();
  await steers.bind(stale);
  const adopted = steers.adopt("s1");

  const fresh = createMockStream();
  await steers.bind(fresh.stream);
  assert.equal(fresh.written.length, 0);
  rejectWrite?.(new Error("stream closed"));
  await adopted;

  assert.deepEqual(fresh.written.map(injectedText), ["s1"]);
});

test("adopt after close is owed; bind after close throws", async () => {
  const steers = createDispatcher();
  steers.close();

  await steers.adopt("y");
  await assert.rejects(steers.bind(createMockStream().stream), /closed/);
  assert.deepEqual(steers.settle(), ["y"]);
});

// ---------------------------------------------------------------------------
// AgentConnectClient
// ---------------------------------------------------------------------------

function createInitialRunRequest(): AgentClientMessage {
  return new AgentClientMessage({
    message: {
      case: "runRequest",
      value: new AgentRunRequest({
        action: new ConversationAction({
          action: { case: "resumeAction", value: new ResumeAction() },
        }),
      }),
    },
  });
}

function createTurnEndedServerStream(): AsyncIterable<AgentServerMessage> {
  return (async function* () {
    yield new AgentServerMessage({
      message: {
        case: "interactionUpdate",
        value: new InteractionUpdate({
          message: { case: "turnEnded", value: new TurnEndedUpdate() },
        }),
      },
    });
  })();
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

test("onRequestStreamCreated writes cannot overtake initial runRequest", async () => {
  const captured: AgentClientMessage[] = [];
  let captureDoneResolve: (() => void) | undefined;
  const captureDone = new Promise<void>((resolve) => {
    captureDoneResolve = resolve;
  });

  const rpcClient: AgentRpcClient = {
    run(input) {
      void (async () => {
        try {
          for await (const message of input) {
            captured.push(message);
            if (captured.length >= 2) {
              break;
            }
          }
        } finally {
          captureDoneResolve?.();
        }
      })();

      return createTurnEndedServerStream();
    },
  };

  const client = new AgentConnectClient(rpcClient);

  await client.run(createInitialRunRequest(), {
    ...baseRunOptions(),
    onRequestStreamCreated: (stream) => {
      void stream.write(
        new AgentClientMessage({
          message: {
            case: "clientHeartbeat",
            value: new ClientHeartbeat(),
          },
        }),
      );
    },
  });

  await captureDone;
  assert.equal(captured.length, 2);
  assert.equal(captured[0]?.message.case, "runRequest");
  assert.equal(captured[1]?.message.case, "clientHeartbeat");
});

test("retries keep x-original-request-id and get a fresh x-request-id", async () => {
  const seen: Array<Record<string, string> | undefined> = [];
  const rpcClient: AgentRpcClient = {
    run(_input, options) {
      seen.push(options?.headers);
      if (seen.length === 1) {
        return {
          [Symbol.asyncIterator]: () => ({
            next: () => Promise.reject(new LostConnection("simulated drop")),
          }),
        };
      }
      return createTurnEndedServerStream();
    },
  };

  const client = new AgentConnectClient(rpcClient);
  await client.run(createInitialRunRequest(), {
    ...baseRunOptions(),
    backoffMs: () => 0,
    headers: { "x-request-id": "gen-1", "x-original-request-id": "gen-1" },
  });

  assert.equal(seen.length, 2);
  assert.equal(seen[0]?.["x-request-id"], "gen-1");
  assert.equal(seen[1]?.["x-original-request-id"], "gen-1");
  assert.notEqual(seen[1]?.["x-request-id"], "gen-1");
});
