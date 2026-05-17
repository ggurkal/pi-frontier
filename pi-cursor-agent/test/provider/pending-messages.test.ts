import assert from "node:assert/strict";
import test from "node:test";
import type { WritableIterable } from "@connectrpc/connect/protocol";
import {
  AgentClientMessage,
  AgentMode,
  AgentRunRequest,
  type AgentServerMessage,
  CancelAction,
  ClientHeartbeat,
  ConversationAction,
  ResumeAction,
  type UserMessageAction,
} from "../../src/__generated__/agent/v1/agent_pb.js";
import {
  createMessageDispatcher,
  PendingMessageQueue,
} from "../../src/provider/pending-messages.js";
import {
  AgentConnectClient,
  type AgentRpcClient,
} from "../../src/vendor/agent-client/connect.js";

// ---------------------------------------------------------------------------
// PendingMessageQueue
// ---------------------------------------------------------------------------

test("PendingMessageQueue defaults to one-at-a-time", () => {
  const q = new PendingMessageQueue();
  assert.equal(q.mode, "one-at-a-time");
});

test("PendingMessageQueue.enqueue then drain returns one item in one-at-a-time mode", () => {
  const q = new PendingMessageQueue("one-at-a-time");
  q.enqueue("a");
  q.enqueue("b");
  q.enqueue("c");

  assert.equal(q.size, 3);
  assert.deepEqual(q.drain(), ["a"]);
  assert.deepEqual(q.drain(), ["b"]);
  assert.deepEqual(q.drain(), ["c"]);
  assert.deepEqual(q.drain(), []);
});

test("PendingMessageQueue.drain returns all items in 'all' mode", () => {
  const q = new PendingMessageQueue("all");
  q.enqueue("a");
  q.enqueue("b");
  q.enqueue("c");

  assert.deepEqual(q.drain(), ["a", "b", "c"]);
  assert.equal(q.size, 0);
  assert.deepEqual(q.drain(), []);
});

test("PendingMessageQueue.hasItems and size reflect state", () => {
  const q = new PendingMessageQueue();
  assert.equal(q.hasItems(), false);
  assert.equal(q.size, 0);

  q.enqueue("x");
  assert.equal(q.hasItems(), true);
  assert.equal(q.size, 1);

  q.drain();
  assert.equal(q.hasItems(), false);
});

test("PendingMessageQueue.clear drops everything", () => {
  const q = new PendingMessageQueue();
  q.enqueue("a");
  q.enqueue("b");
  q.clear();
  assert.equal(q.size, 0);
  assert.deepEqual(q.drain(), []);
});

test("PendingMessageQueue.drain on empty queue is a no-op", () => {
  const q = new PendingMessageQueue();
  assert.deepEqual(q.drain(), []);
});

// ---------------------------------------------------------------------------
// MessageDispatcher
// ---------------------------------------------------------------------------

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

function unwrap(msg: AgentClientMessage | undefined): ConversationAction {
  assert.ok(msg, "expected an AgentClientMessage but got undefined");
  assert.equal(msg.message.case, "conversationAction");
  return msg.message.value as ConversationAction;
}

function actionCases(messages: AgentClientMessage[]): string[] {
  return messages.map((m) => unwrap(m).action.case ?? "");
}

function userText(msg: AgentClientMessage | undefined): string {
  const action = unwrap(msg);
  assert.equal(action.action.case, "userMessageAction");
  return (action.action.value as UserMessageAction).userMessage?.text ?? "";
}

function ids(written: AgentClientMessage[]): string[] {
  return written
    .map((m) => unwrap(m))
    .filter((a) => a.action.case === "userMessageAction")
    .map(
      (a) => (a.action.value as UserMessageAction).userMessage?.messageId ?? "",
    );
}

function makeIdGen(prefix = "id"): () => string {
  let n = 0;
  return () => `${prefix}-${n++}`;
}

test("steer() on bound stream sends CancelAction then UserMessageAction", async () => {
  const { stream, written } = createMockStream();
  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });
  await dispatcher.bind(stream);

  await dispatcher.steer("course-correct");

  assert.deepEqual(actionCases(written), ["cancelAction", "userMessageAction"]);
  assert.ok(unwrap(written[0]).action.value instanceof CancelAction);
  assert.equal(userText(written[1]), "course-correct");
});

test("followUp() on bound stream sends only UserMessageAction", async () => {
  const { stream, written } = createMockStream();
  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });
  await dispatcher.bind(stream);

  await dispatcher.followUp("also check tests");

  assert.deepEqual(actionCases(written), ["userMessageAction"]);
  assert.equal(userText(written[0]), "also check tests");
});

test("user messages use AgentMode.AGENT and unique ids", async () => {
  const { stream, written } = createMockStream();
  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });
  await dispatcher.bind(stream);

  await dispatcher.followUp("one");
  await dispatcher.followUp("two");

  for (const msg of written) {
    const action = unwrap(msg);
    if (action.action.case === "userMessageAction") {
      const um = (action.action.value as UserMessageAction).userMessage;
      assert.equal(um?.mode, AgentMode.AGENT);
    }
  }
  assert.deepEqual(ids(written), ["id-0", "id-1"]);
});

test("default messageId generator produces unique uuids", async () => {
  const { stream, written } = createMockStream();
  const dispatcher = createMessageDispatcher();
  await dispatcher.bind(stream);

  await dispatcher.followUp("a");
  await dispatcher.followUp("b");

  const generated = ids(written);
  assert.equal(generated.length, 2);
  assert.notEqual(generated[0], "");
  assert.notEqual(generated[1], "");
  assert.notEqual(generated[0], generated[1]);
});

test("steer/followUp queue messages when unbound and flush on bind()", async () => {
  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });

  // Not bound yet — messages just queue, no error.
  await dispatcher.steer("steer-while-down");
  await dispatcher.followUp("follow-while-down");
  assert.equal(dispatcher.pendingCount(), 2);

  const { stream, written } = createMockStream();
  await dispatcher.bind(stream);

  // Steering retry on rebind is delivered WITHOUT a fresh CancelAction
  // (the original cancel was never sent since we were unbound; resending
  // could unintentionally interrupt the resumed turn).
  assert.deepEqual(actionCases(written), [
    "userMessageAction",
    "userMessageAction",
  ]);
  assert.deepEqual(written.map(userText), [
    "steer-while-down",
    "follow-while-down",
  ]);
  assert.equal(dispatcher.pendingCount(), 0);
});

test("reconnect: bind() with a new stream redelivers pending messages", async () => {
  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });

  const a = createMockStream();
  await dispatcher.bind(a.stream);
  await dispatcher.steer("first");
  assert.deepEqual(actionCases(a.written), [
    "cancelAction",
    "userMessageAction",
  ]);

  // Simulate disconnect: unbind, then user steers again while down.
  dispatcher.unbind();
  await dispatcher.steer("during-outage");
  await dispatcher.followUp("queued-followup");
  assert.equal(a.written.length, 2); // nothing more on the old stream
  assert.equal(dispatcher.pendingCount(), 2);

  // Reconnect with a new stream.
  const b = createMockStream();
  await dispatcher.bind(b.stream);

  // No fresh cancel on rebind — both queued messages arrive as plain user
  // messages on the new stream.
  assert.deepEqual(actionCases(b.written), [
    "userMessageAction",
    "userMessageAction",
  ]);
  assert.deepEqual(b.written.map(userText), [
    "during-outage",
    "queued-followup",
  ]);
  assert.equal(dispatcher.pendingCount(), 0);
});

test("multiple reconnects flush only what is still queued", async () => {
  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });

  // First connect: deliver one followUp.
  const a = createMockStream();
  await dispatcher.bind(a.stream);
  await dispatcher.followUp("delivered");
  assert.equal(a.written.length, 1);

  // Disconnect + reconnect with nothing pending: no writes.
  dispatcher.unbind();
  const b = createMockStream();
  await dispatcher.bind(b.stream);
  assert.equal(b.written.length, 0);
});

test("steer() and followUp() reject after close()", async () => {
  const { stream } = createMockStream();
  const dispatcher = createMessageDispatcher();
  await dispatcher.bind(stream);
  dispatcher.close();

  await assert.rejects(
    () => dispatcher.steer("x"),
    /MessageDispatcher is closed/,
  );
  await assert.rejects(
    () => dispatcher.followUp("x"),
    /MessageDispatcher is closed/,
  );
});

test("close() drops pending messages", async () => {
  const dispatcher = createMessageDispatcher();
  await dispatcher.steer("queued");
  await dispatcher.followUp("queued");
  assert.equal(dispatcher.pendingCount(), 2);

  dispatcher.close();
  assert.equal(dispatcher.pendingCount(), 0);
});

test("close() is idempotent", () => {
  const dispatcher = createMessageDispatcher();
  dispatcher.close();
  dispatcher.close();
});

test("bind() after close() throws", async () => {
  const { stream } = createMockStream();
  const dispatcher = createMessageDispatcher();
  dispatcher.close();

  await assert.rejects(
    () => dispatcher.bind(stream),
    /MessageDispatcher is closed/,
  );
});

test("interleaved steer/followUp on bound stream produce correct wire pattern", async () => {
  const { stream, written } = createMockStream();
  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });
  await dispatcher.bind(stream);

  await dispatcher.steer("s1");
  await dispatcher.followUp("f1");
  await dispatcher.steer("s2");

  assert.deepEqual(actionCases(written), [
    "cancelAction",
    "userMessageAction",
    "userMessageAction",
    "cancelAction",
    "userMessageAction",
  ]);
});

test("if cancel write throws, the queued user message is still delivered", async () => {
  // Stream that only fails the first write (the cancel), then succeeds.
  const written: AgentClientMessage[] = [];
  let writes = 0;
  const stream: WritableIterable<AgentClientMessage> = {
    write: async (msg: AgentClientMessage) => {
      writes++;
      if (writes === 1) {
        throw new Error("simulated cancel failure");
      }
      written.push(msg);
    },
    [Symbol.asyncIterator]: () => {
      throw new Error("not used");
    },
    close: () => {},
  };

  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });
  await dispatcher.bind(stream);

  // Should NOT reject — cancel failure is swallowed.
  await dispatcher.steer("still-arrives");

  // The cancel was dropped but the user message landed.
  assert.deepEqual(actionCases(written), ["userMessageAction"]);
  assert.equal(userText(written[0]), "still-arrives");
});

test("if userMessage write throws, message is requeued and redelivered on next bind()", async () => {
  const failingStream: WritableIterable<AgentClientMessage> = {
    write: async () => {
      throw new Error("simulated message write failure");
    },
    [Symbol.asyncIterator]: () => {
      throw new Error("not used");
    },
    close: () => {},
  };

  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
  });
  await dispatcher.bind(failingStream);

  // Best-effort send: failed write should not reject and should remain queued.
  await dispatcher.followUp("retry-me");
  assert.equal(dispatcher.pendingCount(), 1);

  const healthy = createMockStream();
  await dispatcher.bind(healthy.stream);

  assert.deepEqual(actionCases(healthy.written), ["userMessageAction"]);
  assert.equal(userText(healthy.written[0]), "retry-me");
  assert.equal(dispatcher.pendingCount(), 0);
});

test("requeue preserves order for remaining batch items in 'all' mode", async () => {
  let writes = 0;
  const written: AgentClientMessage[] = [];
  const flakyStream: WritableIterable<AgentClientMessage> = {
    write: async (msg: AgentClientMessage) => {
      writes++;
      if (writes === 2) {
        throw new Error("fail on second write");
      }
      written.push(msg);
    },
    [Symbol.asyncIterator]: () => {
      throw new Error("not used");
    },
    close: () => {},
  };

  const dispatcher = createMessageDispatcher({
    generateMessageId: makeIdGen(),
    mode: "all",
  });

  await dispatcher.followUp("a");
  await dispatcher.followUp("b");
  await dispatcher.followUp("c");
  assert.equal(dispatcher.pendingCount(), 3);

  await dispatcher.bind(flakyStream);
  assert.deepEqual(written.map(userText), ["a"]);
  assert.equal(dispatcher.pendingCount(), 2);

  const healthy = createMockStream();
  await dispatcher.bind(healthy.stream);
  assert.deepEqual(healthy.written.map(userText), ["b", "c"]);
  assert.equal(dispatcher.pendingCount(), 0);
});

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

function createEmptyServerStream(): AsyncIterable<AgentServerMessage> {
  return (async function* () {})();
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

      return createEmptyServerStream();
    },
  };

  const client = new AgentConnectClient(rpcClient);

  await client.run(createInitialRunRequest(), {
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
