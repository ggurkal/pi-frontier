import assert from "node:assert/strict";
import test from "node:test";
import {
  AgentServerMessage,
  ConversationStateStructure,
  HeartbeatUpdate,
  InteractionUpdate,
  TextDeltaUpdate,
  TurnEndedUpdate,
} from "../../../src/__generated__/agent/v1/agent_pb.js";
import { StreamEndedWithoutTurnEndedError } from "../../../src/vendor/agent-client/retry-policy.js";
import { RunProgress } from "../../../src/vendor/agent-client/run-progress.js";
import {
  type StallDetector,
  splitStream,
} from "../../../src/vendor/agent-client/split-stream.js";

const update = (message: InteractionUpdate["message"]) =>
  new AgentServerMessage({
    message: {
      case: "interactionUpdate",
      value: new InteractionUpdate({ message }),
    },
  });
const text = () =>
  update({ case: "textDelta", value: new TextDeltaUpdate({ text: "x" }) });
const turnEnded = () =>
  update({ case: "turnEnded", value: new TurnEndedUpdate() });
const checkpoint = () =>
  new AgentServerMessage({
    message: {
      case: "conversationCheckpointUpdate",
      value: new ConversationStateStructure(),
    },
  });

function detector(calls: string[] = []): StallDetector {
  return {
    onServerSentHeartbeat: () => calls.push("server-heartbeat"),
    reset: (type, label) => calls.push(`${type}:${label}`),
    onStreamEnded: () => calls.push("ended"),
    onClientSentHeartbeat: () => {},
    setPaused: () => {},
    dispose: () => {},
  };
}

async function* from(messages: AgentServerMessage[]) {
  yield* messages;
}

function drain(channels: ReturnType<typeof splitStream>) {
  for (const stream of [
    channels.interactionStream,
    channels.execStream,
    channels.checkpointStream,
    channels.kvStream,
  ]) {
    void (async () => {
      for await (const _ of stream);
    })();
  }
}

test("a clean end without turnEnded rejects", async () => {
  const channels = splitStream(from([text()]), {
    detector: detector(),
    progress: new RunProgress(),
  });
  drain(channels);
  await assert.rejects(channels.done, StreamEndedWithoutTurnEndedError);
});

test("a clean end after turnEnded resolves", async () => {
  const channels = splitStream(from([text(), turnEnded()]), {
    detector: detector(),
    progress: new RunProgress(),
  });
  drain(channels);
  await channels.done;
});

test("a clean end without turnEnded after an abort resolves", async () => {
  const controller = new AbortController();
  controller.abort();
  const channels = splitStream(from([text()]), {
    detector: detector(),
    progress: new RunProgress(),
    signal: controller.signal,
  });
  drain(channels);
  await channels.done;
});

test("progress sees messages in wire order", async () => {
  const seen: string[] = [];
  class SpyProgress extends RunProgress {
    override onServerMessage(message: AgentServerMessage): void {
      const msg = message.message;
      seen.push(
        msg.case === "interactionUpdate"
          ? (msg.value.message.case ?? "")
          : (msg.case ?? ""),
      );
      super.onServerMessage(message);
    }
  }
  const channels = splitStream(from([text(), checkpoint(), turnEnded()]), {
    detector: detector(),
    progress: new SpyProgress(),
  });
  drain(channels);
  await channels.done;
  assert.deepEqual(seen, [
    "textDelta",
    "conversationCheckpointUpdate",
    "turnEnded",
  ]);
});

test("server heartbeats count as inbound activity", async () => {
  const calls: string[] = [];
  const channels = splitStream(
    from([
      update({ case: "heartbeat", value: new HeartbeatUpdate() }),
      turnEnded(),
    ]),
    { detector: detector(calls), progress: new RunProgress() },
  );
  drain(channels);
  await channels.done;
  assert.deepEqual(calls.slice(0, 2), [
    "server-heartbeat",
    "inbound_message:heartbeat",
  ]);
});
