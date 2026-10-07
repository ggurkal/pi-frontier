import assert from "node:assert/strict";
import test from "node:test";
import {
  AgentServerMessage,
  ContextInjectionStateUpdate,
  ConversationStateStructure,
  HeartbeatUpdate,
  InteractionUpdate,
  TextDeltaUpdate,
  TokenDeltaUpdate,
  TurnEndedUpdate,
} from "../../../src/__generated__/agent/v1/agent_pb.js";
import { ExecServerMessage } from "../../../src/__generated__/agent/v1/exec_pb.js";
import { ReadArgs } from "../../../src/__generated__/agent/v1/read_exec_pb.js";
import { RequestContextArgs } from "../../../src/__generated__/agent/v1/request_context_exec_pb.js";
import { RunProgress } from "../../../src/vendor/agent-client/run-progress.js";

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
const heartbeat = () =>
  update({ case: "heartbeat", value: new HeartbeatUpdate() });
const checkpoint = () =>
  new AgentServerMessage({
    message: {
      case: "conversationCheckpointUpdate",
      value: new ConversationStateStructure(),
    },
  });
const read = (toolCallId: string) =>
  new AgentServerMessage({
    message: {
      case: "execServerMessage",
      value: new ExecServerMessage({
        id: 1,
        message: {
          case: "readArgs",
          value: new ReadArgs({ path: "a", toolCallId }),
        },
      }),
    },
  });

function feed(progress: RunProgress, ...messages: AgentServerMessage[]) {
  for (const message of messages) progress.onServerMessage(message);
  return progress.snapshot();
}

test("output since the checkpoint is cleared by a checkpoint", () => {
  const progress = new RunProgress();
  assert.equal(feed(progress, text()).outputSinceCheckpoint, true);
  const after = feed(progress, checkpoint());
  assert.equal(after.outputSinceCheckpoint, false);
  assert.equal(after.checkpointThisAttempt, true);
});

test("only a checkpoint after turnEnded is terminal", () => {
  assert.equal(
    feed(new RunProgress(), turnEnded(), checkpoint()).terminalCheckpoint,
    true,
  );
  const before = feed(new RunProgress(), checkpoint(), turnEnded());
  assert.equal(before.terminalCheckpoint, false);
  assert.equal(before.turnEnded, true);
});

test("heartbeats change nothing", () => {
  assert.deepEqual(
    feed(new RunProgress(), heartbeat()),
    new RunProgress().snapshot(),
  );
});

test("token deltas and injection acks stream but are not output", () => {
  const snapshot = feed(
    new RunProgress(),
    update({ case: "tokenDelta", value: new TokenDeltaUpdate({ tokens: 1 }) }),
    update({
      case: "contextInjectionState",
      value: new ContextInjectionStateUpdate(),
    }),
  );
  assert.equal(snapshot.streamed, true);
  assert.equal(snapshot.outputSinceCheckpoint, false);
});

test("a repeated tool call id is not new output", () => {
  const progress = new RunProgress();
  assert.equal(feed(progress, read("c1")).outputSinceCheckpoint, true);
  feed(progress, checkpoint());
  assert.equal(feed(progress, read("c1")).outputSinceCheckpoint, false);
  assert.equal(feed(progress, read("")).outputSinceCheckpoint, true);
});

test("non-Pi execs are not output", () => {
  const snapshot = feed(
    new RunProgress(),
    new AgentServerMessage({
      message: {
        case: "execServerMessage",
        value: new ExecServerMessage({
          id: 1,
          message: {
            case: "requestContextArgs",
            value: new RequestContextArgs(),
          },
        }),
      },
    }),
  );
  assert.equal(snapshot.outputSinceCheckpoint, false);
  assert.equal(snapshot.streamed, true);
});

test("a sent tool result is tracked until the next checkpoint", () => {
  const progress = new RunProgress();
  progress.onExecResultSent();
  assert.equal(progress.snapshot().stateSentSinceCheckpoint, true);
  assert.equal(feed(progress, checkpoint()).stateSentSinceCheckpoint, false);
});

test("a new attempt clears the flags but keeps seen tool call ids", () => {
  const progress = new RunProgress();
  feed(progress, read("c1"), turnEnded());
  progress.startAttempt();
  assert.deepEqual(progress.snapshot(), new RunProgress().snapshot());
  assert.equal(feed(progress, read("c1")).outputSinceCheckpoint, false);
});
