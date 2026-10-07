import assert from "node:assert/strict";
import test from "node:test";
import {
  AgentClientMessage,
  AgentRunRequest,
  AgentServerMessage,
  ConversationAction,
  ConversationStateStructure,
  InteractionUpdate,
  TextDeltaUpdate,
  TurnEndedUpdate,
  UserMessage,
  UserMessageAction,
} from "../../../src/__generated__/agent/v1/agent_pb.js";
import {
  AgentConnectClient,
  type AgentConnectRunOptions,
  type AgentRpcClient,
} from "../../../src/vendor/agent-client/connect.js";

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
  const client: AgentRpcClient = {
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
  return { client, calls };
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
