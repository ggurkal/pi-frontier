import type { WritableIterable } from "@connectrpc/connect/protocol";
import {
  AgentClientMessage,
  AgentMode,
  ConversationAction,
  InjectContextAction,
  UserContextInjection,
  UserMessage,
} from "../__generated__/agent/v1/agent_pb";

/** Server ack for an `injectContextAction`, from `ContextInjectionState`. */
export type ContextInjectionAck =
  | "queued"
  | "delivered"
  | "queuedForNextTurn"
  | "cancelled"
  | "rejected";

type SteerStatus = "pending" | "sent" | "queued" | "delivered" | "failed";

interface Steer {
  injectionId: string;
  text: string;
  status: SteerStatus;
  /** Delivered and included in a checkpoint the server sent afterwards. */
  committed: boolean;
}

/**
 * Steers for one Cursor run, written as `injectContextAction`.
 *
 * Pi owns the steer and follow-up queues and delivers a steer by adding it to
 * the context before reattaching to the run. `adopt` injects that message
 * into the open run. Messages the run does not deliver are returned by
 * `settle`, in context order, so they can be sent as the next run.
 *
 * One injection is in flight at a time, and the first one the run does not
 * accept stops injection, so the run never answers a steer ahead of an
 * earlier one.
 */
export interface SteerDispatcher {
  /**
   * Inject a user message Pi added to the context of a reattached stream.
   * Once the run is closed it is recorded as undelivered. The message is
   * recorded synchronously; the promise only tracks the write, which can
   * block on a stalled stream.
   */
  adopt(text: string): Promise<void>;
  /** Apply a `contextInjectionState` ack. */
  applyAck(injectionId: string, state: ContextInjectionAck): void;
  /** The server sent a checkpoint; delivered steers are now part of it. */
  commit(): void;
  /**
   * Call once the run has finished. Returns the adopted messages the run did
   * not deliver, in context order. Unacked injections count as undelivered.
   */
  settle(): string[];
  /**
   * Bind to a (re)created request stream and write pending injections.
   * A reconnect resumes from the last checkpoint, so steers delivered after
   * it are sent again. Other injections stay open for a late ack.
   */
  bind(stream: WritableIterable<AgentClientMessage>): Promise<void>;
  /** Stop writing. Recorded steers stay available to `settle`. */
  close(): void;
}

export interface SteerDispatcherDeps {
  /** Generation id of the run, sent as `expectedRunId`. */
  runId: string;
  generateId?: () => string;
}

function buildInjectContextAction(
  steer: Steer,
  messageId: string,
  expectedRunId: string,
): AgentClientMessage {
  return new AgentClientMessage({
    message: {
      case: "conversationAction",
      value: new ConversationAction({
        action: {
          case: "injectContextAction",
          value: new InjectContextAction({
            injectionId: steer.injectionId,
            expectedRunId,
            payload: {
              case: "userContext",
              value: new UserContextInjection({
                userMessage: new UserMessage({
                  text: steer.text,
                  messageId,
                  mode: AgentMode.AGENT,
                }),
              }),
            },
          }),
        },
      }),
    },
  });
}

export function createSteerDispatcher(
  deps: SteerDispatcherDeps,
): SteerDispatcher {
  const generateId = deps.generateId ?? (() => crypto.randomUUID());
  const steers: Steer[] = [];

  let stream: WritableIterable<AgentClientMessage> | undefined;
  let closed = false;
  let settled = false;

  const flush = async (): Promise<void> => {
    // Only the first undelivered steer may be written: an earlier one in
    // flight or failed holds back everything after it.
    const steer = steers.find((s) => s.status !== "delivered");
    const target = stream;
    if (steer?.status !== "pending" || !target || closed) return;
    steer.status = "sent";
    try {
      await target.write(
        buildInjectContextAction(steer, generateId(), deps.runId),
      );
    } catch {
      if (steer.status === "sent") steer.status = "pending";
      // A rebind during the write skipped this steer; resend it there.
      if (stream !== target) await flush();
    }
  };

  /** Resend steers a reconnect resumed from before, in order. */
  const rewindToLastCheckpoint = () => {
    const first = steers.findIndex(
      (s) => s.status === "delivered" && !s.committed,
    );
    if (first === -1) return;
    for (const steer of steers.slice(first)) {
      if (steer.status === "failed") continue;
      steer.status = "pending";
      steer.injectionId = generateId();
    }
  };

  return {
    async adopt(text: string): Promise<void> {
      const unusable = closed || settled;
      steers.push({
        injectionId: generateId(),
        text,
        status: unusable ? "failed" : "pending",
        committed: false,
      });
      if (!unusable) await flush();
    },

    applyAck(injectionId: string, state: ContextInjectionAck): void {
      if (settled) return;
      const steer = steers.find((s) => s.injectionId === injectionId);
      if (!steer || steer.status === "delivered" || steer.status === "failed") {
        return;
      }
      switch (state) {
        case "queued":
          steer.status = "queued";
          return;
        case "delivered":
          steer.status = "delivered";
          void flush();
          return;
        case "queuedForNextTurn":
        case "cancelled":
        case "rejected":
          steer.status = "failed";
          return;
      }
    },

    commit(): void {
      for (const steer of steers) {
        if (steer.status === "delivered") steer.committed = true;
      }
    },

    settle(): string[] {
      if (settled) return [];
      settled = true;
      return steers
        .filter((steer) => steer.status !== "delivered")
        .map((steer) => steer.text);
    },

    async bind(newStream: WritableIterable<AgentClientMessage>): Promise<void> {
      if (closed) {
        throw new Error("SteerDispatcher is closed");
      }
      if (stream) rewindToLastCheckpoint();
      stream = newStream;
      await flush();
    },

    close(): void {
      closed = true;
      stream = undefined;
    },
  };
}
