import type { WritableIterable } from "@connectrpc/connect/protocol";
import {
  AgentClientMessage,
  AgentMode,
  CancelAction,
  ConversationAction,
  UserMessage,
  UserMessageAction,
} from "../__generated__/agent/v1/agent_pb";
import type { StreamingBehavior } from "./agent-stream-hook";

/**
 * Drain mode for {@link PendingMessageQueue}.
 *
 * - `"all"`: drain everything in one shot.
 * - `"one-at-a-time"`: drain returns at most one message per call.
 *
 * Mirrors Pi's `QueueMode`. Hardcoded to `"one-at-a-time"` in the dispatcher
 * to match Pi's default behavior.
 */
export type QueueMode = "all" | "one-at-a-time";

/**
 * Default drain mode for queues owned by the dispatcher.
 *
 * Hardcoded to match Pi's default. Not user-configurable: by the time
 * messages reach this layer, Pi's own queue has already applied the
 * user's preferred mode.
 */
const DEFAULT_QUEUE_MODE: QueueMode = "one-at-a-time";

/**
 * Client-side queue of pending text messages waiting to be delivered to
 * the Cursor wire protocol.
 *
 * Modeled directly on Pi's `PendingMessageQueue` so that the semantics
 * are predictable for callers familiar with Pi's `Agent` class.
 */
export class PendingMessageQueue {
  private messages: string[] = [];

  constructor(public readonly mode: QueueMode = DEFAULT_QUEUE_MODE) {}

  enqueue(text: string): void {
    this.messages.push(text);
  }

  hasItems(): boolean {
    return this.messages.length > 0;
  }

  drain(): string[] {
    if (this.mode === "all") {
      return this.messages.splice(0, this.messages.length);
    }

    const [first] = this.messages.splice(0, 1);

    if (first === undefined) {
      return [];
    }

    return [first];
  }

  clear(): void {
    this.messages = [];
  }

  prepend(items: string[]): void {
    if (items.length === 0) {
      return;
    }

    this.messages.unshift(...items);
  }

  get size(): number {
    return this.messages.length;
  }
}

export interface MessageDispatcher {
  /**
   * Steer the agent mid-stream: interrupt current generation and inject a
   * user message. If the stream is bound, sends `CancelAction` immediately
   * and attempts to flush the steering queue. If the stream is unbound
   * (e.g. mid-reconnect), the message is queued and delivered when the
   * stream rebinds.
   *
   * @throws if the dispatcher has been closed.
   */
  steer(text: string): Promise<void>;
  /**
   * Append a follow-up user message. The Cursor server handles delivery
   * timing: the message is processed after the current generation
   * completes. If the stream is unbound, the message is queued.
   *
   * @throws if the dispatcher has been closed.
   */
  followUp(text: string): Promise<void>;
  /**
   * Bind the dispatcher to a (re)created request stream and redeliver any
   * pending messages. Called on initial connect AND on each reconnect.
   *
   * Note: on rebind, queued steering messages are delivered as plain user
   * messages without a fresh `CancelAction`. Server cancellation state
   * from before the disconnect is opaque to us; resending a cancel could
   * unintentionally interrupt the resumed turn.
   */
  bind(stream: WritableIterable<AgentClientMessage>): Promise<void>;
  /**
   * Detach from the current stream. Pending messages remain queued and
   * will be delivered when the dispatcher is rebound.
   */
  unbind(): void;
  /**
   * Close the dispatcher. Drops pending messages and rejects subsequent
   * `steer` / `followUp` calls.
   */
  close(): void;
  /**
   * Mark a previously sent user message as acknowledged by the server.
   * Used to avoid replaying already-applied steer messages on reconnect.
   */
  ackUserMessage(messageId: string): void;
  /** Total messages waiting across both queues. Useful for observability. */
  pendingCount(): number;
}

export interface MessageDispatcherDeps {
  /** UUID generator. Injected so tests can produce deterministic ids. */
  generateMessageId?: () => string;
  /** Drain mode for the underlying queues. Defaults to `"one-at-a-time"`. */
  mode?: QueueMode;
}

function buildUserMessage(text: string, messageId: string): UserMessage {
  return new UserMessage({
    text,
    messageId,
    mode: AgentMode.AGENT,
  });
}

function buildUserMessageAction(
  text: string,
  messageId: string,
): AgentClientMessage {
  return new AgentClientMessage({
    message: {
      case: "conversationAction",
      value: new ConversationAction({
        action: {
          case: "userMessageAction",
          value: new UserMessageAction({
            userMessage: buildUserMessage(text, messageId),
          }),
        },
      }),
    },
  });
}

function buildCancelAction(): AgentClientMessage {
  return new AgentClientMessage({
    message: {
      case: "conversationAction",
      value: new ConversationAction({
        action: { case: "cancelAction", value: new CancelAction() },
      }),
    },
  });
}

/**
 * Create a {@link MessageDispatcher} backed by two {@link PendingMessageQueue}s.
 *
 * The dispatcher is the single source of truth for how `steer` / `followUp`
 * map onto the Cursor wire protocol:
 *
 * - `steer(text)`: emits `CancelAction` then drains the steering queue,
 *   sending each pending message as a `UserMessageAction`.
 * - `followUp(text)`: drains the follow-up queue, sending each pending
 *   message as a `UserMessageAction` (no cancel).
 *
 * The dispatcher must be bound to a request stream before it can deliver
 * messages. While unbound, messages accumulate in their respective queues
 * and are flushed on the next `bind()` call. This is how reconnects are
 * handled: `connect.ts` calls `onRequestStreamCreated` on every (re)connect,
 * which calls `bind()` to flush any messages that were stranded by the
 * previous disconnect.
 */
export function createMessageDispatcher(
  deps: MessageDispatcherDeps = {},
): MessageDispatcher {
  const generateMessageId =
    deps.generateMessageId ?? (() => crypto.randomUUID());
  const mode = deps.mode ?? DEFAULT_QUEUE_MODE;

  const steeringQueue = new PendingMessageQueue(mode);
  const followUpQueue = new PendingMessageQueue(mode);
  const inflightSteering = new Map<string, string>();

  let stream: WritableIterable<AgentClientMessage> | undefined;
  let closed = false;

  const ensureUsable = () => {
    if (closed) {
      throw new Error("MessageDispatcher is closed");
    }
  };

  const flushQueue = async (
    queue: PendingMessageQueue,
    kind: StreamingBehavior,
  ): Promise<void> => {
    if (!stream) return;
    while (queue.hasItems() && !closed && stream) {
      const batch = queue.drain();
      for (let i = 0; i < batch.length; i++) {
        const text = batch[i];
        if (text === undefined) continue;
        if (!stream || closed) {
          // Stream went away mid-drain. Put unsent messages back at the
          // front so ordering is preserved.
          queue.prepend(batch.slice(i));
          return;
        }
        try {
          const messageId = generateMessageId();
          await stream.write(buildUserMessageAction(text, messageId));
          if (kind === "steer") {
            // Consider steer delivery optimistic until user-message-appended
            // arrives from interaction updates.
            inflightSteering.set(messageId, text);
          }
        } catch {
          // Write failed (e.g. transport dropped). Requeue this and remaining
          // items in original order; they'll be retried on next bind().
          queue.prepend(batch.slice(i));
          return;
        }
      }
    }
  };

  return {
    async steer(text: string): Promise<void> {
      ensureUsable();
      steeringQueue.enqueue(text);
      if (stream) {
        // Best-effort: interrupt the active generation. If this write
        // fails, the user message stays in the queue for next bind().
        try {
          await stream.write(buildCancelAction());
        } catch {
          // Swallow: cancel is best-effort. The queued user message
          // will still be redelivered on the next bind().
        }
      }
      await flushQueue(steeringQueue, "steer");
    },

    async followUp(text: string): Promise<void> {
      ensureUsable();
      followUpQueue.enqueue(text);
      await flushQueue(followUpQueue, "followUp");
    },

    async bind(newStream: WritableIterable<AgentClientMessage>): Promise<void> {
      ensureUsable();
      const replacingStream = Boolean(stream && stream !== newStream);
      if (replacingStream && inflightSteering.size > 0) {
        // Connection was replaced before we saw user-message-appended ACKs.
        // Replay optimistic steers in original send order.
        steeringQueue.prepend([...inflightSteering.values()]);
        inflightSteering.clear();
      }
      stream = newStream;
      // Redeliver anything that piled up while we were disconnected.
      // Steering retries are sent as plain user messages (see interface docs).
      await flushQueue(steeringQueue, "steer");
      await flushQueue(followUpQueue, "followUp");
    },

    unbind(): void {
      if (inflightSteering.size > 0) {
        steeringQueue.prepend([...inflightSteering.values()]);
        inflightSteering.clear();
      }
      stream = undefined;
    },

    close(): void {
      closed = true;
      stream = undefined;
      steeringQueue.clear();
      followUpQueue.clear();
      inflightSteering.clear();
    },

    ackUserMessage(messageId: string): void {
      inflightSteering.delete(messageId);
    },

    pendingCount(): number {
      return steeringQueue.size + followUpQueue.size;
    },
  };
}
