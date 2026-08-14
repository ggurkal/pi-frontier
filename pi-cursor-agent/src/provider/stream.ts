import {
  type Api,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context,
  createAssistantMessageEventStream,
  type Model,
  type ToolCall as PiToolCall,
  type SimpleStreamOptions,
  type TextContent,
  type ThinkingContent,
} from "@earendil-works/pi-ai";
import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import type { ConversationStateStructure } from "../__generated__/agent/v1/agent_pb";
import {
  AskQuestionRejected,
  AskQuestionResult,
} from "../__generated__/agent/v1/ask_question_tool_pb";
import AgentService from "../api/agent-service";
import {
  LocalResourceProvider,
  type PiToolContext,
} from "../bridge/cursor-to-pi/local-resource-provider";
import {
  rejectPendingForSession,
  type ToolExecRequest,
} from "../bridge/cursor-to-pi/tool-bridge";
import { preparePiContext } from "../bridge/pi-context";
import {
  buildRunRequest,
  getContextTools,
} from "../bridge/pi-to-cursor/request-builder";
import { CURSOR_API_URL, CURSOR_CLIENT_VERSION } from "../lib/env";
import {
  AgentConnectClient,
  type CheckpointHandler,
  type InteractionListener,
} from "../vendor/agent-client";
import type {
  CoreInteractionQuery,
  CoreInteractionResponse,
  CoreInteractionUpdate,
} from "../vendor/agent-core";
import {
  CURSOR_STATE_ENTRY_TYPE,
  ensureAgentStore,
  evictAgentStore,
  persistAgentStore,
} from "./agent-store";
import {
  type ChannelEvent,
  type ContentEvent,
  consumeInputIntentForText,
  deleteLiveSession,
  getLiveSession,
  hasSeenContextUserMessageKey,
  LiveEventChannel,
  type LiveSession,
  markSeenContextUserMessageKey,
  queueInputIntent,
  setLiveSession,
} from "./agent-stream-hook";
import { toCursorId } from "./model-mapping";
import { createMessageDispatcher } from "./pending-messages";
import { type CursorStateStore, createOverlayState } from "./state";

function createCheckpointHandler(
  handler: (checkpoint: ConversationStateStructure) => void,
): CheckpointHandler {
  return {
    handleCheckpoint(
      _ctx: unknown,
      checkpoint: ConversationStateStructure,
    ): Promise<void> {
      handler(checkpoint);
      return Promise.resolve();
    },
  };
}

const QUERY_REJECTION_REASON = "Not supported";
const ABORT_ERROR_NAME = "AbortError";
const REQUEST_CANCELLED_MESSAGE = "Request cancelled";
const SESSION_ENDED_MESSAGE = "Session ended";
const REQUEST_ABORTED_MESSAGE = "Request aborted";
const USER_ABORTED_REQUEST_MESSAGE = "User aborted request";
const STEER_ABORT_GRACE_WINDOW_MS = 2_000;
const CURSOR_ABORT_BRACKET_PATTERN =
  /\[(?:canceled|aborted)\].*\[(?:canceled|aborted)\]/i;

function isAbortLikeError(error: unknown, signal?: AbortSignal): boolean {
  if (signal?.aborted) {
    return true;
  }

  if (error instanceof DOMException && error.name === ABORT_ERROR_NAME) {
    return true;
  }

  if (!(error instanceof Error)) {
    return false;
  }

  return (
    error.name === ABORT_ERROR_NAME ||
    error.message === REQUEST_CANCELLED_MESSAGE ||
    error.message === SESSION_ENDED_MESSAGE ||
    error.message === REQUEST_ABORTED_MESSAGE ||
    error.message === USER_ABORTED_REQUEST_MESSAGE ||
    error.message.includes(USER_ABORTED_REQUEST_MESSAGE) ||
    CURSOR_ABORT_BRACKET_PATTERN.test(error.message)
  );
}

async function awaitChannelEvent(
  channel: LiveEventChannel,
  signal?: AbortSignal,
): Promise<ChannelEvent | null> {
  if (!signal) {
    return channel.next();
  }

  if (signal.aborted) {
    throw signal.reason instanceof Error
      ? signal.reason
      : new Error(REQUEST_CANCELLED_MESSAGE);
  }

  let onAbort: (() => void) | undefined;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () =>
      reject(
        signal.reason instanceof Error
          ? signal.reason
          : new Error(REQUEST_CANCELLED_MESSAGE),
      );
    signal.addEventListener("abort", onAbort, { once: true });
  });

  try {
    return await Promise.race([channel.next(), aborted]);
  } finally {
    if (onAbort) {
      signal.removeEventListener("abort", onAbort);
    }
  }
}

function createInteractionListenerAdapter(
  onUpdate: (update: CoreInteractionUpdate) => void,
): InteractionListener {
  return {
    async sendUpdate(
      _ctx: unknown,
      update: CoreInteractionUpdate,
    ): Promise<void> {
      onUpdate(update);
    },
    async query(
      _ctx: unknown,
      query: CoreInteractionQuery,
    ): Promise<CoreInteractionResponse> {
      switch (query.type) {
        case "ask-question-request":
          return {
            result: new AskQuestionResult({
              result: {
                case: "rejected",
                value: new AskQuestionRejected({
                  reason: QUERY_REJECTION_REASON,
                }),
              },
            }),
          };
        case "web-search-request":
        case "web-fetch-request":
        case "exa-search-request":
        case "exa-fetch-request":
        case "switch-mode-request":
          return { approved: false, reason: QUERY_REJECTION_REASON };
        case "create-plan-request":
          return {
            result: {
              planUri: "",
              result: {
                case: "error",
                value: { error: QUERY_REJECTION_REASON },
              },
            },
          } as CoreInteractionResponse;
        case "setup-vm-environment-request":
          return {} as CoreInteractionResponse;
        default:
          return { approved: false, reason: QUERY_REJECTION_REASON };
      }
    },
  };
}

type CursorAssistantMessage = AssistantMessage & {
  duration?: number;
  ttft?: number;
};

interface LiveContentState {
  currentText: TextContent | null;
  currentThinking: ThinkingContent | null;
}

function finalizeText(
  state: LiveContentState,
  output: CursorAssistantMessage,
  stream: AssistantMessageEventStream,
): void {
  if (!state.currentText) return;
  stream.push({
    type: "text_end",
    contentIndex: output.content.indexOf(state.currentText),
    content: state.currentText.text,
    partial: output,
  });
  state.currentText = null;
}

function finalizeThinking(
  state: LiveContentState,
  output: CursorAssistantMessage,
  stream: AssistantMessageEventStream,
): void {
  if (!state.currentThinking) return;
  stream.push({
    type: "thinking_end",
    contentIndex: output.content.indexOf(state.currentThinking),
    content: state.currentThinking.thinking,
    partial: output,
  });
  state.currentThinking = null;
}

function pushContentEvent(
  event: ContentEvent,
  state: LiveContentState,
  output: CursorAssistantMessage,
  stream: AssistantMessageEventStream,
): void {
  switch (event.kind) {
    case "text-delta": {
      finalizeThinking(state, output, stream);
      if (!state.currentText) {
        state.currentText = { type: "text", text: "" };
        output.content.push(state.currentText);
        stream.push({
          type: "text_start",
          contentIndex: output.content.length - 1,
          partial: output,
        });
      }
      state.currentText.text += event.text;
      stream.push({
        type: "text_delta",
        contentIndex: output.content.indexOf(state.currentText),
        delta: event.text,
        partial: output,
      });
      break;
    }
    case "thinking-delta": {
      finalizeText(state, output, stream);
      if (!state.currentThinking) {
        state.currentThinking = { type: "thinking", thinking: "" };
        output.content.push(state.currentThinking);
        stream.push({
          type: "thinking_start",
          contentIndex: output.content.length - 1,
          partial: output,
        });
      }
      state.currentThinking.thinking += event.text;
      stream.push({
        type: "thinking_delta",
        contentIndex: output.content.indexOf(state.currentThinking),
        delta: event.text,
        partial: output,
      });
      break;
    }
    case "thinking-completed": {
      finalizeThinking(state, output, stream);
      break;
    }
  }
}

function finalizeAllContent(
  state: LiveContentState,
  output: CursorAssistantMessage,
  stream: AssistantMessageEventStream,
): void {
  finalizeText(state, output, stream);
  finalizeThinking(state, output, stream);
}

async function consumeUntilBoundary(
  channel: LiveEventChannel,
  output: CursorAssistantMessage,
  stream: AssistantMessageEventStream,
  usageState: { sawTokenDelta: boolean },
  setFirstTokenTime: () => void,
  signal?: AbortSignal,
): Promise<{
  reason: "toolUse" | "stop";
  tools: ToolExecRequest[];
}> {
  const contentState: LiveContentState = {
    currentText: null,
    currentThinking: null,
  };

  while (true) {
    const event = await awaitChannelEvent(channel, signal);

    if (event === null) {
      finalizeAllContent(contentState, output, stream);
      return { reason: "stop", tools: [] };
    }

    switch (event.kind) {
      case "content": {
        setFirstTokenTime();
        pushContentEvent(event.data, contentState, output, stream);
        break;
      }

      case "tool-exec-request": {
        finalizeAllContent(contentState, output, stream);
        return { reason: "toolUse", tools: [event.request] };
      }

      case "token-delta": {
        usageState.sawTokenDelta = true;
        output.usage.output += event.tokens;
        output.usage.totalTokens = output.usage.input + output.usage.output;
        break;
      }

      case "cursor-done": {
        finalizeAllContent(contentState, output, stream);
        return { reason: "stop", tools: [] };
      }

      case "cursor-error": {
        finalizeAllContent(contentState, output, stream);
        throw event.error instanceof Error
          ? event.error
          : new Error(String(event.error));
      }
    }
  }
}

function serializeContentBlocks(
  content: CursorAssistantMessage["content"],
): unknown[] {
  return content.map((block) => {
    switch (block.type) {
      case "text":
        return { type: "text", text: block.text };
      case "thinking":
        return { type: "thinking", thinking: block.thinking };
      case "toolCall":
        return {
          type: "toolCall",
          id: block.id,
          name: block.name,
          arguments: block.arguments,
        };
      default:
        return { type: (block as { type: string }).type };
    }
  });
}

function emitToolCalls(
  tools: ToolExecRequest[],
  output: CursorAssistantMessage,
  stream: AssistantMessageEventStream,
  state: CursorStateStore,
): void {
  for (const request of tools) {
    state.rememberToolCallMeta({
      toolCallId: request.toolCallId,
      cursorExecType: request.cursorExecType,
      piToolName: request.piToolName,
      piToolArgs: request.piToolArgs,
      assistantTimestamp: output.timestamp,
    });

    const block: PiToolCall = {
      type: "toolCall",
      id: request.toolCallId,
      name: request.piToolName,
      arguments: request.piToolArgs,
    };
    output.content.push(block);
    const idx = output.content.length - 1;
    stream.push({ type: "toolcall_start", contentIndex: idx, partial: output });
    stream.push({
      type: "toolcall_end",
      contentIndex: idx,
      toolCall: block,
      partial: output,
    });
  }
}

function extractUserMessageText(
  message: Context["messages"][number],
): string | null {
  if (message.role !== "user") {
    return null;
  }
  if (typeof message.content === "string") {
    const text = message.content.trim();
    return text.length > 0 ? text : null;
  }
  const text = message.content
    .filter(
      (block): block is { type: "text"; text: string } => block.type === "text",
    )
    .map((block) => block.text)
    .join("\n")
    .trim();
  return text.length > 0 ? text : null;
}

function buildContextUserMessageKey(
  message: Context["messages"][number],
  text: string,
): string {
  return `${message.timestamp}:${text}`;
}

function markContextUserMessagesSeen(
  sessionId: string,
  context: Context,
): void {
  for (const message of context.messages) {
    const text = extractUserMessageText(message);
    if (!text) continue;
    markSeenContextUserMessageKey(
      sessionId,
      buildContextUserMessageKey(message, text),
    );
  }
}

async function bridgeQueuedInteractiveInputs(
  sessionId: string,
  context: Context,
  liveSession: LiveSession,
): Promise<void> {
  for (const message of context.messages) {
    const text = extractUserMessageText(message);
    if (!text) {
      continue;
    }
    const key = buildContextUserMessageKey(message, text);
    if (hasSeenContextUserMessageKey(sessionId, key)) {
      continue;
    }
    const mode = consumeInputIntentForText(sessionId, text);
    if (!mode) {
      markSeenContextUserMessageKey(sessionId, key);
      continue;
    }
    try {
      if (mode === "followUp") {
        await liveSession.followUp(text);
      } else {
        liveSession.markSteerIntent?.();
        await liveSession.steer(text);
      }
      markSeenContextUserMessageKey(sessionId, key);
    } catch {
      // Preserve intent for retry on the next stream invocation.
      queueInputIntent(sessionId, text, mode);
    }
  }
}

export function streamCursorAgent(
  pi: ExtensionAPI,
  getCtx: () => ExtensionContext | null,
  state: CursorStateStore,
  model: Model<Api>,
  context: Context,
  options?: SimpleStreamOptions,
): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();

  (async () => {
    const sessionId = options?.sessionId ?? "default";

    const output: CursorAssistantMessage = {
      role: "assistant",
      content: [],
      api: model.api,
      provider: model.provider,
      model: model.id,
      usage: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: Date.now(),
    };
    let session: LiveSession | undefined;
    let effectiveSignal = options?.signal;
    let steerEpochAtStart = 0;

    try {
      session = getLiveSession(sessionId);

      if (!session) {
        const apiKey = options?.apiKey;
        if (!apiKey) {
          throw new Error(
            "Cursor API key (access token) is required. Run /login cursor or set CURSOR_ACCESS_TOKEN.",
          );
        }

        const agentStore = await ensureAgentStore(sessionId);
        const cwd = getCtx()?.cwd ?? process.cwd();
        const requestContextTools = getContextTools(context);

        const channel = new LiveEventChannel(sessionId);
        const sessionAbortController = new AbortController();
        const sessionSignal = sessionAbortController.signal;
        effectiveSignal = options?.signal ?? sessionSignal;

        const piToolCtx: PiToolContext = {
          cwd,
          signal: sessionSignal,
          getActiveTools: () => new Set(pi.getActiveTools()),
          getCtx,
          getChannel: () => channel,
        };

        const piContext = await preparePiContext(context.systemPrompt ?? "");

        const resources = new LocalResourceProvider({
          ctx: piToolCtx,
          requestContextTools,
          cursorRules: piContext.rules,
        });

        const blobStore = agentStore.getBlobStore();
        const cursorModelId = toCursorId(model.id, options?.reasoning);
        const overlayState = createOverlayState(state);
        const { initialRequest, conversationState } = buildRunRequest({
          model: { ...model, id: cursorModelId },
          context,
          conversationId: agentStore.getId(),
          blobStore,
          conversationState: agentStore.getConversationStateStructure(),
          mcpToolDefinitions: requestContextTools,
          state: overlayState,
          systemPromptOverride: piContext.cleanedPrompt,
        });
        agentStore.conversationStateStructure = conversationState;

        let lastFlushedRootBlobId: string | undefined;
        const flushSessionState = async () => {
          const snapshot = await persistAgentStore(sessionId);
          if (!snapshot || snapshot.latestRootBlobId === lastFlushedRootBlobId)
            return;
          lastFlushedRootBlobId = snapshot.latestRootBlobId;
          pi.appendEntry(CURSOR_STATE_ENTRY_TYPE, snapshot);
        };

        const handleInteractionUpdate = (update: CoreInteractionUpdate) => {
          switch (update.type) {
            case "text-delta":
              channel.push({
                kind: "content",
                data: { kind: "text-delta", text: update.text },
              });
              return;
            case "thinking-delta":
              channel.push({
                kind: "content",
                data: { kind: "thinking-delta", text: update.text },
              });
              return;
            case "thinking-completed":
              channel.push({
                kind: "content",
                data: { kind: "thinking-completed", text: "" },
              });
              return;
            case "token-delta":
              channel.push({ kind: "token-delta", tokens: update.tokens });
              return;
            case "user-message-appended": {
              const userMessage = update.userMessage as
                | { messageId?: unknown }
                | undefined;
              const messageId = userMessage?.messageId;
              if (typeof messageId === "string" && messageId.length > 0) {
                dispatcher.ackUserMessage(messageId);
              }
              return;
            }
            default:
              return;
          }
        };

        const baseUrl = model.baseUrl || CURSOR_API_URL;
        const agentService = new AgentService(baseUrl, {
          accessToken: apiKey,
          clientVersion: CURSOR_CLIENT_VERSION,
          clientType: "cli",
        });
        const connectClient = new AgentConnectClient(agentService.rpcClient);
        const interactionListener = createInteractionListenerAdapter(
          handleInteractionUpdate,
        );
        const checkpointHandler = createCheckpointHandler(
          (checkpoint: ConversationStateStructure) => {
            void agentStore.handleCheckpoint(null, checkpoint);
          },
        );
        checkpointHandler.getLatestCheckpoint = () =>
          agentStore.getConversationStateStructure();

        // Dispatcher for mid-stream user messages (steer / followUp).
        // It re-binds on every (re)connect so that messages stranded by a
        // dropped connection are redelivered as soon as a new stream opens.
        const dispatcher = createMessageDispatcher();
        let steerInFlight = 0;
        let lastSteerAttemptAt = 0;
        let steerEpoch = 0;

        const runOptions: Parameters<typeof connectClient.run>[1] = {
          interactionListener,
          resources,
          blobStore,
          checkpointHandler,
          signal: sessionSignal,
          onRequestStreamCreated: (stream) => {
            // Best-effort bind. Errors here would otherwise propagate into
            // connect.ts and abort the run; the dispatcher already handles
            // write failures internally by re-queueing messages.
            void dispatcher.bind(stream).catch(() => {});
          },
        };

        const cursorRunPromise = connectClient
          .run(initialRequest, runOptions)
          .then(() => channel.push({ kind: "cursor-done" }))
          .catch((error) => channel.push({ kind: "cursor-error", error }))
          .finally(() => {
            dispatcher.close();
            channel.markDone();
          });

        session = {
          channel,
          cursorRunPromise,
          flushSessionState,
          abort: (reason) => {
            dispatcher.close();
            sessionAbortController.abort(
              reason ? new Error(reason) : new Error("Session ended"),
            );
          },
          startTime: Date.now(),
          steer: async (text) => {
            steerEpoch++;
            steerInFlight++;
            lastSteerAttemptAt = Date.now();
            try {
              await dispatcher.steer(text);
            } finally {
              steerInFlight = Math.max(0, steerInFlight - 1);
              lastSteerAttemptAt = Date.now();
            }
          },
          followUp: (text) => dispatcher.followUp(text),
          getSteerEpoch: () => steerEpoch,
          markSteerIntent: () => {
            steerEpoch++;
            lastSteerAttemptAt = Date.now();
          },
          wasRecentSteerAttempt: () =>
            steerInFlight > 0 ||
            Date.now() - lastSteerAttemptAt <= STEER_ABORT_GRACE_WINDOW_MS,
        };
        setLiveSession(sessionId, session);
        markContextUserMessagesSeen(sessionId, context);
      }

      if (!session) {
        throw new Error(`Failed to initialize live session: ${sessionId}`);
      }
      const liveSession = session;
      steerEpochAtStart = liveSession.getSteerEpoch?.() ?? 0;
      await bridgeQueuedInteractiveInputs(sessionId, context, liveSession);

      const usageState = { sawTokenDelta: false };
      let firstTokenTimeCaptured = false;

      stream.push({ type: "start", partial: output });

      let result: Awaited<ReturnType<typeof consumeUntilBoundary>>;
      try {
        result = await consumeUntilBoundary(
          liveSession.channel,
          output,
          stream,
          usageState,
          () => {
            if (!firstTokenTimeCaptured) {
              firstTokenTimeCaptured = true;
              if (!liveSession.firstTokenTime) {
                liveSession.firstTokenTime = Date.now();
              }
            }
          },
          options?.signal,
        );
      } catch (error) {
        const steerAbort =
          isAbortLikeError(error, options?.signal) &&
          ((liveSession.getSteerEpoch?.() ?? steerEpochAtStart) >
            steerEpochAtStart ||
            liveSession.wasRecentSteerAttempt?.() === true);
        if (!steerAbort) {
          throw error;
        }

        // A steer handoff can abort the current request signal while the
        // underlying Cursor run is still alive. Continue consuming events from
        // the live channel so the steered turn can complete normally.
        result = await consumeUntilBoundary(
          liveSession.channel,
          output,
          stream,
          usageState,
          () => {
            if (!firstTokenTimeCaptured) {
              firstTokenTimeCaptured = true;
              if (!liveSession.firstTokenTime) {
                liveSession.firstTokenTime = Date.now();
              }
            }
          },
        );
      }

      output.duration = Date.now() - liveSession.startTime;
      if (liveSession.firstTokenTime) {
        output.ttft = liveSession.firstTokenTime - liveSession.startTime;
      }
      output.usage.cost = {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        total: 0,
      };

      if (result.reason === "toolUse" && result.tools.length > 0) {
        emitToolCalls(result.tools, output, stream, state);
        output.stopReason = "toolUse";

        state.rememberAssistantContent({
          timestamp: output.timestamp,
          blocks: serializeContentBlocks(output.content),
        });
        try {
          await session.flushSessionState();
        } catch {}

        stream.push({
          type: "done",
          reason: "toolUse",
          message: { ...output },
        });
      } else {
        output.stopReason = "stop";

        state.rememberAssistantContent({
          timestamp: output.timestamp,
          blocks: serializeContentBlocks(output.content),
        });
        let flushed = false;
        try {
          await session.flushSessionState();
          flushed = true;
        } catch {}
        deleteLiveSession(sessionId);
        await session.cursorRunPromise;
        await evictAgentStore(sessionId, { persist: !flushed }).catch(() => {});
        stream.push({ type: "done", reason: "stop", message: output });
      }
      stream.end();
    } catch (error) {
      const wasAborted = isAbortLikeError(error, effectiveSignal);
      const steerAbort =
        wasAborted &&
        ((session?.getSteerEpoch?.() ?? steerEpochAtStart) >
          steerEpochAtStart ||
          session?.wasRecentSteerAttempt?.() === true);
      if (steerAbort) {
        output.stopReason = "stop";
        stream.push({ type: "done", reason: "stop", message: output });
        stream.end();
        return;
      }

      if (wasAborted && session && !session.wasRecentSteerAttempt?.()) {
        session.abort(REQUEST_ABORTED_MESSAGE);
      }
      output.stopReason = wasAborted ? "aborted" : "error";
      output.errorMessage =
        error instanceof Error ? error.message : String(error);
      let flushed = false;
      try {
        if (session) {
          await session.flushSessionState();
          flushed = true;
          await session.cursorRunPromise.catch(() => {});
        }
      } catch {}
      deleteLiveSession(sessionId);
      rejectPendingForSession(
        sessionId,
        `Stream error: ${output.errorMessage}`,
      );
      await evictAgentStore(sessionId, { persist: !flushed }).catch(() => {});
      stream.push({
        type: "error",
        reason: wasAborted ? "aborted" : "error",
        error: { ...output },
      });
      stream.end();
    }
  })();

  return stream;
}
