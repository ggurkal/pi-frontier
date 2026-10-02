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
  rejectPendingForChannel,
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
import {
  type AgentRpcClient,
  ORIGINAL_REQUEST_ID_HEADER,
} from "../vendor/agent-client/connect";
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
  deleteLiveSession,
  getLiveSession,
  LiveEventChannel,
  type LiveSession,
  setLiveSession,
} from "./agent-stream-hook";
import { toCursorId } from "./model-mapping";
import { createSteerDispatcher } from "./pending-messages";
import {
  awaitSessionTeardown,
  beginSessionStartup,
  runSessionTeardown,
  terminateSession,
} from "./session-lifecycle";
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
          return { approved: true };
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

/**
 * Stable keys for the context's user messages. Equal timestamp and text get
 * an occurrence number, so repeated messages stay distinct.
 */
function contextUserMessageKeys(
  context: Context,
): Array<{ index: number; key: string; text: string }> {
  const occurrences = new Map<string, number>();
  const keys: Array<{ index: number; key: string; text: string }> = [];
  context.messages.forEach((message, index) => {
    const text = extractUserMessageText(message);
    if (!text) return;
    const base = `${message.timestamp}:${text}`;
    const occurrence = occurrences.get(base) ?? 0;
    occurrences.set(base, occurrence + 1);
    keys.push({ index, key: `${base}#${occurrence}`, text });
  });
  return keys;
}

/**
 * Inject the user messages Pi added since the run's last assistant message,
 * such as steers delivered after a tool batch.
 */
function adoptNewUserMessages(session: LiveSession, context: Context): void {
  let start = context.messages.length;
  while (start > 0 && context.messages[start - 1]?.role !== "assistant") {
    start--;
  }
  for (const { index, key, text } of contextUserMessageKeys(context)) {
    if (index < start || session.seenUserMessageKeys.has(key)) continue;
    session.seenUserMessageKeys.add(key);
    // Not awaited: a stalled write must not keep the stream from seeing an
    // abort.
    void session.steers.adopt(text);
  }
}

/** Updates that add to the conversation, making earlier checkpoints stale. */
const OUTPUT_UPDATE_TYPES = new Set<CoreInteractionUpdate["type"]>([
  "text-delta",
  "thinking-delta",
  "thinking-completed",
  "partial-tool-call",
  "tool-call-started",
  "tool-call-delta",
  "tool-call-completed",
  "user-message-appended",
]);

/**
 * History for a run that sends `owed` steers when no checkpoint covers the
 * finished run: the owed messages move to the action and the finished run's
 * output is appended.
 */
function contextForOwedRun(
  context: Context,
  output: CursorAssistantMessage,
  owed: string[],
): Context {
  const remaining = [...owed];
  const messages = [...context.messages];
  for (let i = messages.length - 1; i >= 0 && remaining.length > 0; i--) {
    const message = messages[i];
    const text = message ? extractUserMessageText(message) : null;
    const at = text === null ? -1 : remaining.lastIndexOf(text);
    if (at === -1) continue;
    remaining.splice(at, 1);
    messages.splice(i, 1);
  }
  messages.push({
    ...output,
    content: [...output.content],
    stopReason: "stop",
  });
  return { ...context, messages };
}

/** Rejects with `Request aborted` once `signal` aborts. */
async function unlessAborted<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
): Promise<T> {
  if (!signal) return promise;
  if (signal.aborted) throw new Error(REQUEST_ABORTED_MESSAGE);
  let onAbort: (() => void) | undefined;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(new Error(REQUEST_ABORTED_MESSAGE));
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    return await Promise.race([promise, aborted]);
  } finally {
    if (onAbort) signal.removeEventListener("abort", onAbort);
  }
}

type AgentRpcClientFactory = (
  baseUrl: string,
  accessToken: string,
) => AgentRpcClient;

const defaultAgentRpcClientFactory: AgentRpcClientFactory = (
  baseUrl,
  accessToken,
) =>
  new AgentService(baseUrl, {
    accessToken,
    clientVersion: CURSOR_CLIENT_VERSION,
    clientType: "cli",
  }).rpcClient;

let createAgentRpcClient = defaultAgentRpcClientFactory;

/** Test seam: replace the Cursor transport. Pass `undefined` to restore. */
export function setAgentRpcClientFactory(
  factory: AgentRpcClientFactory | undefined,
): void {
  createAgentRpcClient = factory ?? defaultAgentRpcClientFactory;
}

interface StartLiveSessionParams {
  pi: ExtensionAPI;
  getCtx: () => ExtensionContext | null;
  state: CursorStateStore;
  model: Model<Api>;
  context: Context;
  options: SimpleStreamOptions | undefined;
  sessionId: string;
}

interface RunOverrides {
  /** The run action; see `buildRunRequest`. */
  userText?: string;
  /** Rebuild history from the Pi context instead of the cached checkpoint. */
  ignoreCachedState?: boolean;
  /**
   * The finished session this run takes over from. The run is not started if
   * that session was terminated meanwhile.
   */
  replaces?: LiveSession;
}

async function startLiveSession(
  params: StartLiveSessionParams,
  overrides: RunOverrides = {},
): Promise<LiveSession> {
  const startup = beginSessionStartup(params.sessionId);
  try {
    return await startRun(params, overrides, startup.stillWanted);
  } finally {
    startup.end();
  }
}

async function startRun(
  params: StartLiveSessionParams,
  overrides: RunOverrides,
  stillWanted: () => boolean,
): Promise<LiveSession> {
  const { userText, ignoreCachedState, replaces } = overrides;
  const { pi, getCtx, state, model, context, options, sessionId } = params;
  const apiKey = options?.apiKey;
  if (!apiKey) {
    throw new Error(
      "Cursor API key (access token) is required. Run /login cursor or set CURSOR_ACCESS_TOKEN.",
    );
  }

  const signal = options?.signal;
  await unlessAborted(awaitSessionTeardown(sessionId), signal);
  const agentStore = await unlessAborted(ensureAgentStore(sessionId), signal);
  const cwd = getCtx()?.cwd ?? process.cwd();
  const requestContextTools = getContextTools(context);

  const channel = new LiveEventChannel(sessionId);
  const sessionAbortController = new AbortController();
  const sessionSignal = sessionAbortController.signal;

  const piToolCtx: PiToolContext = {
    cwd,
    signal: sessionSignal,
    getActiveTools: () => new Set(pi.getActiveTools()),
    getCtx,
    getChannel: () => channel,
  };

  const piContext = await unlessAborted(
    preparePiContext(context.systemPrompt ?? ""),
    signal,
  );

  // Last await before the run starts and registers: from here on nothing can
  // interleave, so a cancel either happened already or reaches the new run.
  if (
    signal?.aborted ||
    !stillWanted() ||
    (replaces && getLiveSession(sessionId) !== replaces)
  ) {
    throw new Error(REQUEST_ABORTED_MESSAGE);
  }

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
    conversationState: ignoreCachedState
      ? undefined
      : agentStore.getConversationStateStructure(),
    mcpToolDefinitions: requestContextTools,
    state: overlayState,
    systemPromptOverride: piContext.cleanedPrompt,
    ...(userText !== undefined ? { userText } : {}),
  });
  agentStore.conversationStateStructure = conversationState;

  let lastFlushedRootBlobId: string | undefined;
  const flushSessionState = async (isCurrent: () => boolean = () => true) => {
    const snapshot = await persistAgentStore(sessionId, isCurrent);
    if (
      !snapshot ||
      !isCurrent() ||
      snapshot.latestRootBlobId === lastFlushedRootBlobId
    )
      return;
    lastFlushedRootBlobId = snapshot.latestRootBlobId;
    pi.appendEntry(CURSOR_STATE_ENTRY_TYPE, snapshot);
  };

  const runId = crypto.randomUUID();
  const steers = createSteerDispatcher({ runId });
  let checkpointCurrent = false;

  const handleInteractionUpdate = (update: CoreInteractionUpdate) => {
    if (OUTPUT_UPDATE_TYPES.has(update.type)) checkpointCurrent = false;
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
      case "context-injection-state":
        if (update.state === "delivered") checkpointCurrent = false;
        steers.applyAck(update.injectionId, update.state);
        return;
      default:
        return;
    }
  };

  const connectClient = new AgentConnectClient(
    createAgentRpcClient(model.baseUrl || CURSOR_API_URL, apiKey),
  );
  const interactionListener = createInteractionListenerAdapter(
    handleInteractionUpdate,
  );
  const checkpointHandler = createCheckpointHandler(
    (checkpoint: ConversationStateStructure) => {
      checkpointCurrent = true;
      steers.commit();
      void agentStore.handleCheckpoint(null, checkpoint);
    },
  );
  checkpointHandler.getLatestCheckpoint = () =>
    agentStore.getConversationStateStructure();

  const runOptions: Parameters<typeof connectClient.run>[1] = {
    interactionListener,
    resources,
    blobStore,
    checkpointHandler,
    signal: sessionSignal,
    headers: {
      "x-request-id": runId,
      [ORIGINAL_REQUEST_ID_HEADER]: runId,
    },
    onRequestStreamCreated: (stream) => {
      // Rebinds on every (re)connect. Errors here would otherwise propagate
      // into connect.ts and abort the run.
      void steers.bind(stream).catch(() => {});
    },
  };

  const linkedSignals = new Map<AbortSignal, () => void>();
  const unlinkSignals = () => {
    for (const [signal, onAbort] of linkedSignals) {
      signal.removeEventListener("abort", onAbort);
    }
    linkedSignals.clear();
  };

  const cursorRunPromise = connectClient
    .run(initialRequest, runOptions)
    .then(() => channel.push({ kind: "cursor-done" }))
    .catch((error) => channel.push({ kind: "cursor-error", error }))
    .finally(() => {
      unlinkSignals();
      steers.close();
      channel.markDone();
    });

  const session: LiveSession = {
    channel,
    cursorRunPromise,
    flushSessionState,
    abort: (reason = "Session ended") => {
      steers.close();
      // The run cannot finish while an exec waits on a tool request.
      channel.markDone();
      rejectPendingForChannel(channel, reason);
      sessionAbortController.abort(new Error(reason));
    },
    startTime: Date.now(),
    steers,
    seenUserMessageKeys: new Set(
      contextUserMessageKeys(context).map(({ key }) => key),
    ),
    hasCurrentCheckpoint: () => checkpointCurrent,
    markCheckpointStale: () => {
      checkpointCurrent = false;
    },
    linkAbort: (signal) => {
      if (!signal || linkedSignals.has(signal) || channel.isDone) return;
      const onAbort = () => {
        if (getLiveSession(sessionId) === session) {
          void terminateSession(sessionId, REQUEST_ABORTED_MESSAGE);
        } else {
          session.abort(REQUEST_ABORTED_MESSAGE);
        }
      };
      if (signal.aborted) {
        onAbort();
        return;
      }
      linkedSignals.set(signal, onAbort);
      signal.addEventListener("abort", onAbort, { once: true });
    },
  };
  setLiveSession(sessionId, session);
  session.linkAbort(options?.signal);
  return session;
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
    const startParams: StartLiveSessionParams = {
      pi,
      getCtx,
      state,
      model,
      context,
      options,
      sessionId,
    };
    let session: LiveSession | undefined;

    try {
      session = getLiveSession(sessionId);
      if (session) {
        session.linkAbort(options?.signal);
        // Tool results and adopted steers reach the run from here on.
        session.markCheckpointStale();
        adoptNewUserMessages(session, context);
      } else {
        session = await startLiveSession(startParams);
      }

      const usageState = { sawTokenDelta: false };
      stream.push({ type: "start", partial: output });

      while (true) {
        const liveSession: LiveSession = session;
        let firstTokenTimeCaptured = false;
        const result = await consumeUntilBoundary(
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

        if (result.reason === "stop") {
          // Steers already in Pi's context that this run did not take go out
          // as the next run, answered in this same Pi message.
          const owed = liveSession.steers.settle();
          if (owed.length > 0) {
            // The finished run stays registered until its successor starts,
            // so a termination in between stops the handoff.
            void runSessionTeardown(sessionId, async (isCurrent) => {
              await liveSession.cursorRunPromise;
              await liveSession.flushSessionState(isCurrent).catch(() => {});
            });
            const checkpointed = liveSession.hasCurrentCheckpoint();
            session = await startLiveSession(
              checkpointed
                ? startParams
                : {
                    ...startParams,
                    context: contextForOwedRun(context, output, owed),
                  },
              {
                userText: owed.join("\n\n"),
                ignoreCachedState: !checkpointed,
                replaces: liveSession,
              },
            );
            continue;
          }
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
        } else {
          output.stopReason = "stop";
        }
        state.rememberAssistantContent({
          timestamp: output.timestamp,
          blocks: serializeContentBlocks(output.content),
        });

        if (result.reason === "toolUse") {
          // The run stays live; Pi reattaches after the tool batch.
          await unlessAborted(
            runSessionTeardown(sessionId, (isCurrent) =>
              liveSession.flushSessionState(isCurrent).catch(() => {}),
            ),
            options?.signal,
          );
          stream.push({
            type: "done",
            reason: output.stopReason,
            message: { ...output },
          });
          break;
        }

        if (getLiveSession(sessionId) === liveSession) {
          deleteLiveSession(sessionId);
          // Not awaited: the next run waits for it (`awaitSessionTeardown`).
          void runSessionTeardown(sessionId, async (isCurrent) => {
            let flushed = false;
            try {
              await liveSession.flushSessionState(isCurrent);
              flushed = true;
            } catch {}
            await liveSession.cursorRunPromise;
            await evictAgentStore(sessionId, {
              persist: !flushed,
              isCurrent,
            }).catch(() => {});
          });
        }
        stream.push({ type: "done", reason: "stop", message: output });
        break;
      }
      stream.end();
    } catch (error) {
      const wasAborted = isAbortLikeError(error, options?.signal);
      output.stopReason = wasAborted ? "aborted" : "error";
      output.errorMessage =
        error instanceof Error ? error.message : String(error);

      // Pi never aborts a stream to steer, so every abort is a real cancel.
      // A startup that failed before registering created nothing to clean
      // up. A session no longer registered was already torn down by whoever
      // deregistered it, and the store may belong to a newer run by now.
      const failed = session;
      if (failed) {
        failed.abort(REQUEST_ABORTED_MESSAGE);
        if (getLiveSession(sessionId) === failed) {
          deleteLiveSession(sessionId);
          // Not awaited: it can queue behind a stalled teardown, and the next
          // run waits for it anyway.
          void runSessionTeardown(sessionId, async (isCurrent) => {
            let flushed = false;
            try {
              await failed.flushSessionState(isCurrent);
              flushed = true;
            } catch {}
            await failed.cursorRunPromise.catch(() => {});
            await evictAgentStore(sessionId, {
              persist: !flushed,
              isCurrent,
            }).catch(() => {});
          });
        }
      }
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
