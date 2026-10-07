import {
  createWritableIterable,
  type WritableIterable,
} from "@connectrpc/connect/protocol";
import {
  AgentClientMessage,
  AgentRunRequest,
  type AgentServerMessage,
  ClientHeartbeat,
  ConversationAction,
  type ConversationStateStructure,
  type InteractionResponse,
  type ModelDetails,
  ResumeAction,
} from "../../__generated__/agent/v1/agent_pb";
import {
  ExecClientControlMessage,
  ExecClientMessage,
} from "../../__generated__/agent/v1/exec_pb";
import type { KvClientMessage } from "../../__generated__/agent/v1/kv_pb";
import type { McpTools } from "../../__generated__/agent/v1/mcp_pb";
import type { ResourceAccessor } from "../agent-exec/registry-resource-accessor";
import { SimpleControlledExecManager } from "../agent-exec/simple-controlled-exec-manager";
import { type BlobStore, ControlledKvManager } from "../agent-kv";
import { MapWritable } from "../utils";
import {
  CheckpointController,
  type CheckpointHandler,
} from "./checkpoint-controller";
import { ClientExecController } from "./exec-controller";
import {
  ClientInteractionController,
  type InteractionListener,
} from "./interaction-controller";
import {
  ConnectionStalledError,
  decideRetry,
  type FailureKind,
  isTransportError,
  NoResumeProgressError,
  type ProgressSnapshot,
  type RetryDecision,
  type StallInfo,
} from "./retry-policy";
import { RunProgress } from "./run-progress";
import { type SplitChannels, splitStream } from "./split-stream";
import {
  createStallDetector,
  STALL_THRESHOLD_MS,
  type StallDetector,
} from "./stall-detector";

export interface AgentRpcClient {
  run(
    input: AsyncIterable<AgentClientMessage>,
    options?: { signal?: AbortSignal; headers?: Record<string, string> },
  ): AsyncIterable<AgentServerMessage>;
  /** Drop the HTTP/2 connection so the next request dials a new one. */
  resetConnection?(): void;
}

export interface AgentConnectRunOptions {
  interactionListener: InteractionListener;
  resources: ResourceAccessor;
  blobStore: BlobStore;
  checkpointHandler: CheckpointHandler;
  signal?: AbortSignal;
  headers?: Record<string, string>;
  onConnectionStateChange?: (state: {
    state: "reconnecting" | "connected";
  }) => void;
  /**
   * Called when the request stream is created, allowing mid-stream messages
   * to be sent (e.g., steering and follow-up messages).
   */
  onRequestStreamCreated?: (
    stream: WritableIterable<AgentClientMessage>,
  ) => void;
  /** No inbound message for this long aborts the attempt; `<= 0` disables. */
  stallThresholdMs?: number;
  /** Delay before retry `attempt` (1-based). */
  backoffMs?: (attempt: number) => number;
  /** Every transport or stall failure and the decision taken, before acting on it. */
  onAttemptFailed?: (failure: AttemptFailure) => void;
}

export interface AttemptFailure {
  attempt: number;
  error: unknown;
  kind: FailureKind;
  decision: RetryDecision["decision"];
  reason: RetryDecision["reason"] | "checkpoint_unavailable";
  progress: ProgressSnapshot;
  stall?: StallInfo;
}

const HEARTBEAT_INTERVAL_MS = 5_000;
export const ORIGINAL_REQUEST_ID_HEADER = "x-original-request-id";

interface AttemptControl {
  /** Aborts when the session aborts or the attempt stalls. */
  signal: AbortSignal;
  detector: StallDetector;
  readonly stalled: boolean;
  readonly stallInfo: StallInfo | undefined;
  dispose(): void;
}

function createAttemptControl(
  sessionSignal: AbortSignal | undefined,
  thresholdMs: number,
): AttemptControl {
  const controller = new AbortController();
  const onSessionAbort = () => controller.abort(sessionSignal?.reason);
  if (sessionSignal?.aborted) onSessionAbort();
  else sessionSignal?.addEventListener("abort", onSessionAbort, { once: true });

  let stallInfo: StallInfo | undefined;
  const detector = createStallDetector({
    thresholdMs,
    onStall: (info) => {
      stallInfo = info;
      controller.abort(new ConnectionStalledError(info));
    },
  });
  return {
    signal: controller.signal,
    detector,
    get stalled() {
      return stallInfo !== undefined;
    },
    get stallInfo() {
      return stallInfo;
    },
    dispose() {
      detector.dispose();
      sessionSignal?.removeEventListener("abort", onSessionAbort);
    },
  };
}

/**
 * Retries keep `x-original-request-id` (the generation id the server matches
 * `expectedRunId` against) and get a fresh `x-request-id`, like Cursor desktop.
 */
function attemptHeaders(
  headers: Record<string, string> | undefined,
  attempt: number,
): { headers?: Record<string, string> } {
  if (!headers) return {};
  if (attempt === 0 || !headers[ORIGINAL_REQUEST_ID_HEADER]) {
    return { headers };
  }
  return { headers: { ...headers, "x-request-id": crypto.randomUUID() } };
}

async function backoff(
  attempt: number,
  signal?: AbortSignal,
  backoffMs?: (attempt: number) => number,
): Promise<void> {
  const delay = backoffMs?.(attempt) ?? Math.min(1_000 * 2 ** attempt, 30_000);
  return new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, delay);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

export class AgentConnectClient {
  private readonly client: AgentRpcClient;

  constructor(client: AgentRpcClient) {
    this.client = client;
  }

  /**
   * Runs the request, retrying transport failures where that cannot repeat
   * anything Pi already received:
   * - the turn ended and a checkpoint covers it: complete;
   * - output arrived since the last checkpoint: fail;
   * - a checkpoint arrived in the failed attempt: resume from it;
   * - nothing arrived: resend the current action.
   * Other errors, and any error after the session aborts, are rethrown.
   */
  async run(
    initialRequest: AgentClientMessage,
    options: AgentConnectRunOptions,
  ): Promise<void> {
    const runRequest = initialRequest.message.value as AgentRunRequest;

    let currentState = runRequest.conversationState;
    let currentAction = runRequest.action;
    if (!currentAction) {
      throw new Error("runRequest.action is required");
    }
    const modelDetails = runRequest.modelDetails;
    const mcpTools = runRequest.mcpTools;
    const conversationId = runRequest.conversationId;
    const progress = new RunProgress();
    let attempt = 0;
    let noProgressResumes = 0;
    let stallRetryStartedAt: number | undefined;

    while (true) {
      if (options.signal?.aborted) {
        throw new Error("Request cancelled");
      }
      progress.startAttempt();
      const attemptControl = createAttemptControl(
        options.signal,
        options.stallThresholdMs ?? STALL_THRESHOLD_MS,
      );

      try {
        const request = this.buildRequest(
          currentState,
          currentAction,
          modelDetails,
          mcpTools,
          conversationId,
        );

        await this.runInternal(
          request,
          { ...options, ...attemptHeaders(options.headers, attempt) },
          progress,
          attemptControl,
        );
        return;
      } catch (error) {
        if (options.signal?.aborted) throw error;
        const kind: FailureKind | undefined = attemptControl.stalled
          ? "stall"
          : isTransportError(error, progress.turnEnded)
            ? "transport"
            : undefined;
        if (!kind) throw error;
        const stall = attemptControl.stallInfo;

        const snapshot = progress.snapshot();
        const actionIsResume = currentAction.action.case === "resumeAction";
        const decided = decideRetry(kind, snapshot, {
          attempt,
          actionIsResume,
          noProgressResumes,
          stallRetryStartedAt,
          now: Date.now(),
        });
        let checkpoint: ConversationStateStructure | undefined;
        if (decided.decision === "resume") {
          checkpoint = options.checkpointHandler.getLatestCheckpoint?.();
        }
        const failure: AttemptFailure =
          decided.decision === "resume" && !checkpoint
            ? {
                attempt,
                error,
                kind,
                decision: "fail",
                reason: "checkpoint_unavailable",
                progress: snapshot,
                ...(stall ? { stall } : {}),
              }
            : {
                attempt,
                error,
                kind,
                ...decided,
                progress: snapshot,
                ...(stall ? { stall } : {}),
              };
        options.onAttemptFailed?.(failure);

        if (failure.decision === "complete") return;
        if (failure.decision === "fail") {
          this.client.resetConnection?.();
          if (failure.reason === "no_progress") {
            throw new NoResumeProgressError(error);
          }
          if (failure.reason === "stall_budget" && stall) {
            throw new ConnectionStalledError(
              stall,
              "Connection stalled repeatedly",
            );
          }
          throw error;
        }
        if (failure.decision === "resume") {
          currentState = checkpoint;
          currentAction = new ConversationAction({
            action: { case: "resumeAction", value: new ResumeAction() },
          });
          noProgressResumes = 0;
        } else if (actionIsResume && snapshot.streamed) {
          noProgressResumes++;
        }

        stallRetryStartedAt =
          kind === "stall" ? (stallRetryStartedAt ?? Date.now()) : undefined;
        options.onConnectionStateChange?.({ state: "reconnecting" });
        this.client.resetConnection?.();
        attempt++;
        await backoff(attempt, options.signal, options.backoffMs);
      } finally {
        attemptControl.dispose();
      }
    }
  }

  private buildRequest(
    conversationState: ConversationStateStructure | undefined,
    action: ConversationAction,
    modelDetails: ModelDetails | undefined,
    mcpTools: McpTools | undefined,
    conversationId: string | undefined,
  ): AgentClientMessage {
    return new AgentClientMessage({
      message: {
        case: "runRequest",
        value: new AgentRunRequest({
          ...(conversationState ? { conversationState } : {}),
          action,
          ...(modelDetails ? { modelDetails } : {}),
          ...(mcpTools ? { mcpTools } : {}),
          ...(conversationId ? { conversationId } : {}),
        }),
      },
    });
  }

  /**
   * Internal implementation that may throw any error type.
   * All errors are caught and converted at the public `run` boundary.
   */
  private async runInternal(
    initialRequest: AgentClientMessage,
    options: AgentConnectRunOptions,
    progress: RunProgress,
    attemptControl: AttemptControl,
  ): Promise<void> {
    const controlledExecManager = SimpleControlledExecManager.fromResources(
      options.resources,
    );

    const stallDetector = attemptControl.detector;

    const baseRequestStream = createWritableIterable<AgentClientMessage>();

    // Queue the initial run request first so replayed mid-stream messages
    // cannot overtake the run handshake on reconnect.
    void baseRequestStream.write(initialRequest);

    // Notify consumer after the initial request is queued.
    options.onRequestStreamCreated?.(baseRequestStream);

    const runOptions: {
      signal?: AbortSignal;
      headers?: Record<string, string>;
    } = {};
    runOptions.signal = attemptControl.signal;
    if (options.headers) runOptions.headers = options.headers;

    const response = this.client.run(baseRequestStream, runOptions);

    const channels: SplitChannels = splitStream(response, {
      detector: stallDetector,
      progress,
      signal: attemptControl.signal,
      onFirstMessage: () =>
        options.onConnectionStateChange?.({ state: "connected" }),
    });

    // Heartbeat sender using setTimeout (not setInterval)
    let heartbeatTimeout: ReturnType<typeof setTimeout> | undefined;

    const scheduleHeartbeat = () => {
      heartbeatTimeout = setTimeout(() => {
        baseRequestStream
          .write(
            new AgentClientMessage({
              message: {
                case: "clientHeartbeat",
                value: new ClientHeartbeat(),
              },
            }),
          )
          .then(() => {
            stallDetector.onClientSentHeartbeat();
            scheduleHeartbeat();
          })
          .catch(() => {});
      }, HEARTBEAT_INTERVAL_MS);
    };

    const clearHeartbeat = () => {
      if (heartbeatTimeout !== undefined) {
        clearTimeout(heartbeatTimeout);
        heartbeatTimeout = undefined;
      }
    };

    scheduleHeartbeat();

    try {
      const execOutputStream = new MapWritable<
        ExecClientMessage | ExecClientControlMessage,
        AgentClientMessage
      >(baseRequestStream, (message) => {
        if (message instanceof ExecClientMessage) {
          progress.onExecResultSent();
          return new AgentClientMessage({
            message: { case: "execClientMessage", value: message },
          });
        }
        if (message instanceof ExecClientControlMessage) {
          return new AgentClientMessage({
            message: { case: "execClientControlMessage", value: message },
          });
        }
        throw new Error("Unknown exec message type");
      });

      const kvOutputStream = new MapWritable<
        KvClientMessage,
        AgentClientMessage
      >(
        baseRequestStream,
        (message) =>
          new AgentClientMessage({
            message: { case: "kvClientMessage", value: message },
          }),
      );

      const queryResponseStream = new MapWritable<
        InteractionResponse,
        AgentClientMessage
      >(
        baseRequestStream,
        (response) =>
          new AgentClientMessage({
            message: { case: "interactionResponse", value: response },
          }),
      );

      const interactionController = new ClientInteractionController(
        channels.interactionStream,
        options.interactionListener,
        queryResponseStream,
        stallDetector,
      );

      const execController = new ClientExecController(
        channels.execStream,
        execOutputStream,
        controlledExecManager,
      );

      const kvManager = new ControlledKvManager(
        channels.kvStream,
        kvOutputStream,
        options.blobStore,
      );

      const checkpointController = new CheckpointController(
        channels.checkpointStream,
        options.checkpointHandler,
        null,
        () => interactionController.whenIdle(),
      );

      const ctx = null;

      const results = await Promise.allSettled([
        channels.done.finally(() => {
          clearHeartbeat();
          execOutputStream.close();
        }),
        execController.run(ctx),
        interactionController.run(ctx),
        checkpointController.run(),
        kvManager.run(ctx),
      ]);

      for (const result of results) {
        if (result.status === "rejected") {
          throw result.reason;
        }
      }
    } finally {
      clearHeartbeat();
      baseRequestStream.close();
    }
  }
}
