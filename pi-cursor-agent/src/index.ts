import type {
  Api,
  OAuthCredentials,
  OAuthLoginCallbacks,
} from "@earendil-works/pi-ai";
import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import AiService from "./api/ai-service";
import Auth from "./api/auth";
import { resolveToolResult } from "./bridge/cursor-to-pi/tool-bridge";
import AuthManager from "./lib/auth";
import {
  CURSOR_API_URL,
  CURSOR_CLIENT_VERSION,
  CURSOR_WEBSITE_URL,
} from "./lib/env";
import {
  getSkipApprovalEnabled,
  parseSkipApprovalArgs,
  resolveSkipApprovalEnabled,
  SKIP_APPROVAL_OPTIONS,
  setSkipApprovalEnabled,
  updateSkipApprovalStatus,
} from "./lib/skip-approval";
import { restoreAgentStoreFromBranch } from "./provider/agent-store";
import {
  getLiveSession,
  queueInputIntent,
  toStreamingBehavior,
} from "./provider/agent-stream-hook";
import { routeStreamingInputToLiveSession } from "./provider/input-routing";
import {
  getCachedPiModels,
  updateCachedPiModelsIfStale,
} from "./provider/models";
import {
  retainOnlyActiveSessionMemory,
  terminateSession,
} from "./provider/session-lifecycle";
import { createStateStore } from "./provider/state";
import { streamCursorAgent } from "./provider/stream";

const auth = new AuthManager(new Auth(CURSOR_API_URL), CURSOR_WEBSITE_URL);

const createAiService = (accessToken: string) => {
  return new AiService(CURSOR_API_URL, {
    accessToken,
    clientVersion: CURSOR_CLIENT_VERSION,
    clientType: "cli",
  });
};

const updateCachedModelsInBackground = (accessToken: string) => {
  const ai = createAiService(accessToken);
  void updateCachedPiModelsIfStale(ai).catch(() => {}); // ignore
};

const updateCachedModelsFromContextInBackground = (ctx: ExtensionContext) => {
  void (async () => {
    const accessToken =
      await ctx.modelRegistry.getApiKeyForProvider("cursor-agent");
    if (!accessToken) {
      return;
    }

    await updateCachedPiModelsIfStale(createAiService(accessToken));
  })().catch(() => {}); // ignore
};

const login = async (
  callbacks: OAuthLoginCallbacks,
): Promise<OAuthCredentials> => {
  const credentials = await auth.login(callbacks);
  updateCachedModelsInBackground(credentials.access);
  return credentials;
};

const refreshToken = async (
  credentials: OAuthCredentials,
  signal: AbortSignal,
): Promise<OAuthCredentials> => {
  const refreshed = await auth.refresh(credentials, signal);
  updateCachedModelsInBackground(refreshed.access);
  return refreshed;
};

export default (pi: ExtensionAPI) => {
  let lastCtx: ExtensionContext | null = null;
  let currentSessionId: string | null = null;
  const getCtx = () => lastCtx;

  const state = createStateStore((type, data) => {
    pi.appendEntry(type, data);
  });

  const cleanupPreviousSession = async (newSessionId: string) => {
    const previousSessionId = currentSessionId;
    currentSessionId = newSessionId;
    if (previousSessionId && previousSessionId !== newSessionId) {
      await terminateSession(previousSessionId, "Session ended");
    }
  };

  const refreshBranchState = async (ctx: ExtensionContext) => {
    lastCtx = ctx;
    const sessionId = ctx.sessionManager.getSessionId();
    await cleanupPreviousSession(sessionId);
    state.resetFromContext(ctx);
    try {
      await restoreAgentStoreFromBranch(
        sessionId,
        ctx.sessionManager.getBranch(),
      );
    } catch {}
    retainOnlyActiveSessionMemory(sessionId);
  };

  const applySkipApproval = (enabled: boolean, ctx: ExtensionContext) => {
    setSkipApprovalEnabled(enabled);
    updateSkipApprovalStatus(ctx);
  };

  pi.registerFlag("skip-approval", {
    description: "Skip Cursor dangerous command approval prompts",
    type: "boolean",
    default: false,
  });

  pi.registerCommand("skip-approval", {
    description: "Toggle skipping Cursor dangerous command approvals",
    getArgumentCompletions: (prefix) => {
      const filtered = SKIP_APPROVAL_OPTIONS.filter((option) =>
        option.startsWith(prefix),
      );
      return filtered.map((value) => ({ value, label: value }));
    },
    handler: async (args, ctx) => {
      const action = parseSkipApprovalArgs(args ?? "");
      if (action === undefined) {
        if (ctx.hasUI) {
          ctx.ui.notify("Usage: /skip-approval [on|off]", "error");
        }
        return;
      }
      applySkipApproval(resolveSkipApprovalEnabled(action), ctx);
    },
  });

  pi.registerShortcut("ctrl+shift+y", {
    description: "Toggle skip-approval",
    handler: async (ctx) => {
      applySkipApproval(!getSkipApprovalEnabled(), ctx);
    },
  });

  pi.on("before_agent_start", async (_, ctx) => {
    lastCtx = ctx;
  });

  pi.on("agent_start", async (_, ctx) => {
    lastCtx = ctx;
  });

  pi.on("model_select", async (event, ctx) => {
    lastCtx = ctx;
    if (event.model.provider === "cursor-agent") {
      updateCachedModelsFromContextInBackground(ctx);
    }
  });

  pi.on("session_start", async (_, ctx) => {
    await refreshBranchState(ctx);
    if (pi.getFlag("skip-approval") === true) {
      setSkipApprovalEnabled(true);
    }
    updateSkipApprovalStatus(ctx);
    updateCachedModelsFromContextInBackground(ctx);
  });

  pi.on("session_shutdown", async () => {
    const sessionId = currentSessionId;
    currentSessionId = null;
    lastCtx = null;
    if (sessionId) {
      await terminateSession(sessionId, "Session ended");
    }
  });

  pi.on("session_tree", async (_, ctx) => {
    await refreshBranchState(ctx);
    updateSkipApprovalStatus(ctx);
  });

  pi.on("input", async (event, ctx) => {
    const sessionId = ctx.sessionManager.getSessionId();
    const liveSession = getLiveSession(sessionId);
    const source = (event as { source?: unknown }).source;
    const streamingBehavior = toStreamingBehavior(
      (event as { streamingBehavior?: unknown; deliverAs?: unknown })
        .streamingBehavior ??
        (event as { streamingBehavior?: unknown; deliverAs?: unknown })
          .deliverAs,
    );
    const text = (event as { text?: unknown }).text;
    if (
      source === "interactive" &&
      streamingBehavior &&
      typeof text === "string" &&
      text.trim().length > 0
    ) {
      if (liveSession) {
        try {
          if (streamingBehavior === "steer") {
            liveSession.markSteerIntent?.();
            await liveSession.steer(text);
          } else {
            await liveSession.followUp(text);
          }
        } catch {
          queueInputIntent(sessionId, text, streamingBehavior);
        }
      } else {
        queueInputIntent(sessionId, text, streamingBehavior);
      }
    }
    const action = await routeStreamingInputToLiveSession(event, liveSession);
    return { action };
  });

  pi.on("tool_execution_end", async (event) => {
    resolveToolResult({
      role: "toolResult",
      toolCallId: event.toolCallId,
      toolName: event.toolName,
      content: event.result?.content ?? [],
      details: event.result?.details,
      isError: event.isError,
      timestamp: Date.now(),
    });
  });

  pi.registerProvider("cursor-agent", {
    baseUrl: CURSOR_API_URL,
    apiKey: "$CURSOR_ACCESS_TOKEN",
    api: "cursor-agent" as unknown as Api,
    streamSimple: (model, context, options) =>
      streamCursorAgent(pi, getCtx, state, model, context, options),
    models: getCachedPiModels(),
    oauth: {
      name: "Cursor",
      login,
      refreshToken,
      getApiKey: (cred) => cred.access,
    },
  });
};
