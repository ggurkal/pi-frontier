import { type LiveSession, toStreamingBehavior } from "./agent-stream-hook";

interface StreamingInputEventLike {
  text: string;
  streamingBehavior?: unknown;
  deliverAs?: unknown;
  source?: unknown;
}

export async function routeStreamingInputToLiveSession(
  event: StreamingInputEventLike,
  liveSession:
    | Pick<LiveSession, "steer" | "followUp" | "markSteerIntent">
    | undefined,
): Promise<"handled" | "continue"> {
  if (!liveSession) {
    return "continue";
  }

  // Keep interactive queuing in Pi core so queue_update events render
  // steering/follow-up messages in the UI.
  if (event.source === "interactive") {
    return "continue";
  }

  const explicitBehavior = toStreamingBehavior(
    event.streamingBehavior ?? event.deliverAs,
  );
  const behavior = explicitBehavior;
  if (!behavior) {
    return "continue";
  }

  try {
    if (behavior === "steer") {
      liveSession.markSteerIntent?.();
      await liveSession.steer(event.text);
    } else {
      await liveSession.followUp(event.text);
    }
    return "handled";
  } catch {
    return "continue";
  }
}
