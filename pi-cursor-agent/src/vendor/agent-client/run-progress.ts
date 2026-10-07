import type { AgentServerMessage } from "../../__generated__/agent/v1/agent_pb";
import type { ProgressSnapshot } from "./retry-policy";

/** Interaction updates Pi shows. */
const OUTPUT_UPDATE_CASES: ReadonlySet<string> = new Set([
  "textDelta",
  "thinkingDelta",
  "thinkingCompleted",
  "partialToolCall",
  "toolCallStarted",
  "toolCallDelta",
  "toolCallCompleted",
]);

/** Exec requests that make Pi run a tool. */
export const PI_TOOL_EXEC_CASES: ReadonlySet<string> = new Set([
  "readArgs",
  "lsArgs",
  "grepArgs",
  "deleteArgs",
  "writeArgs",
  "shellArgs",
  "shellStreamArgs",
  "mcpArgs",
]);

/**
 * What a run attempt received since its last checkpoint, fed in wire order.
 * Tool call ids are kept across attempts: a resumed run that requests a tool
 * call again reuses it (see `tool-bridge.ts`), so that is not new output.
 */
export class RunProgress {
  private state: ProgressSnapshot = RunProgress.empty();
  private readonly seenToolCallIds = new Set<string>();

  private static empty(): ProgressSnapshot {
    return {
      turnEnded: false,
      terminalCheckpoint: false,
      checkpointThisAttempt: false,
      outputSinceCheckpoint: false,
      stateSentSinceCheckpoint: false,
      streamed: false,
    };
  }

  startAttempt(): void {
    this.state = RunProgress.empty();
  }

  /** Call synchronously for every inbound message, before it is dispatched. */
  onServerMessage(message: AgentServerMessage): void {
    const state = this.state;
    const msg = message.message;
    switch (msg.case) {
      case "interactionUpdate": {
        const updateCase = msg.value.message.case;
        if (updateCase === "heartbeat") return;
        state.streamed = true;
        if (updateCase === "turnEnded") state.turnEnded = true;
        if (updateCase && OUTPUT_UPDATE_CASES.has(updateCase)) {
          state.outputSinceCheckpoint = true;
        }
        return;
      }
      case "conversationCheckpointUpdate":
        state.streamed = true;
        state.checkpointThisAttempt = true;
        state.terminalCheckpoint ||= state.turnEnded;
        state.outputSinceCheckpoint = false;
        state.stateSentSinceCheckpoint = false;
        return;
      case "execServerMessage": {
        state.streamed = true;
        const exec = msg.value.message;
        if (!exec.case || !PI_TOOL_EXEC_CASES.has(exec.case)) return;
        const id = (exec.value as { toolCallId?: string }).toolCallId ?? "";
        if (id === "" || !this.seenToolCallIds.has(id)) {
          state.outputSinceCheckpoint = true;
        }
        if (id !== "") this.seenToolCallIds.add(id);
        return;
      }
      default:
        state.streamed = true;
    }
  }

  /** Call when a tool result is written to the request stream. */
  onExecResultSent(): void {
    this.state.stateSentSinceCheckpoint = true;
  }

  get turnEnded(): boolean {
    return this.state.turnEnded;
  }

  snapshot(): ProgressSnapshot {
    return { ...this.state };
  }
}
