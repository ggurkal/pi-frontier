import assert from "node:assert/strict";
import test from "node:test";
import type { ToolResultMessage } from "@earendil-works/pi-ai";
import {
  rejectPendingForChannel,
  rejectPendingForSession,
  requestToolExecution,
  resolveToolResult,
  type ToolExecRequest,
} from "../../../src/bridge/cursor-to-pi/tool-bridge.js";
import { LiveEventChannel } from "../../../src/provider/agent-stream-hook.js";

let idCounter = 0;
const newId = () => `tool-bridge-test-${idCounter++}`;

function request(toolCallId: string): ToolExecRequest {
  return {
    toolCallId,
    cursorExecType: "read",
    piToolName: "read",
    piToolArgs: { path: "README.md" },
  };
}

function result(toolCallId: string): ToolResultMessage {
  return {
    role: "toolResult",
    toolCallId,
    toolName: "read",
    content: [{ type: "text", text: "contents" }],
    isError: false,
    timestamp: Date.now(),
  };
}

function pushedCount(channel: LiveEventChannel): number {
  return (Reflect.get(channel, "events") as unknown[]).length;
}

test("a repeated request for a running tool call attaches to it", async () => {
  const channel = new LiveEventChannel("s1");
  const id = newId();
  const first = requestToolExecution(channel, request(id));
  const second = requestToolExecution(channel, request(id));
  assert.equal(pushedCount(channel), 1);

  const done = result(id);
  assert.equal(resolveToolResult(done), true);
  assert.equal(await first, done);
  assert.equal(await second, done);
});

test("a request for a finished tool call gets the cached result", async () => {
  const channel = new LiveEventChannel("s1");
  const id = newId();
  const first = requestToolExecution(channel, request(id));
  const done = result(id);
  resolveToolResult(done);
  await first;

  assert.equal(await requestToolExecution(channel, request(id)), done);
  assert.equal(pushedCount(channel), 1);
});

test("the same tool call id in another session runs again", () => {
  const id = newId();
  const a = new LiveEventChannel("s1");
  const b = new LiveEventChannel("s2");
  void requestToolExecution(a, request(id));
  void requestToolExecution(b, request(id)).catch(() => {});
  assert.equal(pushedCount(b), 1);
  rejectPendingForSession("s1", "cleanup");
  rejectPendingForSession("s2", "cleanup");
});

test("rejecting a session rejects running calls and forgets results", async () => {
  const channel = new LiveEventChannel("s-reject");
  const running = newId();
  const finished = newId();
  const pending = requestToolExecution(channel, request(running));
  void requestToolExecution(channel, request(finished));
  resolveToolResult(result(finished));

  rejectPendingForSession("s-reject", "gone");
  await assert.rejects(pending, /gone/);
  void requestToolExecution(channel, request(finished)).catch(() => {});
  assert.equal(pushedCount(channel), 3);
  rejectPendingForSession("s-reject", "cleanup");
});

test("rejecting a channel rejects its running calls and forgets its results", async () => {
  const channel = new LiveEventChannel("s-channel");
  const finished = newId();
  void requestToolExecution(channel, request(finished));
  resolveToolResult(result(finished));
  const running = requestToolExecution(channel, request(newId()));

  rejectPendingForChannel(channel, "ended");
  await assert.rejects(running, /ended/);
  void requestToolExecution(channel, request(finished)).catch(() => {});
  assert.equal(pushedCount(channel), 3);
  rejectPendingForChannel(channel, "cleanup");
});

test("the result cache is bounded", () => {
  const channel = new LiveEventChannel("s-bounded");
  const ids = Array.from({ length: 257 }, newId);
  for (const id of ids) {
    void requestToolExecution(channel, request(id));
    resolveToolResult(result(id));
  }
  const [oldest] = ids;
  assert.ok(oldest);
  void requestToolExecution(channel, request(oldest)).catch(() => {});
  assert.equal(pushedCount(channel), 258);
  rejectPendingForChannel(channel, "cleanup");
});

test("resolving an unknown tool call id returns false", () => {
  assert.equal(resolveToolResult(result(newId())), false);
});
