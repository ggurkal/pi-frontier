import assert from "node:assert/strict";
import test from "node:test";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
  createOverlayState,
  createStateStore,
} from "../../src/provider/state.js";

function mockCtx(
  entries: Array<{ type: string; customType?: string; data?: unknown }>,
): ExtensionContext {
  return {
    sessionManager: { getBranch: () => entries },
  } as unknown as ExtensionContext;
}

const META = {
  toolCallId: "call-1",
  cursorExecType: "read",
  piToolName: "read",
  piToolArgs: { path: "README.md" },
  assistantTimestamp: 1000,
} as const;

test("remember and get tool call meta", () => {
  const appended: string[] = [];
  const state = createStateStore((type) => appended.push(type));

  state.rememberToolCallMeta({ ...META });
  const meta = state.getToolCallMeta("call-1");

  assert.ok(meta);
  assert.equal(meta.cursorExecType, "read");
  assert.equal(meta.piToolName, "read");
  assert.deepEqual(meta.piToolArgs, { path: "README.md" });
  assert.deepEqual(appended, ["pi-cursor-agent:tool-call-meta"]);
});

test("remember and get assistant content", () => {
  const appended: string[] = [];
  const state = createStateStore((type) => appended.push(type));

  state.rememberAssistantContent({
    timestamp: 1000,
    blocks: [{ type: "text", text: "Hello" }],
  });

  assert.equal(state.getAssistantContent(1000)?.blocks.length, 1);
  assert.deepEqual(appended, ["pi-cursor-agent:assistant-content"]);
});

test("returns undefined for unknown ids", () => {
  const state = createStateStore(() => {});
  assert.equal(state.getToolCallMeta("x"), undefined);
  assert.equal(state.getAssistantContent(0), undefined);
});

test("resetFromContext restores valid entries", () => {
  const state = createStateStore(() => {});
  state.resetFromContext(
    mockCtx([
      {
        type: "custom",
        customType: "pi-cursor-agent:tool-call-meta",
        data: { ...META },
      },
      {
        type: "custom",
        customType: "pi-cursor-agent:assistant-content",
        data: { timestamp: 1000, blocks: [{ type: "text", text: "Hi" }] },
      },
    ]),
  );

  assert.equal(state.getToolCallMeta("call-1")?.cursorExecType, "read");
  assert.equal(state.getAssistantContent(1000)?.blocks.length, 1);
});

test("resetFromContext skips malformed and incomplete entries", () => {
  const state = createStateStore(() => {});
  state.resetFromContext(
    mockCtx([
      // empty toolCallId
      {
        type: "custom",
        customType: "pi-cursor-agent:tool-call-meta",
        data: { toolCallId: "" },
      },
      // missing cursorExecType
      {
        type: "custom",
        customType: "pi-cursor-agent:tool-call-meta",
        data: {
          toolCallId: "x",
          piToolName: "read",
          piToolArgs: {},
          assistantTimestamp: 1,
        },
      },
      // bad timestamp type
      {
        type: "custom",
        customType: "pi-cursor-agent:assistant-content",
        data: { timestamp: "bad" },
      },
      // non-custom entry
      { type: "user", data: {} },
    ]),
  );

  assert.equal(state.getToolCallMeta(""), undefined);
  assert.equal(state.getToolCallMeta("x"), undefined);
});

test("resetFromContext clears previous state", () => {
  const state = createStateStore(() => {});
  state.rememberToolCallMeta({ ...META });
  state.resetFromContext(mockCtx([]));
  assert.equal(state.getToolCallMeta("call-1"), undefined);
});

test("multiple tool calls tracked independently", () => {
  const state = createStateStore(() => {});
  state.rememberToolCallMeta({
    ...META,
    toolCallId: "a",
    cursorExecType: "read",
  });
  state.rememberToolCallMeta({
    ...META,
    toolCallId: "b",
    cursorExecType: "ls",
    piToolName: "bash",
  });

  assert.equal(state.getToolCallMeta("a")?.cursorExecType, "read");
  assert.equal(state.getToolCallMeta("b")?.cursorExecType, "ls");
});

test("overlay reads cascade to base, writes do not persist", () => {
  const appended: string[] = [];
  const base = createStateStore((type) => appended.push(type));
  base.rememberToolCallMeta({ ...META, toolCallId: "base-call" });

  const overlay = createOverlayState(base);

  // reads cascade to base
  assert.equal(overlay.getToolCallMeta("base-call")?.piToolName, "read");

  // writes stay in overlay, not persisted
  const beforeCount = appended.length;
  overlay.rememberToolCallMeta({ ...META, toolCallId: "overlay-call" });
  assert.equal(appended.length, beforeCount); // no new appendEntry
  assert.equal(overlay.getToolCallMeta("overlay-call")?.piToolName, "read");
  assert.equal(base.getToolCallMeta("overlay-call"), undefined); // not in base

  // same for assistant content
  overlay.rememberAssistantContent({
    timestamp: 999,
    blocks: [{ type: "text", text: "tmp" }],
  });
  assert.equal(appended.length, beforeCount);
  assert.equal(overlay.getAssistantContent(999)?.blocks.length, 1);
  assert.equal(base.getAssistantContent(999), undefined);
});

test("overlay shadows base on key collision", () => {
  const base = createStateStore(() => {});
  base.rememberToolCallMeta({ ...META, toolCallId: "shared" });

  const overlay = createOverlayState(base);
  overlay.rememberToolCallMeta({
    ...META,
    toolCallId: "shared",
    cursorExecType: "grep",
  });

  assert.equal(overlay.getToolCallMeta("shared")?.cursorExecType, "grep");
  assert.equal(base.getToolCallMeta("shared")?.cursorExecType, "read");
});
