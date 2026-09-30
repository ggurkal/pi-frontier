import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  requestToolExecution,
  resolveToolResult,
} from "../../src/bridge/cursor-to-pi/tool-bridge.js";
import {
  deleteAgentStore,
  ensureAgentStore,
  hasAgentStore,
  persistAgentStore,
  retainOnlyAgentStore,
} from "../../src/lib/agent-store/index.js";
import {
  getLiveSession,
  LiveEventChannel,
  type LiveSession,
  retainOnlyLiveSession,
  setLiveSession,
} from "../../src/provider/agent-stream-hook.js";
import { createSteerDispatcher } from "../../src/provider/pending-messages.js";
import {
  awaitSessionTeardown,
  retainOnlyActiveSessionMemory,
  runSessionTeardown,
  setTeardownTimeoutMs,
  terminateSession,
} from "../../src/provider/session-lifecycle.js";

function createLiveSession(
  label: string,
  overrides: Partial<LiveSession> = {},
): LiveSession {
  return {
    channel: new LiveEventChannel(label),
    cursorRunPromise: Promise.resolve(),
    flushSessionState: async () => {},
    abort: () => {},
    startTime: Date.now(),
    steers: createSteerDispatcher({ runId: label }),
    seenUserMessageKeys: new Set(),
    hasCurrentCheckpoint: () => false,
    markCheckpointStale: () => {},
    linkAbort: () => {},
    ...overrides,
  };
}

test("retainOnlyLiveSession keeps only the selected live session", () => {
  const aborted: string[] = [];
  setLiveSession("session-a", createLiveSession("session-a"));
  setLiveSession(
    "session-b",
    createLiveSession("session-b", { abort: () => aborted.push("b") }),
  );

  retainOnlyLiveSession("session-a");

  assert.ok(getLiveSession("session-a"));
  assert.equal(getLiveSession("session-b"), undefined);
  assert.deepEqual(aborted, ["b"]);

  retainOnlyLiveSession(null);
  assert.equal(getLiveSession("session-a"), undefined);
});

test("concurrent store loads share one entry; a load overtaken by a drop fails and publishes nothing", async () => {
  const baseDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "pi-cursor-agent-store-test-"),
  );

  try {
    const [a, b] = await Promise.all([
      ensureAgentStore(baseDir, "session-c"),
      ensureAgentStore(baseDir, "session-c"),
    ]);
    assert.equal(a, b);
    deleteAgentStore("session-c");

    const stale = ensureAgentStore(baseDir, "session-c");
    deleteAgentStore("session-c");
    await assert.rejects(stale, /dropped while loading/);
    assert.equal(hasAgentStore("session-c"), false);

    const fresh = await ensureAgentStore(baseDir, "session-c");
    assert.notEqual(fresh, a);
    assert.equal(await ensureAgentStore(baseDir, "session-c"), fresh);
  } finally {
    retainOnlyAgentStore(null);
    await fs.rm(baseDir, { recursive: true, force: true });
  }
});

test("a persist that may no longer commit leaves the files untouched", async () => {
  const baseDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "pi-cursor-agent-store-test-"),
  );

  try {
    await ensureAgentStore(baseDir, "session-p");
    await persistAgentStore(baseDir, "session-p", () => false);
    const dir = path.join(baseDir, "chats", "session-p");
    assert.deepEqual(await fs.readdir(dir), []);

    await persistAgentStore(baseDir, "session-p");
    assert.deepEqual((await fs.readdir(dir)).sort(), [
      "blobs.json",
      "meta.json",
    ]);
  } finally {
    retainOnlyAgentStore(null);
    await fs.rm(baseDir, { recursive: true, force: true });
  }
});

test("both store files commit on one decision even if the lease expires right after", async () => {
  const baseDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "pi-cursor-agent-store-test-"),
  );

  try {
    await ensureAgentStore(baseDir, "session-l");
    let checks = 0;
    await persistAgentStore(baseDir, "session-l", () => ++checks === 1);

    assert.equal(checks, 1);
    const dir = path.join(baseDir, "chats", "session-l");
    assert.deepEqual((await fs.readdir(dir)).sort(), [
      "blobs.json",
      "meta.json",
    ]);
  } finally {
    retainOnlyAgentStore(null);
    await fs.rm(baseDir, { recursive: true, force: true });
  }
});

test("a stalled accepted commit cannot land after a newer persist", async () => {
  const baseDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "pi-cursor-agent-store-test-"),
  );
  const originalRename = fs.rename;

  try {
    const entry = await ensureAgentStore(baseDir, "session-r");
    entry.jsonStore.blobs.set("aa", new Uint8Array([1]));
    entry.jsonStore.metadata.latestRootBlobId = new Uint8Array([0xaa]);

    let release!: () => void;
    const blocked = new Promise<void>((r) => {
      release = r;
    });
    let started!: () => void;
    const firstStarted = new Promise<void>((r) => {
      started = r;
    });
    let blockedOnce = false;
    fs.rename = (async (from: string, to: string) => {
      if (!blockedOnce && to.endsWith("blobs.json")) {
        blockedOnce = true;
        started();
        await blocked;
      }
      return originalRename(from, to);
    }) as typeof fs.rename;

    let current = true;
    const stale = persistAgentStore(baseDir, "session-r", () => current);
    await firstStarted;
    current = false;
    entry.jsonStore.blobs.set("bb", new Uint8Array([2]));
    entry.jsonStore.metadata.latestRootBlobId = new Uint8Array([0xbb]);
    const newer = persistAgentStore(baseDir, "session-r");
    // Unqueued, the newer persist would finish here, before the stale one.
    await Promise.race([newer, new Promise((r) => setTimeout(r, 200))]);
    release();
    await Promise.all([stale, newer]);

    const dir = path.join(baseDir, "chats", "session-r");
    const meta = JSON.parse(
      await fs.readFile(path.join(dir, "meta.json"), "utf8"),
    );
    const blobs = JSON.parse(
      await fs.readFile(path.join(dir, "blobs.json"), "utf8"),
    );
    assert.equal(meta.latestRootBlobId, "bb");
    assert.deepEqual(blobs.blobs.map((b: { id: string }) => b.id).sort(), [
      "aa",
      "bb",
    ]);
  } finally {
    fs.rename = originalRename;
    retainOnlyAgentStore(null);
    await fs.rm(baseDir, { recursive: true, force: true });
  }
});

test("retainOnlyAgentStore keeps only the selected in-memory store", async () => {
  const baseDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "pi-cursor-agent-store-test-"),
  );

  try {
    await ensureAgentStore(baseDir, "session-a");
    await ensureAgentStore(baseDir, "session-b");

    assert.equal(hasAgentStore("session-a"), true);
    assert.equal(hasAgentStore("session-b"), true);

    retainOnlyAgentStore("session-a");
    assert.equal(hasAgentStore("session-a"), true);
    assert.equal(hasAgentStore("session-b"), false);

    retainOnlyAgentStore(null);
    assert.equal(hasAgentStore("session-a"), false);
  } finally {
    retainOnlyAgentStore(null);
    await fs.rm(baseDir, { recursive: true, force: true });
  }
});

test("terminateSession aborts the live session and rejects pending tool results", async () => {
  let abortReason: string | undefined;
  let flushed = 0;
  let resolveRun!: () => void;
  const cursorRunPromise = new Promise<void>((resolve) => {
    resolveRun = resolve;
  });

  const session = createLiveSession("session-a", {
    cursorRunPromise,
    flushSessionState: async () => {
      flushed += 1;
    },
    abort: (reason?: string) => {
      abortReason = reason;
      resolveRun();
    },
  });
  setLiveSession("session-a", session);

  const pending = requestToolExecution(session.channel, {
    toolCallId: "call-a",
    cursorExecType: "read",
    piToolName: "read",
    piToolArgs: { path: "README.md" },
  });

  await terminateSession("session-a", "Session ended");

  await assert.rejects(pending, /Session ended/);
  assert.equal(abortReason, "Session ended");
  assert.equal(flushed, 1);
  assert.equal(getLiveSession("session-a"), undefined);
});

test("awaitSessionTeardown waits until termination has flushed", async () => {
  let resolveRun!: () => void;
  const cursorRunPromise = new Promise<void>((resolve) => {
    resolveRun = resolve;
  });
  let flushed = false;
  setLiveSession(
    "session-t",
    createLiveSession("session-t", {
      cursorRunPromise,
      flushSessionState: async () => {
        flushed = true;
      },
    }),
  );

  const terminating = terminateSession("session-t", "Session ended");
  let waited = false;
  const waiting = awaitSessionTeardown("session-t").then(() => {
    waited = true;
  });
  await new Promise<void>((r) => setImmediate(r));

  assert.equal(getLiveSession("session-t"), undefined);
  assert.equal(waited, false);
  resolveRun();
  await waiting;
  assert.equal(flushed, true);
  await terminating;
});

test("session teardowns run in order and new runs wait for all of them", async () => {
  const order: string[] = [];
  let release!: () => void;
  const first = runSessionTeardown("session-o", async () => {
    await new Promise<void>((r) => {
      release = r;
    });
    order.push("first");
  });
  const second = runSessionTeardown("session-o", async () => {
    order.push("second");
  });
  const waiting = awaitSessionTeardown("session-o").then(() => {
    order.push("new run");
  });
  await new Promise<void>((r) => setImmediate(r));
  assert.deepEqual(order, []);

  release();
  await Promise.all([first, second, waiting]);
  assert.deepEqual(order, ["first", "second", "new run"]);
});

test("a teardown task past the timeout stops blocking and is told it is stale", async () => {
  setTeardownTimeoutMs(20);
  try {
    let release!: () => void;
    const seen: boolean[] = [];
    void runSessionTeardown("session-s", async (isCurrent) => {
      seen.push(isCurrent());
      await new Promise<void>((r) => {
        release = r;
      });
      seen.push(isCurrent());
    });

    await awaitSessionTeardown("session-s");
    release();
    await new Promise<void>((r) => setImmediate(r));

    assert.deepEqual(seen, [true, false]);
  } finally {
    setTeardownTimeoutMs(undefined);
  }
});

test("retainOnlyActiveSessionMemory keeps the active session and rejects others", async () => {
  const sessionA = createLiveSession("session-a");
  const sessionB = createLiveSession("session-b");
  setLiveSession("session-a", sessionA);
  setLiveSession("session-b", sessionB);

  const channelA = new LiveEventChannel("session-a");
  const channelB = new LiveEventChannel("session-b");
  const keepPending = requestToolExecution(channelA, {
    toolCallId: "call-keep",
    cursorExecType: "read",
    piToolName: "read",
    piToolArgs: { path: "README.md" },
  });
  const dropPending = requestToolExecution(channelB, {
    toolCallId: "call-drop",
    cursorExecType: "read",
    piToolName: "read",
    piToolArgs: { path: "README.md" },
  });

  retainOnlyActiveSessionMemory("session-a", "Session ended");

  assert.ok(getLiveSession("session-a"));
  assert.equal(getLiveSession("session-b"), undefined);
  await assert.rejects(dropPending, /Session ended/);

  resolveToolResult({
    role: "toolResult",
    toolCallId: "call-keep",
    toolName: "read",
    content: [],
    isError: false,
    timestamp: Date.now(),
  });
  await assert.doesNotReject(keepPending);

  retainOnlyActiveSessionMemory(null, "Session ended");
});
