import assert from "node:assert/strict";
import test from "node:test";
import { routeStreamingInputToLiveSession } from "../../src/provider/input-routing.js";

test("returns continue when there is no live session", async () => {
  const result = await routeStreamingInputToLiveSession(
    { text: "hello", streamingBehavior: "steer" },
    undefined,
  );

  assert.equal(result, "continue");
});

test("routes non-interactive steer input to live session", async () => {
  const calls: string[] = [];
  let marked = 0;
  const liveSession = {
    markSteerIntent: () => {
      marked++;
    },
    steer: async (text: string) => {
      calls.push(`steer:${text}`);
    },
    followUp: async (text: string) => {
      calls.push(`followUp:${text}`);
    },
  };

  const result = await routeStreamingInputToLiveSession(
    { text: "course correct", streamingBehavior: "steer", source: "extension" },
    liveSession,
  );

  assert.equal(result, "handled");
  assert.equal(marked, 1);
  assert.deepEqual(calls, ["steer:course correct"]);
});

test("routes non-interactive follow-up input to live session", async () => {
  const calls: string[] = [];
  let marked = 0;
  const liveSession = {
    markSteerIntent: () => {
      marked++;
    },
    steer: async (text: string) => {
      calls.push(`steer:${text}`);
    },
    followUp: async (text: string) => {
      calls.push(`followUp:${text}`);
    },
  };

  const result = await routeStreamingInputToLiveSession(
    { text: "later", streamingBehavior: "followUp", source: "rpc" },
    liveSession,
  );

  assert.equal(result, "handled");
  assert.equal(marked, 0);
  assert.deepEqual(calls, ["followUp:later"]);
});

test("supports legacy deliverAs field", async () => {
  const calls: string[] = [];
  const liveSession = {
    steer: async (text: string) => {
      calls.push(`steer:${text}`);
    },
    followUp: async (text: string) => {
      calls.push(`followUp:${text}`);
    },
  };

  const result = await routeStreamingInputToLiveSession(
    { text: "queued", deliverAs: "followUp", source: "extension" },
    liveSession,
  );

  assert.equal(result, "handled");
  assert.deepEqual(calls, ["followUp:queued"]);
});

test("returns continue for unsupported streaming behavior", async () => {
  const calls: string[] = [];
  const liveSession = {
    steer: async (text: string) => {
      calls.push(`steer:${text}`);
    },
    followUp: async (text: string) => {
      calls.push(`followUp:${text}`);
    },
  };

  const result = await routeStreamingInputToLiveSession(
    { text: "noop", streamingBehavior: "nextTurn", source: "extension" },
    liveSession,
  );

  assert.equal(result, "continue");
  assert.deepEqual(calls, []);
});

test("returns continue for interactive steer input so core can queue", async () => {
  const calls: string[] = [];
  let marked = 0;
  const liveSession = {
    markSteerIntent: () => {
      marked++;
    },
    steer: async (text: string) => {
      calls.push(`steer:${text}`);
    },
    followUp: async (text: string) => {
      calls.push(`followUp:${text}`);
    },
  };

  const result = await routeStreamingInputToLiveSession(
    {
      text: "fallback steer",
      source: "interactive",
      streamingBehavior: "steer",
    },
    liveSession,
  );

  assert.equal(result, "continue");
  assert.equal(marked, 0);
  assert.deepEqual(calls, []);
});

test("returns continue for interactive follow-up input so core can queue", async () => {
  const calls: string[] = [];
  let marked = 0;
  const liveSession = {
    markSteerIntent: () => {
      marked++;
    },
    steer: async (text: string) => {
      calls.push(`steer:${text}`);
    },
    followUp: async (text: string) => {
      calls.push(`followUp:${text}`);
    },
  };

  const result = await routeStreamingInputToLiveSession(
    {
      text: "follow this up",
      source: "interactive",
      streamingBehavior: "followUp",
    },
    liveSession,
  );

  assert.equal(result, "continue");
  assert.equal(marked, 0);
  assert.deepEqual(calls, []);
});

test("returns continue when non-interactive input has no explicit behavior", async () => {
  const calls: string[] = [];
  const liveSession = {
    steer: async (text: string) => {
      calls.push(`steer:${text}`);
    },
    followUp: async (text: string) => {
      calls.push(`followUp:${text}`);
    },
  };

  const result = await routeStreamingInputToLiveSession(
    { text: "from extension", source: "extension" },
    liveSession,
  );

  assert.equal(result, "continue");
  assert.deepEqual(calls, []);
});

test("returns continue when live session routing fails", async () => {
  const liveSession = {
    steer: async () => {
      throw new Error("boom");
    },
    followUp: async () => {},
  };

  const result = await routeStreamingInputToLiveSession(
    { text: "hello", streamingBehavior: "steer", source: "extension" },
    liveSession,
  );

  assert.equal(result, "continue");
});
