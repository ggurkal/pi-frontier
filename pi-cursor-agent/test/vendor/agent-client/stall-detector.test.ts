import assert from "node:assert/strict";
import test from "node:test";
import type { StallInfo } from "../../../src/vendor/agent-client/retry-policy.js";
import { createStallDetector } from "../../../src/vendor/agent-client/stall-detector.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function detect(thresholdMs: number) {
  const stalls: StallInfo[] = [];
  const detector = createStallDetector({
    thresholdMs,
    onStall: (info) => stalls.push(info),
  });
  return { detector, stalls };
}

async function every(ms: number, forMs: number, action: () => void) {
  const end = Date.now() + forMs;
  while (Date.now() < end) {
    action();
    await sleep(ms);
  }
}

test("fires once after the threshold without inbound activity", async () => {
  const { detector, stalls } = detect(40);
  await sleep(150);
  assert.equal(stalls.length, 1);
  assert.equal(stalls[0]?.thresholdMs, 40);
  detector.dispose();
});

test("inbound messages keep the stream alive", async () => {
  const { detector, stalls } = detect(50);
  await every(10, 150, () => detector.reset("inbound_message", "x"));
  assert.equal(stalls.length, 0);
  detector.dispose();
});

test("outbound writes and client heartbeats do not", async () => {
  const { detector, stalls } = detect(50);
  await every(10, 150, () => {
    detector.reset("outbound_write", "x");
    detector.onClientSentHeartbeat();
  });
  assert.equal(stalls.length, 1);
  assert.ok(stalls[0]?.lastClientHeartbeatAgoMs !== undefined);
  detector.dispose();
});

test("a pause holds the timer, and resuming restarts it", async () => {
  const { detector, stalls } = detect(50);
  detector.setPaused(true);
  await sleep(150);
  assert.equal(stalls.length, 0);
  detector.setPaused(false);
  await sleep(25);
  assert.equal(stalls.length, 0);
  await sleep(120);
  assert.equal(stalls.length, 1);
  detector.dispose();
});

test("a disposed detector never fires", async () => {
  const { detector, stalls } = detect(40);
  detector.dispose();
  await sleep(100);
  assert.equal(stalls.length, 0);
});

test("a threshold of 0 disables detection", async () => {
  const { detector, stalls } = detect(0);
  await sleep(50);
  assert.equal(stalls.length, 0);
  detector.dispose();
});
