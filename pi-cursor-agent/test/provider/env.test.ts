import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import {
  cursorProjectDir,
  parseStallTimeout,
  slugifyCursorProjectPath,
} from "../../src/provider/env.js";

test("parseStallTimeout", () => {
  assert.equal(parseStallTimeout(undefined), 30_000);
  assert.equal(parseStallTimeout(""), 30_000);
  assert.equal(parseStallTimeout("abc"), 30_000);
  assert.equal(parseStallTimeout("0"), 0);
  assert.equal(parseStallTimeout("-5"), 0);
  assert.equal(parseStallTimeout("5000"), 20_000);
  assert.equal(parseStallTimeout("45000"), 45_000);
});

test("slugifyCursorProjectPath matches Cursor project directory names", () => {
  assert.equal(
    slugifyCursorProjectPath("/home/ada/code/pi-frontier/pi-cursor-agent"),
    "home-ada-code-pi-frontier-pi-cursor-agent",
  );
  assert.equal(
    slugifyCursorProjectPath("/home/ada/.config/ghostty"),
    "home-ada-config-ghostty",
  );
  assert.equal(
    slugifyCursorProjectPath("/tmp/9289847b-23d9-413e-a015-abf76d41640b"),
    "tmp-9289847b-23d9-413e-a015-abf76d41640b",
  );
});

test("cursorProjectDir keeps metadata outside the workspace", () => {
  const agentDir = "/home/ada/.pi/agent";
  assert.equal(
    cursorProjectDir("/home/ada/code/launcher", agentDir),
    path.join(agentDir, "cursor-agent", "projects", "home-ada-code-launcher"),
  );
});
