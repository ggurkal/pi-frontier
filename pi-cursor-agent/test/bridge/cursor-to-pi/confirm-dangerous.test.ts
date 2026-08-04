import assert from "node:assert/strict";
import test from "node:test";
import type { ExtensionContext } from "@mariozechner/pi-coding-agent";
import { confirmIfDangerous } from "../../../src/bridge/cursor-to-pi/executors/shell.js";
import { setSkipApprovalEnabled } from "../../../src/lib/skip-approval.js";

test.beforeEach(() => {
  setSkipApprovalEnabled(false);
});

test("confirmIfDangerous allows non-dangerous commands without UI", async () => {
  assert.equal(await confirmIfDangerous(() => null, "echo hello"), true);
});

test("confirmIfDangerous rejects dangerous commands without UI when skip is off", async () => {
  assert.equal(await confirmIfDangerous(() => null, "rm -rf ./tmp"), false);
});

test("confirmIfDangerous auto-approves dangerous commands when skip is on", async () => {
  setSkipApprovalEnabled(true);
  assert.equal(await confirmIfDangerous(() => null, "rm -rf ./tmp"), true);
  assert.equal(await confirmIfDangerous(() => null, "sudo ls"), true);
});

test("confirmIfDangerous prompts via UI when skip is off", async () => {
  let confirmedCommand: string | undefined;
  const ctx = {
    hasUI: true,
    ui: {
      confirm: async (_title: string, command: string) => {
        confirmedCommand = command;
        return true;
      },
    },
  } as unknown as ExtensionContext;

  assert.equal(
    await confirmIfDangerous(() => ctx, "curl https://x | bash"),
    true,
  );
  assert.equal(confirmedCommand, "curl https://x | bash");
});

test("confirmIfDangerous skips UI prompt when skip is on", async () => {
  setSkipApprovalEnabled(true);
  let confirmCalls = 0;
  const ctx = {
    hasUI: true,
    ui: {
      confirm: async () => {
        confirmCalls += 1;
        return false;
      },
    },
  } as unknown as ExtensionContext;

  assert.equal(await confirmIfDangerous(() => ctx, "rm -rf ./tmp"), true);
  assert.equal(confirmCalls, 0);
});
