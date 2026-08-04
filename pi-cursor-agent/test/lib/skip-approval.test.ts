import assert from "node:assert/strict";
import test from "node:test";
import {
  getSkipApprovalEnabled,
  parseSkipApprovalArgs,
  resolveSkipApprovalEnabled,
  setSkipApprovalEnabled,
} from "../../src/lib/skip-approval.js";

test.beforeEach(() => {
  setSkipApprovalEnabled(false);
});

test("parseSkipApprovalArgs treats empty as toggle", () => {
  assert.equal(parseSkipApprovalArgs(""), "toggle");
  assert.equal(parseSkipApprovalArgs("  "), "toggle");
});

test("parseSkipApprovalArgs accepts on and off case-insensitively", () => {
  assert.equal(parseSkipApprovalArgs("on"), "on");
  assert.equal(parseSkipApprovalArgs("OFF"), "off");
  assert.equal(parseSkipApprovalArgs(" On "), "on");
});

test("parseSkipApprovalArgs rejects unknown args", () => {
  assert.equal(parseSkipApprovalArgs("maybe"), undefined);
  assert.equal(parseSkipApprovalArgs("on please"), undefined);
});

test("resolveSkipApprovalEnabled honors on off and toggle", () => {
  setSkipApprovalEnabled(false);
  assert.equal(resolveSkipApprovalEnabled("on"), true);
  assert.equal(resolveSkipApprovalEnabled("off"), false);
  assert.equal(resolveSkipApprovalEnabled("toggle"), true);

  setSkipApprovalEnabled(true);
  assert.equal(resolveSkipApprovalEnabled("toggle"), false);
});

test("setSkipApprovalEnabled updates process state", () => {
  assert.equal(getSkipApprovalEnabled(), false);
  setSkipApprovalEnabled(true);
  assert.equal(getSkipApprovalEnabled(), true);
});
