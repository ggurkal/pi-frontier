import assert from "node:assert/strict";
import test from "node:test";
import { parseStallTimeout } from "../../src/provider/env.js";

test("parseStallTimeout", () => {
  assert.equal(parseStallTimeout(undefined), 30_000);
  assert.equal(parseStallTimeout(""), 30_000);
  assert.equal(parseStallTimeout("abc"), 30_000);
  assert.equal(parseStallTimeout("0"), 0);
  assert.equal(parseStallTimeout("-5"), 0);
  assert.equal(parseStallTimeout("5000"), 20_000);
  assert.equal(parseStallTimeout("45000"), 45_000);
});
