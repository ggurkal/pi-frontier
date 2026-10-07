import assert from "node:assert/strict";
import test from "node:test";
import {
  ExecClientMessage,
  type ExecServerControlMessage,
  ExecServerMessage,
} from "../../../src/__generated__/agent/v1/exec_pb.js";
import { ClientExecController } from "../../../src/vendor/agent-client/exec-controller.js";

test("a rejected exec result write is not an unhandled rejection", async () => {
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => unhandled.push(reason);
  process.on("unhandledRejection", onUnhandled);

  let endServerStream!: () => void;
  const serverOpen = new Promise<void>((resolve) => {
    endServerStream = resolve;
  });
  const serverStream = (async function* (): AsyncGenerator<
    ExecServerMessage | ExecServerControlMessage
  > {
    yield new ExecServerMessage({ id: 1 });
    await serverOpen;
  })();
  const controller = new ClientExecController(
    serverStream,
    { write: () => Promise.reject(new Error("aborted")) },
    {
      async *handle() {
        yield new ExecClientMessage({ id: 1 });
      },
      handleControlMessage() {},
    },
  );

  try {
    const run = controller.run(undefined).catch(() => {});
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.deepEqual(unhandled, []);
    endServerStream();
    await run;
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
});
