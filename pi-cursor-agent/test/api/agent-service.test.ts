import assert from "node:assert/strict";
import http2 from "node:http2";
import type { AddressInfo } from "node:net";
import test from "node:test";
import { type Client, Code, ConnectError } from "@connectrpc/connect";
import { createWritableIterable } from "@connectrpc/connect/protocol";
import type {
  AgentClientMessage,
  AgentServerMessage,
} from "../../src/__generated__/agent/v1/agent_pb.js";
import type { AgentService as AgentServiceDef } from "../../src/__generated__/agent/v1/agent_service_connect.js";
import AgentService, {
  wrapAbortSafeStream,
} from "../../src/api/agent-service.js";

function createNeverEndingStream(returnSpy?: {
  called: boolean;
}): AsyncIterable<AgentServerMessage> {
  return {
    [Symbol.asyncIterator]() {
      return {
        next() {
          return new Promise<IteratorResult<AgentServerMessage>>(() => {});
        },
        async return() {
          if (returnSpy) {
            returnSpy.called = true;
          }
          return { done: true, value: undefined as never };
        },
      };
    },
  };
}

test("wrapAbortSafeStream aborts by closing the underlying iterator", async () => {
  const controller = new AbortController();
  const returnSpy = { called: false };
  const stream = wrapAbortSafeStream(
    createNeverEndingStream(returnSpy),
    controller.signal,
  );

  const iterator = stream[Symbol.asyncIterator]();
  const nextPromise = iterator.next();
  controller.abort();

  await assert.rejects(nextPromise, (error: unknown) => {
    assert.ok(error instanceof Error);
    assert.equal(error.name, "AbortError");
    return true;
  });
  assert.equal(returnSpy.called, true);
});

test("rpcClient forwards the signal so Connect cancels the stream", async () => {
  const controller = new AbortController();
  const returnSpy = { called: false };
  let seenOptions:
    | { signal?: AbortSignal; headers?: Record<string, string> }
    | undefined;

  const client = {
    run(
      _input: AsyncIterable<AgentClientMessage>,
      options?: { signal?: AbortSignal; headers?: Record<string, string> },
    ): AsyncIterable<AgentServerMessage> {
      seenOptions = options;
      return createNeverEndingStream(returnSpy);
    },
  } as Client<typeof AgentServiceDef>;

  const service = Object.create(AgentService.prototype) as AgentService;
  Reflect.set(service as object, "client", client);

  const stream = service.rpcClient.run(
    {
      async *[Symbol.asyncIterator]() {},
    },
    { signal: controller.signal, headers: { "x-test": "1" } },
  );

  const iterator = stream[Symbol.asyncIterator]();
  const nextPromise = iterator.next();
  controller.abort();

  await assert.rejects(nextPromise, (error: unknown) => {
    assert.ok(error instanceof Error);
    assert.equal(error.name, "AbortError");
    return true;
  });

  assert.deepEqual(seenOptions?.headers, { "x-test": "1" });
  const reason: unknown = seenOptions?.signal?.reason;
  assert.ok(reason instanceof ConnectError);
  assert.equal(reason.code, Code.Canceled);
  assert.equal(returnSpy.called, true);
});

test("rpcClient.resetConnection aborts the HTTP/2 session", () => {
  let calls = 0;
  const service = Object.create(AgentService.prototype) as AgentService;
  Reflect.set(service as object, "client", {});
  Reflect.set(service as object, "sessionManager", {
    abort() {
      calls++;
    },
  });

  service.rpcClient.resetConnection?.();
  assert.equal(calls, 1);
});

test("an aborted run cancels the HTTP/2 stream with CANCEL", async () => {
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => unhandled.push(reason);
  process.on("unhandledRejection", onUnhandled);

  let resolveRstCode!: (code: number) => void;
  const rstCode = new Promise<number>((resolve) => {
    resolveRstCode = resolve;
  });
  const server = http2.createServer();
  server.on("stream", (stream) => {
    stream.on("close", () => resolveRstCode(stream.rstCode));
    stream.on("error", () => {});
    stream.respond({
      ":status": 200,
      "content-type": "application/connect+proto",
    });
  });
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const { port } = server.address() as AddressInfo;

  const service = new AgentService(`http://localhost:${port}`, {
    accessToken: "token",
    clientType: "test",
    clientVersion: "0",
  });
  const controller = new AbortController();
  const requests = createWritableIterable<AgentClientMessage>();
  const stream = service.rpcClient.run(requests, {
    signal: controller.signal,
  });

  try {
    const next = stream[Symbol.asyncIterator]().next();
    await new Promise((resolve) => setTimeout(resolve, 50));
    controller.abort(new Error("user cancelled"));
    await assert.rejects(next);
    assert.equal(await rstCode, http2.constants.NGHTTP2_CANCEL);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.deepEqual(unhandled, []);
  } finally {
    process.off("unhandledRejection", onUnhandled);
    service.rpcClient.resetConnection?.();
    await new Promise((resolve) => server.close(resolve));
  }
});
