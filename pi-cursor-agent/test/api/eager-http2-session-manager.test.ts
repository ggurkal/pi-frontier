import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import http2 from "node:http2";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  EagerHttp2SessionManager,
  readEagerly,
} from "../../src/api/eager-http2-session-manager.js";

const CHUNKS = Array.from({ length: 20 }, (_, i) => `chunk-${i}\n`);

let server: http2.Http2Server;
let baseUrl: string;

before(async () => {
  server = http2.createServer();
  server.on("stream", (stream) => {
    stream.respond({ ":status": 200 });
    for (const chunk of CHUNKS) stream.write(chunk);
    // Ends the response, then resets the still-open request side.
    stream.end(() => stream.close(http2.constants.NGHTTP2_NO_ERROR));
  });
  await new Promise<void>((resolve) => server.listen(0, resolve));
  baseUrl = `http://localhost:${(server.address() as AddressInfo).port}`;
});

after(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

async function readSlowly(
  manager: EagerHttp2SessionManager,
  path: string,
): Promise<string> {
  const stream = await manager.request(
    "POST",
    path,
    { ":method": "POST", ":path": path },
    {},
  );
  stream.write("request still open");
  let body = "";
  for await (const chunk of stream) {
    body += chunk;
    await sleep(5);
  }
  return body;
}

test("keeps response data that arrived before RST_STREAM(NO_ERROR)", async () => {
  const manager = new EagerHttp2SessionManager(baseUrl);
  try {
    assert.equal(await readSlowly(manager, "/"), CHUNKS.join(""));
  } finally {
    manager.abort();
  }
});

function fakeStream(): http2.ClientHttp2Stream & EventEmitter {
  return Object.assign(new EventEmitter(), {
    destroy() {},
  }) as unknown as http2.ClientHttp2Stream & EventEmitter;
}

test("fails a response that closes before it ends", async () => {
  const stream = fakeStream();
  readEagerly(stream);
  stream.emit("data", Buffer.from("partial"));
  stream.emit("close");

  const iterator = stream[Symbol.asyncIterator]();
  assert.equal(String((await iterator.next()).value), "partial");
  await assert.rejects(iterator.next(), {
    code: "ERR_STREAM_PREMATURE_CLOSE",
  });
});

test("fails a response with the stream's error", async () => {
  const stream = fakeStream();
  readEagerly(stream);
  const iterator = stream[Symbol.asyncIterator]();
  const next = iterator.next();
  stream.emit("error", new Error("boom"));
  stream.emit("close");

  await assert.rejects(next, { message: "boom" });
});
