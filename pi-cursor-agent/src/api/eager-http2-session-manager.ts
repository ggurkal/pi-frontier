import type * as http2 from "node:http2";
import { Http2SessionManager } from "@connectrpc/connect-node";

/**
 * Workaround for a Bun `node:http2` bug; remove once the Bun that pi ships
 * includes https://github.com/oven-sh/bun/pull/43754.
 *
 * Cursor ends a Run response and then sends RST_STREAM(NO_ERROR), because the
 * request side of the bidi stream is still open (RFC 9113 section 8.1). Bun
 * then discards response data nothing has read yet, so Connect's async
 * iterator misses the final messages and fails with `[unknown] Premature
 * close`. Reading each response eagerly into a queue keeps that data. Node
 * delivers it to paused readers, but can still fail the same way when the
 * reader lags (https://github.com/nodejs/node/issues/65677).
 */
export class EagerHttp2SessionManager extends Http2SessionManager {
  override async request(
    method: string,
    path: string,
    headers: http2.OutgoingHttpHeaders,
    options: Omit<http2.ClientSessionRequestOptions, "signal">,
  ): Promise<http2.ClientHttp2Stream> {
    const stream = await super.request(method, path, headers, options);
    readEagerly(stream);
    return stream;
  }
}

function prematureCloseError(): Error {
  return Object.assign(new Error("Premature close"), {
    code: "ERR_STREAM_PREMATURE_CLOSE",
  });
}

/**
 * Buffers `stream`'s data as it arrives and serves it through the stream's
 * async iterator, which is how Connect reads response bodies.
 */
export function readEagerly(stream: http2.ClientHttp2Stream): void {
  const chunks: Buffer[] = [];
  let ended = false;
  let failure: unknown;
  let wake: (() => void) | undefined;
  const notify = () => {
    wake?.();
    wake = undefined;
  };

  stream.on("data", (chunk: Buffer) => {
    chunks.push(chunk);
    notify();
  });
  stream.on("end", () => {
    ended = true;
    notify();
  });
  stream.on("error", (error) => {
    failure ??= error;
    notify();
  });
  stream.on("close", () => {
    if (!ended) failure ??= prematureCloseError();
    notify();
  });

  const iterator: AsyncIterableIterator<Buffer> = {
    async next() {
      while (true) {
        const chunk = chunks.shift();
        if (chunk) return { done: false, value: chunk };
        if (ended) return { done: true, value: undefined };
        if (failure !== undefined) throw failure;
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    },
    async return() {
      stream.destroy();
      return { done: true, value: undefined };
    },
    [Symbol.asyncIterator]() {
      return iterator;
    },
  };
  Object.defineProperty(stream, Symbol.asyncIterator, {
    configurable: true,
    value: () => iterator,
  });
}
