import type {
  InteractionQuery,
  InteractionResponse,
  InteractionUpdate,
} from "../../__generated__/agent/v1/agent_pb";
import {
  type CoreInteractionQuery,
  type CoreInteractionResponse,
  type CoreInteractionUpdate,
  convertInteractionResponseToProto,
  convertProtoToInteractionQuery,
  convertProtoToInteractionUpdate,
} from "../agent-core/interaction-conversion";
import type { InteractionMessage } from "./split-stream";

export interface Writable<T> {
  write(value: T): Promise<void>;
}

export interface InteractionListener {
  sendUpdate(ctx: unknown, update: CoreInteractionUpdate): Promise<void>;
  query(
    ctx: unknown,
    query: CoreInteractionQuery,
  ): Promise<CoreInteractionResponse>;
}

export class ClientInteractionController {
  private readonly interactionStream: AsyncIterable<InteractionMessage>;
  private readonly interactionListener: InteractionListener;
  private readonly queryResponseStream: Writable<InteractionResponse>;
  /** Updates are applied in order on this chain; it never rejects. */
  private tail: Promise<void> = Promise.resolve();

  constructor(
    interactionStream: AsyncIterable<InteractionMessage>,
    interactionListener: InteractionListener,
    queryResponseStream: Writable<InteractionResponse>,
  ) {
    this.interactionStream = interactionStream;
    this.interactionListener = interactionListener;
    this.queryResponseStream = queryResponseStream;
  }

  /** Settles once every update read so far has been applied. */
  whenIdle(): Promise<void> {
    return this.tail;
  }

  async run(ctx: unknown): Promise<void> {
    let firstError: Error | undefined;

    for await (const message of this.interactionStream) {
      if (message.case === "interactionQuery") {
        this.handleInteractionQuery(ctx, message.value);
      } else if (message.case === "interactionUpdate") {
        this.tail = this.tail
          .then(() => this.handleInteractionUpdate(ctx, message.value))
          .catch((error: unknown) => {
            console.error("Error handling interaction update", error);
            firstError ??=
              error instanceof Error ? error : new Error(String(error));
          });
      }
    }

    await this.tail;
    if (firstError !== undefined) {
      throw firstError;
    }
  }

  private async handleInteractionUpdate(
    ctx: unknown,
    update: InteractionUpdate,
  ): Promise<void> {
    const coreUpdate = convertProtoToInteractionUpdate(update);
    if (coreUpdate) {
      await this.interactionListener.sendUpdate(ctx, coreUpdate);
    }
  }

  private handleInteractionQuery(
    ctx: unknown,
    queryProto: InteractionQuery,
  ): void {
    const coreQuery = convertProtoToInteractionQuery(queryProto);
    void this.interactionListener
      .query(ctx, coreQuery)
      .then((response) => {
        const responseProto = convertInteractionResponseToProto(
          response,
          queryProto.id,
          coreQuery.type,
        );
        return this.queryResponseStream.write(responseProto);
      })
      .catch((error) => {
        console.error("Error handling interaction query", error);
      });
  }
}
