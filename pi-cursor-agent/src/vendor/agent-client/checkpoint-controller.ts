import type { ConversationStateStructure } from "../../__generated__/agent/v1/agent_pb";

export interface CheckpointHandler {
  handleCheckpoint(
    ctx: unknown,
    checkpoint: ConversationStateStructure,
  ): Promise<void>;
  getLatestCheckpoint?: () => ConversationStateStructure | undefined;
}

export class CheckpointController {
  private readonly checkpointStream: AsyncIterable<ConversationStateStructure>;
  private readonly checkpointHandler: CheckpointHandler;
  private readonly ctx: unknown;
  private readonly beforeHandle: (() => Promise<void>) | undefined;

  /**
   * `beforeHandle` is called as each checkpoint is read; the checkpoint is
   * handled once it settles. Passing the interaction controller's `whenIdle`
   * applies each checkpoint after the updates that preceded it on the wire.
   */
  constructor(
    checkpointStream: AsyncIterable<ConversationStateStructure>,
    checkpointHandler: CheckpointHandler,
    ctx: unknown,
    beforeHandle?: () => Promise<void>,
  ) {
    this.checkpointStream = checkpointStream;
    this.checkpointHandler = checkpointHandler;
    this.ctx = ctx;
    this.beforeHandle = beforeHandle;
  }

  async run(): Promise<void> {
    const ctx = this.ctx;
    const promises: Promise<void>[] = [];
    for await (const checkpoint of this.checkpointStream) {
      const ready = this.beforeHandle?.() ?? Promise.resolve();
      promises.push(
        ready.then(() =>
          this.checkpointHandler.handleCheckpoint(ctx, checkpoint),
        ),
      );
    }
    await Promise.all(promises);
  }
}
