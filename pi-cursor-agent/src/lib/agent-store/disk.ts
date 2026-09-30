import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { type AgentMetadata, fromHex, toHex } from "../../vendor/agent-kv";

interface BlobEntry {
  id: string;
  data: string;
}

interface BlobsFile {
  version: 1;
  blobs: BlobEntry[];
}

interface MetaFile {
  version: 1;
  agentId: string;
  latestRootBlobId: string;
  name: string;
  createdAt: number;
  mode: string;
  lastUsedModel?: string;
}

const getSessionDir = (baseDir: string, sessionId: string): string =>
  path.join(baseDir, "chats", sessionId);

const getBlobsFilePath = (baseDir: string, sessionId: string): string =>
  path.join(getSessionDir(baseDir, sessionId), "blobs.json");

const getMetaFilePath = (baseDir: string, sessionId: string): string =>
  path.join(getSessionDir(baseDir, sessionId), "meta.json");

/** Tells a write whether it may still replace the files once they are ready. */
export type ShouldCommit = () => boolean;

export const loadBlobsFromDisk = async (
  baseDir: string,
  sessionId: string,
): Promise<Map<string, Uint8Array>> => {
  try {
    const text = await fs.readFile(
      getBlobsFilePath(baseDir, sessionId),
      "utf-8",
    );
    const parsed = JSON.parse(text) as BlobsFile;
    if (!parsed || parsed.version !== 1 || !Array.isArray(parsed.blobs)) {
      return new Map();
    }
    const map = new Map<string, Uint8Array>();
    for (const entry of parsed.blobs) {
      if (
        !entry ||
        typeof entry.id !== "string" ||
        typeof entry.data !== "string"
      )
        continue;
      try {
        map.set(entry.id, new Uint8Array(Buffer.from(entry.data, "base64")));
      } catch {
        // skip corrupt entries
      }
    }
    return map;
  } catch {
    return new Map();
  }
};

const blobsFileContents = (blobs: Map<string, Uint8Array>): string => {
  const file: BlobsFile = {
    version: 1,
    blobs: Array.from(blobs.entries()).map(([id, data]) => ({
      id,
      data: Buffer.from(data).toString("base64"),
    })),
  };
  return JSON.stringify(file);
};

export const loadMetaFromDisk = async (
  baseDir: string,
  sessionId: string,
): Promise<AgentMetadata | null> => {
  try {
    const text = await fs.readFile(
      getMetaFilePath(baseDir, sessionId),
      "utf-8",
    );
    const parsed = JSON.parse(text) as MetaFile;
    if (!parsed || parsed.version !== 1 || typeof parsed.agentId !== "string") {
      return null;
    }
    return {
      agentId: parsed.agentId,
      latestRootBlobId: parsed.latestRootBlobId
        ? fromHex(parsed.latestRootBlobId)
        : new Uint8Array(),
      name: parsed.name ?? "New Agent",
      createdAt: parsed.createdAt ?? Date.now(),
      mode: (parsed.mode as AgentMetadata["mode"]) ?? "default",
      ...(parsed.lastUsedModel != null && {
        lastUsedModel: parsed.lastUsedModel,
      }),
    };
  } catch {
    return null;
  }
};

const metaFileContents = (metadata: AgentMetadata): string => {
  const file: MetaFile = {
    version: 1,
    agentId: metadata.agentId,
    latestRootBlobId: toHex(metadata.latestRootBlobId),
    name: metadata.name,
    createdAt: metadata.createdAt,
    mode: metadata.mode,
    ...(metadata.lastUsedModel != null && {
      lastUsedModel: metadata.lastUsedModel,
    }),
  };
  return JSON.stringify(file);
};

/** Commit phases per session directory, so an older commit can't land after a newer one. */
const commitQueues = new Map<string, Promise<void>>();

const inCommitQueue = async (
  dir: string,
  commit: () => Promise<void>,
): Promise<void> => {
  const previous = commitQueues.get(dir) ?? Promise.resolve();
  const current = previous.then(commit);
  const settled = current.catch(() => {});
  commitQueues.set(dir, settled);
  try {
    await current;
  } finally {
    if (commitQueues.get(dir) === settled) commitQueues.delete(dir);
  }
};

/**
 * Write both store files. Each is staged under a unique temporary name. Then,
 * in the session's commit queue, `shouldCommit` is asked once: if it allows,
 * blobs are renamed into place before metadata, so metadata never names a
 * root blob missing from disk.
 */
export const saveStoreToDisk = async (
  baseDir: string,
  sessionId: string,
  blobs: Map<string, Uint8Array>,
  metadata: AgentMetadata,
  shouldCommit?: ShouldCommit,
): Promise<void> => {
  const dir = getSessionDir(baseDir, sessionId);
  await fs.mkdir(dir, { recursive: true });
  const staged = [
    {
      path: getBlobsFilePath(baseDir, sessionId),
      contents: blobsFileContents(blobs),
    },
    {
      path: getMetaFilePath(baseDir, sessionId),
      contents: metaFileContents(metadata),
    },
  ].map((file) => ({ ...file, tmpPath: `${file.path}.${randomUUID()}.tmp` }));

  try {
    await Promise.all(
      staged.map((file) => fs.writeFile(file.tmpPath, file.contents, "utf-8")),
    );
    await inCommitQueue(dir, async () => {
      if (shouldCommit && !shouldCommit()) return;
      for (const file of staged) {
        await fs.rename(file.tmpPath, file.path);
      }
    });
  } finally {
    await Promise.all(
      staged.map((file) => fs.rm(file.tmpPath, { force: true })),
    );
  }
};
