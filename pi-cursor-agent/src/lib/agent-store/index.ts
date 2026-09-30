import { AgentStore, getDefaultAgentMetadata } from "../../vendor/agent-kv";
import {
  loadBlobsFromDisk,
  loadMetaFromDisk,
  type ShouldCommit,
  saveStoreToDisk,
} from "./disk";
import { JsonBlobStoreWithMetadata } from "./json-blob-store";

interface StoreEntry {
  store: AgentStore;
  jsonStore: JsonBlobStoreWithMetadata;
}

let sessionStores = new Map<string, StoreEntry>();
/** In-flight loads. Dropping a store removes its load, which then does not publish. */
const loading = new Map<string, Promise<StoreEntry>>();

const invalidate = (sessionId: string): void => {
  loading.delete(sessionId);
};

const loadStore = async (
  baseDir: string,
  sessionId: string,
): Promise<StoreEntry> => {
  const [blobs, meta] = await Promise.all([
    loadBlobsFromDisk(baseDir, sessionId),
    loadMetaFromDisk(baseDir, sessionId),
  ]);

  const metadata = meta ?? getDefaultAgentMetadata();
  const jsonStore = new JsonBlobStoreWithMetadata(blobs, metadata);
  const store = new AgentStore(jsonStore, jsonStore);

  if (metadata.latestRootBlobId.length > 0) {
    await store.resetFromDb(null);
  }

  return { store, jsonStore };
};

/**
 * Concurrent callers share one load. A load that finishes after the store
 * was dropped is discarded and its callers get an error: the drop may have
 * persisted newer state, and a dropped session must not come back.
 */
export const ensureAgentStore = async (
  baseDir: string,
  sessionId: string,
): Promise<StoreEntry> => {
  const existing = sessionStores.get(sessionId);
  if (existing) {
    return existing;
  }

  let pending = loading.get(sessionId);
  if (!pending) {
    const load: Promise<StoreEntry> = loadStore(baseDir, sessionId).then(
      (entry) => {
        if (loading.get(sessionId) === load) {
          loading.delete(sessionId);
          sessionStores.set(sessionId, entry);
        }
        return entry;
      },
      (error: unknown) => {
        if (loading.get(sessionId) === load) loading.delete(sessionId);
        throw error;
      },
    );
    pending = load;
    loading.set(sessionId, load);
  }

  const entry = await pending;
  if (sessionStores.get(sessionId) !== entry) {
    throw new Error(`Agent store for ${sessionId} was dropped while loading`);
  }
  return entry;
};

/**
 * Write the session's store to disk. The files are only replaced if
 * `shouldCommit` still allows it and the store was not dropped or reloaded
 * meanwhile, so a late write never overwrites newer state.
 */
export const persistAgentStore = async (
  baseDir: string,
  sessionId: string,
  shouldCommit?: ShouldCommit,
): Promise<StoreEntry | null> => {
  const entry = sessionStores.get(sessionId);
  if (!entry) {
    return null;
  }

  const commit = () =>
    sessionStores.get(sessionId) === entry && (shouldCommit?.() ?? true);
  await saveStoreToDisk(
    baseDir,
    sessionId,
    entry.jsonStore.blobs,
    entry.jsonStore.metadata,
    commit,
  );

  return entry;
};

export const applySnapshotToStore = async (
  entry: StoreEntry,
  agentId: string,
  latestRootBlobId: Uint8Array,
): Promise<void> => {
  entry.jsonStore.metadata.agentId = agentId;
  entry.jsonStore.metadata.latestRootBlobId = latestRootBlobId;

  if (latestRootBlobId.length > 0) {
    await entry.store.resetFromDb(null);
  }
};

export const deleteAgentStore = (sessionId: string): boolean => {
  invalidate(sessionId);
  return sessionStores.delete(sessionId);
};

export const hasAgentStore = (sessionId: string): boolean => {
  return sessionStores.has(sessionId);
};

export const retainOnlyAgentStore = (sessionId: string | null): void => {
  for (const id of [...loading.keys()]) {
    if (id !== sessionId) invalidate(id);
  }
  const entry = sessionId ? sessionStores.get(sessionId) : undefined;
  sessionStores =
    sessionId && entry ? new Map([[sessionId, entry]]) : new Map();
};
