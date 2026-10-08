import os from "node:os";
import path from "node:path";
import {
  MIN_STALL_THRESHOLD_MS,
  STALL_THRESHOLD_MS,
} from "../vendor/agent-client/stall-detector";

const PI_CODING_AGENT_DIR =
  process.env["PI_CODING_AGENT_DIR"] || path.join(os.homedir(), ".pi", "agent");

export const PI_CURSOR_AGENT_CACHE_DIR = path.join(
  PI_CODING_AGENT_DIR,
  "cache",
  "pi-cursor-agent",
);

export const PI_CURSOR_AGENT_MODELS_CACHE_FILE = path.join(
  PI_CURSOR_AGENT_CACHE_DIR,
  "models.json",
);

export const PI_CURSOR_AGENT_MODELS_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

export const PI_CURSOR_AGENT_LOGS_DIR = path.join(
  PI_CODING_AGENT_DIR,
  "cursor-agent",
  "logs",
);

/**
 * Cursor project slug: absolute path with every non-alphanumeric run collapsed
 * to one dash. `/home/ada/.config/ghostty` becomes `home-ada-config-ghostty`.
 */
export function slugifyCursorProjectPath(workspacePath: string): string {
  return path
    .resolve(workspacePath)
    .replace(/[^a-zA-Z0-9]/g, "-")
    .split("-")
    .filter((part) => part.length > 0)
    .join("-");
}

/** Metadata root Cursor uses for `agent-tools/`. */
export function cursorProjectDir(
  workspacePath: string,
  agentDir: string = PI_CODING_AGENT_DIR,
): string {
  return path.join(
    agentDir,
    "cursor-agent",
    "projects",
    slugifyCursorProjectPath(workspacePath),
  );
}

/** Unset or invalid: Cursor's default. `0` or less disables stall detection. */
export function parseStallTimeout(raw: string | undefined): number {
  const value =
    raw === undefined || raw.trim() === "" ? Number.NaN : Number(raw);
  if (!Number.isFinite(value)) return STALL_THRESHOLD_MS;
  if (value <= 0) return 0;
  return Math.max(Math.round(value), MIN_STALL_THRESHOLD_MS);
}

export const PI_CURSOR_AGENT_STALL_TIMEOUT_MS = parseStallTimeout(
  process.env["PI_CURSOR_AGENT_STALL_TIMEOUT_MS"],
);
