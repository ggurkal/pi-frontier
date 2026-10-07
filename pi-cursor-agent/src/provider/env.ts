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
