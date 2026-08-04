import type { ToolResultMessage } from "@mariozechner/pi-ai";
import type { ExtensionContext } from "@mariozechner/pi-coding-agent";
import type {
  ShellArgs,
  ShellResult,
} from "../../../__generated__/agent/v1/shell_exec_pb";
import {
  ShellFailure,
  ShellRejected,
  ShellResult as ShellResultClass,
  ShellSuccess,
} from "../../../__generated__/agent/v1/shell_exec_pb";
import { getSkipApprovalEnabled } from "../../../lib/skip-approval";
import type { Executor } from "../../../vendor/agent-exec";
import { toolResultToText } from "../../shared/tool-result";
import {
  decodeToolCallId,
  type PiToolContext,
} from "../local-resource-provider/types";
import { requestToolExecution } from "../tool-bridge";

export function buildShellResultFromToolResult(
  args: { command: string; workingDirectory: string },
  result: ToolResultMessage,
): ShellResult {
  const output = toolResultToText(result);
  if (result.isError) {
    return new ShellResultClass({
      result: {
        case: "failure",
        value: new ShellFailure({
          command: args.command,
          workingDirectory: args.workingDirectory,
          exitCode: 1,
          signal: "",
          stdout: "",
          stderr: output || "Shell failed",
          executionTime: 0,
          aborted: false,
        }),
      },
    });
  }
  return new ShellResultClass({
    result: {
      case: "success",
      value: new ShellSuccess({
        command: args.command,
        workingDirectory: args.workingDirectory,
        exitCode: 0,
        signal: "",
        stdout: output,
        stderr: "",
        executionTime: 0,
      }),
    },
  });
}

export function buildShellRejectedResult(
  command: string,
  workingDirectory: string,
  reason: string,
): ShellResult {
  return new ShellResultClass({
    result: {
      case: "rejected",
      value: new ShellRejected({
        command,
        workingDirectory,
        reason,
        isReadonly: false,
      }),
    },
  });
}

type ShellLexeme =
  | { type: "token"; value: string }
  | { type: "operator"; value: string };

interface ShellSegment {
  tokens: string[];
  trailingOperator?: string;
}

interface ResolvedExecutable {
  name: string;
  index: number;
}

const DANGEROUS_EXECUTABLES = new Set(["dd", "shutdown", "reboot"]);
const WRAPPER_EXECUTABLES = new Set([
  "sudo",
  "env",
  "command",
  "builtin",
  "nohup",
  "time",
]);
const NETWORK_FETCH_EXECUTABLES = new Set(["curl", "wget"]);
const SHELL_INTERPRETERS = new Set(["sh", "bash", "zsh", "dash", "ksh"]);

function tokenizeShell(command: string): ShellLexeme[] {
  const lexemes: ShellLexeme[] = [];
  let current = "";
  let quote: "'" | '"' | null = null;
  let i = 0;

  const pushToken = () => {
    if (!current) {
      return;
    }
    lexemes.push({ type: "token", value: current });
    current = "";
  };

  while (i < command.length) {
    const ch = command[i] ?? "";

    if (quote === null) {
      if (ch === "'" || ch === '"') {
        quote = ch;
        i += 1;
        continue;
      }

      if (ch === "\\") {
        const next = command[i + 1];
        if (next !== undefined) {
          current += next;
          i += 2;
          continue;
        }
        i += 1;
        continue;
      }

      if (ch === "\n") {
        pushToken();
        lexemes.push({ type: "operator", value: "\n" });
        i += 1;
        continue;
      }

      if (ch === " " || ch === "\t" || ch === "\r") {
        pushToken();
        i += 1;
        continue;
      }

      if (ch === "&") {
        pushToken();
        if (command[i + 1] === "&") {
          lexemes.push({ type: "operator", value: "&&" });
          i += 2;
        } else {
          lexemes.push({ type: "operator", value: "&" });
          i += 1;
        }
        continue;
      }

      if (ch === "|") {
        pushToken();
        if (command[i + 1] === "|") {
          lexemes.push({ type: "operator", value: "||" });
          i += 2;
        } else if (command[i + 1] === "&") {
          lexemes.push({ type: "operator", value: "|&" });
          i += 2;
        } else {
          lexemes.push({ type: "operator", value: "|" });
          i += 1;
        }
        continue;
      }

      if (ch === ";") {
        pushToken();
        lexemes.push({ type: "operator", value: ";" });
        i += 1;
        continue;
      }

      current += ch;
      i += 1;
      continue;
    }

    if (quote === "'") {
      if (ch === "'") {
        quote = null;
      } else {
        current += ch;
      }
      i += 1;
      continue;
    }

    if (ch === '"') {
      quote = null;
      i += 1;
      continue;
    }

    if (ch === "\\") {
      const next = command[i + 1];
      if (
        next === '"' ||
        next === "\\" ||
        next === "$" ||
        next === "`" ||
        next === "\n"
      ) {
        current += next;
        i += 2;
        continue;
      }
    }

    current += ch;
    i += 1;
  }

  pushToken();
  return lexemes;
}

function splitIntoSegments(lexemes: ShellLexeme[]): ShellSegment[] {
  const segments: ShellSegment[] = [];
  let current: string[] = [];

  for (const lexeme of lexemes) {
    if (lexeme.type === "token") {
      current.push(lexeme.value);
      continue;
    }

    if (current.length > 0) {
      segments.push({ tokens: current, trailingOperator: lexeme.value });
      current = [];
    }
  }

  if (current.length > 0) {
    segments.push({ tokens: current });
  }

  return segments;
}

function isEnvAssignment(token: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*=.*/.test(token);
}

function normalizeExecutableName(token: string): string {
  const unescaped = token.replace(/^\\+/, "");
  const parts = unescaped.split("/");
  const base = parts[parts.length - 1] ?? unescaped;
  return base.toLowerCase();
}

function resolveExecutable(tokens: string[]): ResolvedExecutable | null {
  let index = 0;

  while (index < tokens.length) {
    const token = tokens[index] ?? "";

    if (token === "--") {
      index += 1;
      continue;
    }

    if (isEnvAssignment(token)) {
      index += 1;
      continue;
    }

    const name = normalizeExecutableName(token);
    if (!name) {
      index += 1;
      continue;
    }

    if (!WRAPPER_EXECUTABLES.has(name)) {
      return { name, index };
    }

    index += 1;

    if (name === "sudo") {
      while (index < tokens.length) {
        const value = tokens[index] ?? "";
        if (value === "--") {
          index += 1;
          break;
        }
        if (value.startsWith("-")) {
          index += 1;
          continue;
        }
        break;
      }
      continue;
    }

    if (name === "env") {
      while (index < tokens.length) {
        const value = tokens[index] ?? "";
        if (value === "--") {
          index += 1;
          break;
        }
        if (value.startsWith("-") || isEnvAssignment(value)) {
          index += 1;
          continue;
        }
        break;
      }
      continue;
    }

    while (index < tokens.length) {
      const value = tokens[index] ?? "";
      if (value === "--") {
        index += 1;
        break;
      }
      if (value.startsWith("-")) {
        index += 1;
        continue;
      }
      break;
    }
  }

  return null;
}

function hasSudoPrefix(tokens: string[]): boolean {
  let index = 0;
  while (index < tokens.length && isEnvAssignment(tokens[index] ?? "")) {
    index += 1;
  }

  while (index < tokens.length) {
    const token = tokens[index] ?? "";
    if (token === "--") {
      index += 1;
      continue;
    }

    const name = normalizeExecutableName(token);
    if (!name) {
      index += 1;
      continue;
    }

    if (name === "sudo") {
      return true;
    }

    if (!WRAPPER_EXECUTABLES.has(name)) {
      return false;
    }

    index += 1;

    if (name === "env") {
      while (index < tokens.length) {
        const value = tokens[index] ?? "";
        if (value === "--") {
          index += 1;
          break;
        }
        if (value.startsWith("-") || isEnvAssignment(value)) {
          index += 1;
          continue;
        }
        break;
      }
      continue;
    }

    while (index < tokens.length) {
      const value = tokens[index] ?? "";
      if (value === "--") {
        index += 1;
        break;
      }
      if (value.startsWith("-")) {
        index += 1;
        continue;
      }
      break;
    }
  }

  return false;
}

function hasRecursiveForceFlags(
  tokens: string[],
  executableIndex: number,
): boolean {
  let hasRecursive = false;
  let hasForce = false;

  for (let i = executableIndex + 1; i < tokens.length; i += 1) {
    const token = tokens[i] ?? "";

    if (token.startsWith("--")) {
      if (token === "--recursive") {
        hasRecursive = true;
      }
      if (token === "--force") {
        hasForce = true;
      }
    } else if (token.startsWith("-") && token.length > 1) {
      const flags = token.slice(1);
      if (flags.includes("r") || flags.includes("R")) {
        hasRecursive = true;
      }
      if (flags.includes("f")) {
        hasForce = true;
      }
    }

    if (hasRecursive && hasForce) {
      return true;
    }
  }

  return false;
}

export function isDangerousShellCommand(command: string): boolean {
  const segments = splitIntoSegments(tokenizeShell(command));
  if (segments.length === 0) {
    return false;
  }

  const resolved = segments.map((segment) => ({
    ...segment,
    executable: resolveExecutable(segment.tokens),
    hasSudoPrefix: hasSudoPrefix(segment.tokens),
  }));

  if (resolved.some((segment) => segment.hasSudoPrefix)) {
    return true;
  }

  for (const segment of resolved) {
    const executable = segment.executable;
    if (!executable) {
      continue;
    }

    if (
      executable.name === "rm" &&
      hasRecursiveForceFlags(segment.tokens, executable.index)
    ) {
      return true;
    }

    if (executable.name.startsWith("mkfs")) {
      return true;
    }

    if (DANGEROUS_EXECUTABLES.has(executable.name)) {
      return true;
    }
  }

  for (let i = 0; i < resolved.length - 1; i += 1) {
    const left = resolved[i];
    const right = resolved[i + 1];
    if (!left || !right) {
      continue;
    }

    if (left.trailingOperator !== "|" && left.trailingOperator !== "|&") {
      continue;
    }

    const leftCommand = left.executable?.name;
    const rightCommand = right.executable?.name;

    if (!leftCommand || !rightCommand) {
      continue;
    }

    if (
      NETWORK_FETCH_EXECUTABLES.has(leftCommand) &&
      SHELL_INTERPRETERS.has(rightCommand)
    ) {
      return true;
    }
  }

  return false;
}

export async function confirmIfDangerous(
  getCtx: () => ExtensionContext | null,
  command: string,
): Promise<boolean> {
  if (!isDangerousShellCommand(command)) return true;
  if (getSkipApprovalEnabled()) return true;
  const ctx = getCtx();
  if (!ctx?.hasUI) return false;
  return ctx.ui.confirm("Cursor command approval", command);
}

export class LocalShellExecutor implements Executor<ShellArgs, ShellResult> {
  private readonly ctx: PiToolContext;

  constructor(ctx: PiToolContext) {
    this.ctx = ctx;
  }

  async execute(_ctx: unknown, args: ShellArgs): Promise<ShellResult> {
    const toolCallId = decodeToolCallId(args.toolCallId);
    const workingDirectory = args.workingDirectory || this.ctx.cwd;

    if (!this.ctx.getActiveTools().has("bash")) {
      return buildShellRejectedResult(
        args.command,
        workingDirectory,
        "Tool not available",
      );
    }

    const approved = await confirmIfDangerous(this.ctx.getCtx, args.command);
    if (!approved) {
      return buildShellRejectedResult(
        args.command,
        workingDirectory,
        "Command rejected",
      );
    }

    const timeoutSeconds =
      args.timeout && args.timeout > 0 ? args.timeout : undefined;

    const piResult = await requestToolExecution(
      this.ctx.getChannel?.() ?? null,
      {
        toolCallId,
        cursorExecType: "shell",
        piToolName: "bash",
        piToolArgs: {
          command: args.command,
          ...(timeoutSeconds != null ? { timeout: timeoutSeconds } : {}),
        },
      },
    );

    return buildShellResultFromToolResult(
      { command: args.command, workingDirectory },
      piResult,
    );
  }
}
