import type { ExtensionContext } from "@earendil-works/pi-coding-agent";

const STATUS_ID = "skip-approval";
const STATUS_LABEL = "▶ skipping approvals";

let skipApprovalEnabled = false;

export function getSkipApprovalEnabled(): boolean {
  return skipApprovalEnabled;
}

export function setSkipApprovalEnabled(enabled: boolean): void {
  skipApprovalEnabled = enabled;
}

export const SKIP_APPROVAL_OPTIONS = ["on", "off"] as const;
export type SkipApprovalOption = (typeof SKIP_APPROVAL_OPTIONS)[number];
export type SkipApprovalAction = SkipApprovalOption | "toggle";

function isSkipApprovalOption(value: string): value is SkipApprovalOption {
  return (SKIP_APPROVAL_OPTIONS as readonly string[]).includes(value);
}

export function parseSkipApprovalArgs(
  args: string,
): SkipApprovalAction | undefined {
  const trimmed = args.trim().toLowerCase();
  if (!trimmed) return "toggle";
  if (isSkipApprovalOption(trimmed)) return trimmed;
  return undefined;
}

export function resolveSkipApprovalEnabled(
  action: SkipApprovalAction,
): boolean {
  if (action === "on") return true;
  if (action === "off") return false;
  return !skipApprovalEnabled;
}

export function updateSkipApprovalStatus(ctx: ExtensionContext): void {
  if (!ctx.hasUI) return;
  if (skipApprovalEnabled) {
    ctx.ui.setStatus(STATUS_ID, ctx.ui.theme.fg("warning", STATUS_LABEL));
    return;
  }
  ctx.ui.setStatus(STATUS_ID, undefined);
}
