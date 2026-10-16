import { InstanceApiError } from "./api";

export type RefusalPlace = "conversation" | "work" | "files";

export interface Refusal {
  status?: number;
  name?: string;
  reason?: string;
  executionReason?: string;
  detail?: string;
  extra: Record<string, unknown>;
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function text(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function own<T>(table: Record<string, T>, key: string): T | undefined {
  return Object.hasOwn(table, key) ? table[key] : undefined;
}

function bodyRefusal(
  body: Record<string, unknown>,
  status?: number,
): Refusal | null {
  const { reason, execution_reason, ...extra } = body;
  const parsed = text(reason);
  if (!parsed) return null;
  return {
    ...(status === undefined ? {} : { status }),
    reason: parsed,
    ...(text(execution_reason)
      ? { executionReason: text(execution_reason) }
      : {}),
    ...(typeof body.detail === "string" ? { detail: body.detail } : {}),
    extra,
  };
}

/** Read transport metadata without ever turning diagnostic prose into UI copy. */
export function readRefusal(source: unknown): Refusal | null {
  if (source instanceof InstanceApiError) {
    if (!text(source.reason)) return null;
    return {
      status: source.status,
      reason: source.reason,
      ...(source.executionReason
        ? { executionReason: source.executionReason }
        : {}),
      detail: source.message,
      extra: source.extra,
    };
  }
  const value = record(source);
  if (!value) return null;
  if (typeof value.status === "number" && typeof value.message === "string") {
    const prefix = `HTTP ${value.status}: `;
    if (!value.message.startsWith(prefix)) return null;
    try {
      const body = record(JSON.parse(value.message.slice(prefix.length)));
      return body ? bodyRefusal(body, value.status) : null;
    } catch {
      return null;
    }
  }
  // Only an explicitly supplied frame name counts; Error.prototype.name does not.
  const name = Object.hasOwn(value, "name") ? text(value.name) : undefined;
  if (name && typeof value.message === "string") {
    return {
      name,
      ...(text(value.reason) ? { reason: text(value.reason) } : {}),
      detail: value.message,
      extra: {},
    };
  }
  return bodyRefusal(value);
}

const refusals = new Map<string, Refusal>();

export function rememberRefusal(threadId: string, refusal: Refusal): void {
  refusals.set(threadId, refusal);
}

export function takeRefusal(threadId: string): Refusal | null {
  const refusal = refusals.get(threadId) ?? null;
  refusals.delete(threadId);
  return refusal;
}

const frameReasons: Record<string, string> = {
  ResidentBusy: "busy",
  ResidentRunLimit: "run-limit",
  ResidentRestarting: "restarting",
  ResidentStartingElsewhere: "starting-elsewhere",
  ResidentStartFailed: "start-failed",
  ResidentFilesFull: "files-full",
  ResidentFilesBusy: "storage-busy",
};

const runSentences: Record<string, [string, string]> = {
  busy: ["agentInstances.busy", "agentWork.attempt.busy"],
  "run-limit": [
    "agentInstances.conversationLimit",
    "agentWork.attempt.runLimit",
  ],
  restarting: ["agentInstances.restarting", "agentWork.attempt.restarting"],
  "starting-elsewhere": [
    "agentInstances.startingElsewhere",
    "agentWork.attempt.startingElsewhere",
  ],
  "start-failed": [
    "runProgress.residentStartFailed",
    "agentWork.attempt.startFailed",
  ],
  "files-full": ["agentInstances.fullStart", "agentWork.attempt.full"],
};

const sentences: Record<string, string> = {
  "storage-busy": "agentInstances.filesBusy",
  "ai-assistant-none-free": "agentInstances.assistants.noneFree",
  "not-removable": "agentInstances.unfinishedCompleted",
  "no-running-attempt": "agentWork.stopNothing",
  "file-too-large": "agentInstances.files.tooLarge",
  "file-write-failed": "agentInstances.files.refused",
  "invalid-name": "agentInstances.files.invalidName",
  "reserved-path": "agentInstances.files.reserved",
  "name-collision": "agentInstances.files.nameTaken",
  "file-changed": "agentInstances.files.fileChanged",
  "directory-not-empty": "agentInstances.files.folderNotEmpty",
  "agent-not-active": "agentInstances.files.notActive",
  "not-found": "agentInstances.files.notFound",
  "owned-space": "storageSpaces.agentFilesOf",
  "managers-only": "sandboxNetwork.managersOnly",
  lifecycle: "agentInstances.interruptedByManager",
  "storage-recovery": "agentInstances.interruptedByRecovery",
};

export function refusalSentence(
  refusal: Refusal,
  place: RefusalPlace,
): string | null {
  if (refusal.name === "ResidentInterrupted") {
    return refusal.reason === "lifecycle"
      ? "agentInstances.interruptedByManager"
      : refusal.reason === "storage-recovery"
        ? "agentInstances.interruptedByRecovery"
        : "agentInstances.interrupted";
  }
  const reason =
    refusal.reason ??
    (refusal.name ? own(frameReasons, refusal.name) : undefined);
  if (!reason) {
    return refusal.name === "AgentDenied" ||
      refusal.name === "BackingUnavailable"
      ? "agentInstances.interrupted"
      : null;
  }
  if (reason === "execution-unavailable") {
    const keys: Record<string, string> = {
      "operator-hold": "agentsPaused",
      "host-replaced": "serverMoved",
      "space-unverified": "filesUnchecked",
    };
    const key = refusal.executionReason
      ? own(keys, refusal.executionReason)
      : undefined;
    return key
      ? `agentInstances.unavailable.${key}`
      : "sidebar.agentsUnavailable";
  }
  if (reason === "ai-assistant-limit") {
    return `agentInstances.assistants.${refusal.extra.per_user === 0 ? "notOffered" : "limitReached"}`;
  }
  if (reason === "ai-employee-limit") {
    return `agentInstances.employees.${refusal.extra.allowed === 0 ? "notOffered" : "limitReached"}`;
  }
  if (reason === "work-limit") {
    return `agentWork.workLimit.${refusal.extra.max === 1 ? "one" : "many"}`;
  }
  if (reason === "files-full" && place === "files")
    return "agentInstances.files.full";
  const run = own(runSentences, reason);
  return (
    run?.[place === "work" ? 1 : 0] ??
    own(sentences, reason) ??
    "agentInstances.operationError"
  );
}
