import { UnauthorizedError } from "@/core/api/errors";
import type { Translations } from "@/core/i18n/locales/types";
import {
  StorageRequestError,
  type SpaceRecoveryOperation,
} from "@/core/spaces/api";

import { InstanceApiError, type AgentInstanceDetail } from "./api";

type Copy = Translations["agentInstances"]["files"]["recovery"];
export type FileChange = {
  row: SpaceRecoveryOperation;
  description: string;
  folder: string;
  file: string;
};
export type RecoveryResult = {
  change: FileChange;
  status:
    | "accepted"
    | "settled"
    | "hostUnconfirmed"
    | "active"
    | "absent"
    | "unknown";
  ready: boolean;
};

export function describeRecoveryChange(
  row: SpaceRecoveryOperation,
  copy: Copy,
): FileChange | undefined {
  const { action, path, destination, destination_path: copyPath } = row.request;
  let target: string | undefined;
  let description: string;
  switch (action) {
    case "put":
    case "write":
      if (!path) return;
      description = copy.save(path);
      target = path;
      break;
    case "mkdir":
      if (!path) return;
      description = copy.folder(path);
      target = path;
      break;
    case "rename":
      if (!path || !destination) return;
      description = copy.rename(path, destination);
      target = destination;
      break;
    case "remove":
      if (!path) return;
      description = copy.remove(path);
      target = path;
      break;
    case "copy":
      if (!copyPath) return;
      description = copy.copy(copyPath);
      target = copyPath;
      break;
    default:
      return;
  }
  if (typeof target !== "string") return;
  const slash = target.lastIndexOf("/");
  return {
    row,
    description,
    folder: slash < 0 ? copy.rootFolder : target.slice(0, slash),
    file: target.slice(slash + 1),
  };
}

export function sameChange(
  a: SpaceRecoveryOperation,
  b: SpaceRecoveryOperation,
) {
  return (
    a.operation_id === b.operation_id &&
    a.generation === b.generation &&
    a.created_at === b.created_at &&
    !!a.request &&
    a.request.action === b.request.action &&
    a.request.path === b.request.path &&
    a.request.destination === b.request.destination &&
    a.request.destination_path === b.request.destination_path
  );
}

export function recoveryOutcome(
  read: PromiseSettledResult<SpaceRecoveryOperation>,
  row: SpaceRecoveryOperation,
): RecoveryResult["status"] {
  if (read.status === "rejected")
    return read.reason instanceof StorageRequestError &&
      read.reason.status === 404
      ? "absent"
      : "unknown";
  const op = read.value;
  if (!op || !sameChange(op, row)) return "absent";
  if (op.phase === "pending") return "active";
  if (op.phase !== "failed" && op.phase !== "complete") return "unknown";
  if (
    op.phase === "failed" &&
    op.result?.resolution === "owner-accepted-current-state" &&
    op.result.actor_kind === "human" &&
    op.result.actor_id
  )
    return "accepted";
  if (op.result?.resolution === "host-unconfirmed") return "hostUnconfirmed";
  if (
    ["host-settled", "host-reconciled", "host-discarded"].includes(
      op.result?.resolution ?? "",
    )
  )
    return "settled";
  return "unknown";
}

export function recoveryAccessLost(error: unknown, missingResource = false) {
  return (
    error instanceof UnauthorizedError ||
    ((error instanceof StorageRequestError ||
      error instanceof InstanceApiError) &&
      (error.status === 401 ||
        error.status === 403 ||
        (missingResource && error.status === 404)))
  );
}

export function workspaceReady(detail: AgentInstanceDetail) {
  return (
    detail.home?.state === "available" &&
    (detail.home.workspace === "absent" ||
      detail.home.workspace === "running") &&
    detail.execution?.execution === "ready"
  );
}
