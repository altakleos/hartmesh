import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

export interface StorageSpace {
  id: string;
  backing_handle: string;
  name: string;
  custody: {
    kind: "personal" | "company";
    principal: { kind: "human" | "nonhuman"; subject_id: string } | null;
  };
  mode: "native" | "mediated";
  generation: number;
  status: "active" | "archived";
  permissions: number;
  storage_state?: "available" | "recovery-pending";
  quota?: {
    max_bytes: number;
    max_inodes: number;
    available_bytes: number;
    available_inodes: number;
    editor: string;
  };
}

export interface SpaceRecovery {
  backups: {
    id: string;
    generation: number;
    size_bytes: number;
    consistency: "quiesced-filesystem";
  }[];
  operations: {
    operation_id: string;
    generation: number;
    phase: "pending" | "complete" | "failed";
    request: { action: string };
  }[];
  attachments: { id: string; phase: string }[];
  can_fence: boolean;
}

export function getSpaceRecovery(id: string, signal?: AbortSignal) {
  return request<SpaceRecovery>(`${resourceURL(id)}/recovery`, { signal });
}

export function changeSpaceLifecycle(
  id: string,
  generation: number,
  action: "backup" | "restore" | "archive" | "delete",
  backupId?: string,
  signal?: AbortSignal,
) {
  return request<unknown>(`${resourceURL(id)}/lifecycle`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      action,
      generation,
      operation_id: operationID(),
      ...(backupId ? { backup_id: backupId } : {}),
    }),
    signal,
  });
}

export function acceptSpaceCurrentState(
  id: string,
  generation: number,
  operationId: string,
  signal?: AbortSignal,
) {
  return request<{ complete: boolean }>(`${resourceURL(id)}/recovery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      operation_id: operationId,
      generation,
      acknowledge_uncertain_outcome: true,
    }),
    signal,
  });
}

export function retireSpaceAttachments(
  id: string,
  generation: number,
  attachmentIds: string[],
  signal?: AbortSignal,
) {
  return request<{ complete: boolean }>(
    `${resourceURL(id)}/attachments/retire`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ generation, attachment_ids: attachmentIds }),
      signal,
    },
  );
}

export interface SpaceFile {
  name: string;
  path: string;
  kind: "file" | "directory" | "symlink" | "unsupported";
  size: number;
  modified: number;
  accessible: boolean;
  target_kind?: "file" | "directory" | null;
}

export class StorageRequestError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

function resourceURL(id: string) {
  if (!/^[0-9a-f]{32}$/.test(id))
    throw new Error("Invalid storage resource reference");
  return `${getBackendBaseURL()}/api/spaces/${id}`;
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, { cache: "no-store", ...init });
  const payload: unknown = await response.json();
  if (!response.ok) {
    const detail =
      typeof payload === "object" && payload !== null && "detail" in payload
        ? payload.detail
        : undefined;
    throw new StorageRequestError(
      response.status,
      typeof detail === "string"
        ? detail
        : `Storage request failed (${response.status})`,
    );
  }
  return payload as T;
}

function operationID() {
  return crypto.randomUUID().replaceAll("-", "");
}

export function spaceFileURL(id: string, path: string, download = false) {
  const params = new URLSearchParams({ path });
  if (download) params.set("download", "true");
  return `${resourceURL(id)}/content?${params}`;
}

export function listSpaces(signal?: AbortSignal) {
  return request<{ spaces: StorageSpace[] }>(
    `${getBackendBaseURL()}/api/spaces`,
    { signal },
  );
}

export function getSpace(id: string, signal?: AbortSignal) {
  return request<StorageSpace>(resourceURL(id), { signal });
}

export function createSpace(
  name: string,
  custody: "personal" | "company",
  signal?: AbortSignal,
) {
  return request<StorageSpace>(`${getBackendBaseURL()}/api/spaces`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, custody }),
    signal,
  });
}

export function listSpaceFiles(id: string, path: string, signal?: AbortSignal) {
  return request<{ files: SpaceFile[]; truncated: boolean }>(
    `${resourceURL(id)}/files?${new URLSearchParams({ path })}`,
    { signal },
  );
}

export function readSpaceText(id: string, path: string, signal?: AbortSignal) {
  return request<{ text: string; sha256: string; concurrency: string }>(
    `${resourceURL(id)}/text?${new URLSearchParams({ path })}`,
    { signal },
  );
}

export function writeSpaceFile(
  id: string,
  path: string,
  content: BodyInit,
  options: {
    generation: number;
    expectedSha256?: string;
    create?: boolean;
    signal?: AbortSignal;
  },
) {
  const params = new URLSearchParams({
    path,
    generation: String(options.generation),
    operation_id: operationID(),
  });
  if (options.expectedSha256)
    params.set("expected_sha256", options.expectedSha256);
  if (options.create) params.set("create", "true");
  return request<{ sha256: string }>(`${resourceURL(id)}/content?${params}`, {
    method: "PUT",
    headers: { "Content-Type": "application/octet-stream" },
    body: content,
    signal: options.signal,
  });
}

export function mutateSpaceFile(
  id: string,
  generation: number,
  action: "mkdir" | "rename" | "remove",
  path: string,
  destination?: string,
  signal?: AbortSignal,
) {
  return request<{ complete: boolean }>(`${resourceURL(id)}/files`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      generation,
      operation_id: operationID(),
      action,
      path,
      ...(destination === undefined ? {} : { destination }),
    }),
    signal,
  });
}

export function copySpaceFile(
  destination: StorageSpace,
  source: StorageSpace,
  sourcePath: string,
  destinationPath: string,
  acknowledgeDisclosure: boolean,
  signal?: AbortSignal,
) {
  return request<{ sha256: string }>(`${resourceURL(destination.id)}/import`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      source_id: source.id,
      source_generation: source.generation,
      destination_generation: destination.generation,
      source_path: sourcePath,
      destination_path: destinationPath,
      operation_id: operationID(),
      acknowledge_disclosure: acknowledgeDisclosure,
    }),
    signal,
  });
}
