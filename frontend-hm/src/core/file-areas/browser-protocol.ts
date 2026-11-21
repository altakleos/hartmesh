import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";
import { useFeatures, type FeaturesResponse } from "@/core/features";
import { StorageRequestError, type SpaceFile } from "@/core/spaces/api";

import type { BrowserArea } from "./browser";

export function browserOperations(features: FeaturesResponse) {
  const protocol = features.file_browser;
  if (
    !protocol ||
    !Array.isArray(protocol.versions) ||
    !protocol.versions.includes(1) ||
    !["ordinary", "spaces"].includes(String(protocol.storage_mode))
  )
    return undefined;
  if (protocol.storage_mode === "ordinary") {
    return {
      personal: protocol.operations?.personal,
      shared: Array.isArray(protocol.operations?.shared)
        ? protocol.operations.shared.filter(
            (operation) => operation === "select" || operation === "folder",
          )
        : [],
      agent: [],
    };
  }
  return protocol.operations;
}
export function useBrowserProtocol() {
  const result = useFeatures(browserOperations);
  return {
    settled: !result.isPending,
    failed: result.isError,
    supports: (source: BrowserArea, operation: string) =>
      !result.isError &&
      Array.isArray(result.data?.[source]) &&
      result.data[source].includes(operation),
    refresh: result.refetch,
  };
}
export function agentBrowserURL(
  instance: string,
  operation: "folder" | "text" | "content",
  path: string,
  download = false,
) {
  const params = new URLSearchParams({ path });
  if (download) params.set("download", "true");
  return (
    getBackendBaseURL() +
    "/api/file-browser/v1/agents/" +
    encodeURIComponent(instance) +
    "/" +
    operation +
    "?" +
    params.toString()
  );
}
async function read<T>(url: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal, cache: "no-store" });
  if (!response.ok) {
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    const detail =
      typeof payload === "object" && payload && "detail" in payload
        ? payload.detail
        : undefined;
    const reason =
      typeof detail === "object" && detail && "code" in detail
        ? String(detail.code)
        : undefined;
    throw new StorageRequestError(
      response.status,
      typeof detail === "string" ? detail : "File read failed",
      reason,
    );
  }
  return response.json() as Promise<T>;
}
export const readAgentFolder = (
  instance: string,
  path: string,
  signal: AbortSignal,
) =>
  read<{ files: SpaceFile[]; truncated: boolean }>(
    agentBrowserURL(instance, "folder", path),
    signal,
  );
export const readBrowserText = (
  instance: string,
  path: string,
  signal: AbortSignal,
) =>
  read<{ text: string; sha256: string; concurrency: string }>(
    agentBrowserURL(instance, "text", path),
    signal,
  );
