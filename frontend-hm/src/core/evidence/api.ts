import { throwGatewayApiError } from "@/core/api/errors";
import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";

import { parseEvidenceSummary, type ThreadEvidence } from "./types";

interface RunListItem {
  run_id: string;
  created_at: string;
}

function runUrl(threadId: string, runId?: string): string {
  const base = `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/runs`;
  return runId === undefined ? base : `${base}/${encodeURIComponent(runId)}`;
}

async function json(response: Response, fallback: string): Promise<unknown> {
  if (!response.ok) await throwGatewayApiError(response, fallback);
  return response.json() as Promise<unknown>;
}

export async function fetchThreadEvidence(
  threadId: string,
  signal?: AbortSignal,
): Promise<ThreadEvidence | null> {
  const get = (url: string) => (signal ? fetch(url, { signal }) : fetch(url));
  const runsValue = await json(
    await get(runUrl(threadId)),
    "Failed to load thread runs",
  );
  if (!Array.isArray(runsValue)) throw new Error("Invalid run list");
  const runs = runsValue.filter(
    (item): item is RunListItem =>
      typeof item === "object" &&
      item !== null &&
      typeof (item as RunListItem).run_id === "string" &&
      typeof (item as RunListItem).created_at === "string",
  );
  const latest = [...runs].sort((left, right) =>
    right.created_at.localeCompare(left.created_at),
  )[0];
  if (!latest) return null;
  const value = await json(
    await get(`${runUrl(threadId, latest.run_id)}/evidence`),
    "Failed to load run evidence",
  );
  return { runId: latest.run_id, summary: parseEvidenceSummary(value) };
}

export async function fetchEvidenceBundle(
  threadId: string,
  runId: string,
  signal?: AbortSignal,
): Promise<{ blob: Blob; filename: string }> {
  const response = await fetch(
    `${runUrl(threadId, runId)}/artifacts/evidence-bundle`,
    signal ? { method: "POST", signal } : { method: "POST" },
  );
  if (!response.ok) {
    await throwGatewayApiError(response, "Failed to generate evidence bundle");
  }
  const disposition = response.headers.get("Content-Disposition") ?? "";
  const filename =
    /filename="([^"]+)"/.exec(disposition)?.[1] ?? "run-evidence.zip";
  return { blob: await response.blob(), filename };
}
