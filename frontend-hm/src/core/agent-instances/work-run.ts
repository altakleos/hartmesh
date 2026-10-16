import { getBackendBaseURL } from "@/core/config";

import { request } from "./api";

export async function readWorkRun(
  threadId: string,
  runId: string,
  signal?: AbortSignal,
): Promise<{ stop_reason: string | null }> {
  const value: unknown = await request(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}/runs/${encodeURIComponent(runId)}`,
    { signal },
  );
  if (
    !value ||
    typeof value !== "object" ||
    Reflect.get(value, "thread_id") !== threadId ||
    Reflect.get(value, "run_id") !== runId
  )
    throw new Error("Run could not be read");
  const reason: unknown = Reflect.get(value, "stop_reason");
  if (reason != null && typeof reason !== "string")
    throw new Error("Run could not be read");
  return { stop_reason: typeof reason === "string" ? reason : null };
}
