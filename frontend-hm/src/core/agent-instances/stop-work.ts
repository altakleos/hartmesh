import {
  commandWork,
  getWork,
  InstanceApiError,
  operationID,
  type WorkRecord,
  type WorkStopCommand,
} from "./api";

export interface WorkStopResult {
  kind: "stopping" | "nothing" | "failed";
  record: WorkRecord;
  retry?: WorkStopCommand;
}
export async function stopWorkAttempt(
  instance: string,
  record: WorkRecord,
  { signal, retry }: { signal?: AbortSignal; retry?: WorkStopCommand } = {},
): Promise<WorkStopResult> {
  const live = () => {
    if (signal?.aborted) throw new DOMException("Stop retired", "AbortError");
  };
  live();
  const target = retry?.attempt_id ?? record.attempt?.id;
  if (
    !target ||
    (!retry && !["starting", "running"].includes(record.attempt?.status ?? ""))
  )
    return { kind: "nothing", record };
  let body: WorkStopCommand = retry ?? {
    operation_id: operationID(),
    action: "stop",
    expected_revision: record.revision,
    expected_assignment_revision: record.assignment_revision,
    attempt_id: target,
  };
  let current = record;
  for (let resends = 0; resends <= 3; resends++) {
    live();
    try {
      const updated = await commandWork(instance, record.id, body, signal);
      live();
      return { kind: "stopping", record: updated };
    } catch (error) {
      live();
      if (!(error instanceof InstanceApiError) || error.status >= 500)
        return { kind: "failed", record: current, retry: body };
      if (error.status !== 409) return { kind: "failed", record: current };
      try {
        current = await getWork(instance, record.id, signal);
      } catch {
        live();
        return { kind: "failed", record: current, retry: body };
      }
      live();
      if (error.reason === "no-running-attempt")
        return { kind: "nothing", record: current };
      // Never redirect a captured Stop to a replacement attempt.
      if (
        current.attempt?.id !== target ||
        !["starting", "running"].includes(current.attempt.status) ||
        resends === 3
      )
        return { kind: "failed", record: current };
      body = {
        ...body,
        expected_revision: current.revision,
        expected_assignment_revision: current.assignment_revision,
      };
    }
  }
  return { kind: "failed", record: current };
}
