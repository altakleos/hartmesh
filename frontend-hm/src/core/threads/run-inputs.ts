import type { Message } from "@langchain/langgraph-sdk";
import { z } from "zod";

import { fetch } from "@/core/api/fetcher";
import { getBackendBaseURL } from "@/core/config";
import { getMessageRunId } from "@/core/messages/run-duration";

/**
 * Queue/Steer inputs for an active interactive run.
 *
 * `queue` runs after the current run finishes; `steer` is applied to the
 * current run at its next safe point. The Gateway owns every status: the
 * client only submits, cancels or retries, and reads the recent list back.
 */
export type RunInputMode = "queue" | "steer";

export const runInputSchema = z.object({
  id: z.string(),
  target_run_id: z.string(),
  message_id: z.string(),
  text: z.string(),
  mode: z.enum(["queue", "steer"]),
  status: z.enum([
    "pending",
    "applying",
    "dispatching",
    "applied",
    "not_applied",
    "cancelled",
  ]),
  created_at: z.string(),
  run_id: z.string().optional(),
});
export type RunInput = z.infer<typeof runInputSchema>;

const runInputStateSchema = z.object({
  items: z.array(runInputSchema),
  active_run_id: z.string().nullable(),
});
export type RunInputState = z.infer<typeof runInputStateSchema>;

export const runInputsKey = (threadId: string) =>
  ["threads", threadId, "inputs"] as const;

/** A refused run-input request; `status` tells a rejection from an unknown outcome. */
export class RunInputRequestError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "RunInputRequestError";
  }
}

async function readResponse(response: Response): Promise<unknown> {
  if (!response.ok) {
    const body = (await response.json().catch(() => ({}))) as {
      detail?: unknown;
    };
    throw new RunInputRequestError(
      typeof body.detail === "string" ? body.detail : `HTTP ${response.status}`,
      response.status,
    );
  }
  return response.json() as Promise<unknown>;
}

function threadInputsURL(threadId: string) {
  return `${getBackendBaseURL()}/api/threads/${encodeURIComponent(threadId)}`;
}

export async function fetchRunInputState(
  threadId: string,
  signal?: AbortSignal,
): Promise<RunInputState> {
  const response = await fetch(`${threadInputsURL(threadId)}/inputs`, {
    signal,
  });
  return runInputStateSchema.parse(await readResponse(response));
}

export async function submitRunInput({
  threadId,
  runId,
  requestId,
  text,
  mode,
  context,
  signal,
}: {
  threadId: string;
  runId: string;
  requestId: string;
  text: string;
  mode: RunInputMode;
  context?: Record<string, unknown>;
  signal?: AbortSignal;
}): Promise<RunInput> {
  const response = await fetch(
    `${threadInputsURL(threadId)}/runs/${encodeURIComponent(runId)}/inputs`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        request_id: requestId,
        text,
        mode,
        context: context ?? {},
      }),
      signal,
    },
  );
  return runInputSchema.parse(await readResponse(response));
}

export async function changeRunInput(
  threadId: string,
  input: Pick<RunInput, "id" | "target_run_id">,
  action: "cancel" | "retry",
  signal?: AbortSignal,
): Promise<unknown> {
  const url = `${threadInputsURL(threadId)}/runs/${encodeURIComponent(input.target_run_id)}/inputs/${encodeURIComponent(input.id)}`;
  return readResponse(
    await fetch(action === "retry" ? `${url}/retry` : url, {
      method: action === "retry" ? "POST" : "DELETE",
      signal,
    }),
  );
}

/** Statuses the Gateway can still move without a new request from the person. */
export const LIVE_RUN_INPUT_STATUSES: ReadonlySet<RunInput["status"]> = new Set(
  ["pending", "applying", "dispatching"],
);

/**
 * Show a steer input as applied once the run's own state or transcript says
 * so, without waiting for the next poll of the input list.
 */
export function reconcileRunInputs(
  items: readonly RunInput[],
  appliedRunInputs: { run_id: string; ids: string[] } | null | undefined,
  messages: readonly Message[],
): RunInput[] {
  return items.map((item) => {
    if (
      item.mode !== "steer" ||
      (item.status !== "pending" && item.status !== "applying")
    )
      return item;
    const inState =
      appliedRunInputs?.run_id === item.target_run_id &&
      appliedRunInputs.ids.includes(item.id);
    const inTranscript = messages.some(
      (message) =>
        message.type === "human" &&
        message.additional_kwargs?.run_input_id === item.id &&
        getMessageRunId(message) === item.target_run_id,
    );
    return inState || inTranscript ? { ...item, status: "applied" } : item;
  });
}

/** An input's identity: its ID is unique only within its target run. */
export const runInputKey = (input: Pick<RunInput, "id" | "target_run_id">) =>
  `${input.target_run_id}:${input.id}`;

/**
 * Statuses that may still turn into a run: waiting inputs, and unapplied ones
 * the person can send again.
 */
const UNSETTLED_RUN_INPUT_STATUSES: ReadonlySet<RunInput["status"]> = new Set([
  "pending",
  "applying",
  "dispatching",
  "not_applied",
]);

/** Record the inputs seen unsettled, so their later runs can be followed. */
export function rememberUnsettledRunInputs(
  state: RunInputState | undefined,
  seen: Set<string>,
): void {
  for (const item of state?.items ?? [])
    if (UNSETTLED_RUN_INPUT_STATUSES.has(item.status))
      seen.add(runInputKey(item));
}

/**
 * The run this view should join, or null.
 *
 * - The run the Gateway reports taking inputs, unless this tab already
 *   streams it (its reconnect pointer) or has streamed, finished or stopped it.
 * - Otherwise the run a queued or re-sent input started. Such a run takes
 *   inputs only once its graph starts, so it is followed from the input's own
 *   `run_id`; only inputs this view saw unsettled count, so opening a chat
 *   never replays an old run.
 *
 * Pass only state fetched since the view started: a cached answer from an
 * earlier visit can name a run that is long over.
 */
export function selectRunToFollow({
  state,
  unsettledSeen,
  streamed,
  pointer,
}: {
  state: RunInputState | undefined;
  unsettledSeen: ReadonlySet<string>;
  streamed: ReadonlySet<string>;
  pointer: string | null;
}): string | null {
  if (!state) return null;
  const eligible = (runId: string | null | undefined): runId is string =>
    !!runId && runId !== pointer && !streamed.has(runId);
  if (eligible(state.active_run_id)) return state.active_run_id;
  for (let index = state.items.length - 1; index >= 0; index -= 1) {
    const item = state.items[index]!;
    if (
      item.status === "applied" &&
      item.run_id !== item.target_run_id &&
      unsettledSeen.has(runInputKey(item)) &&
      eligible(item.run_id)
    )
      return item.run_id;
  }
  return null;
}
