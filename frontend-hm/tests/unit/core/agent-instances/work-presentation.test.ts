import { expect, test } from "@rstest/core";

import type { WorkRecord } from "@/core/agent-instances/api";
import {
  workActivationBlock,
  workAttemptLabel,
  workStopCause,
} from "@/core/agent-instances/work-presentation";

function view(
  status: NonNullable<WorkRecord["attempt"]>["status"],
): Pick<WorkRecord, "attempt" | "outcome"> {
  return { attempt: { id: "attempt", status }, outcome: null };
}
test.each([
  ["starting", "starting"],
  ["running", "running"],
  ["stopping", "stopping"],
  ["uncertain", "uncertain"],
  ["cancelled", "cancelled"],
  ["failed", "failed"],
  ["succeeded", "noResult"],
] as const)("attempt %s selects %s", (status, label) =>
  expect(workAttemptLabel(view(status))).toBe(label),
);
test("contained status takes precedence, while only known start refusals say did not start", () => {
  const record = view("failed");
  record.attempt!.contained = { reason: "run-orphaned", at: "now" };
  expect(workAttemptLabel(record, "busy")).toBe("contained");
  record.attempt!.contained.reason = "start-failed";
  expect(workAttemptLabel(record)).toBe("notStarted");
  record.attempt!.contained = null;
  for (const reason of [
    "busy",
    "run-limit",
    "restarting",
    "starting-elsewhere",
    "start-failed",
    "files-full",
    "storage-busy",
  ])
    expect(workAttemptLabel(record, reason)).toBe("notStarted");
  expect(workAttemptLabel(record, "private-unknown")).toBe("failed");
});
test("running workspace startup and preserved completion reports have distinct labels", () => {
  const record = view("running");
  record.attempt!.workspace = "starting";
  expect(workAttemptLabel(record)).toBe("starting");
  record.attempt!.status = "succeeded";
  record.attempt!.candidate = {
    id: "outcome",
    statement: "Done",
    evidence_revision: 1,
    sources: [],
  };
  expect(workAttemptLabel(record)).toBe("succeeded");
  record.attempt!.candidate = null;
  record.outcome = {
    id: "outcome",
    statement: "Done",
    evidence_revision: 1,
    sources: [],
  };
  expect(workAttemptLabel(record)).toBe("succeeded");
});
const events = [
  {
    action: "attempt_settled",
    actor_id: "service",
    created_at: "08:00",
    report: { attempt_id: "attempt" },
  },
  { action: "cancel", actor_id: "later", created_at: "07:59" },
  { action: "edit", actor_id: "first", created_at: "07:58" },
  { action: "activate", actor_id: "starter", created_at: "07:57" },
  { action: "stop", actor_id: "old-attempt", created_at: "07:56" },
];
test("the oldest stop/edit/withdraw between the current activation and settlement supplies the cause", () => {
  expect(workStopCause("attempt", events)).toEqual({
    kind: "edit",
    actor: "first",
    at: "07:58",
  });
  expect(
    workStopCause("attempt", [
      events[0]!,
      { action: "stop", actor_id: "sender", created_at: "07:59" },
      events[3]!,
    ]),
  ).toEqual({ kind: "stop", actor: "sender", at: "07:59" });
});
test("missing or misordered brackets stay plain and a complete bracket with no human request means service stop", () => {
  expect(workStopCause("other-attempt", events)).toEqual({ kind: "plain" });
  expect(workStopCause("attempt", events.slice(0, 3))).toEqual({
    kind: "plain",
  });
  expect(workStopCause("attempt", [events[3]!, events[0]!])).toEqual({
    kind: "plain",
  });
  expect(
    workStopCause("attempt", [events[0]!, events[3]!, events[4]!]),
  ).toEqual({ kind: "service" });
});
test("activation notices use their specified precedence instead of hiding the control", () => {
  const state = {
    pending: true,
    status: "suspended",
    policy: false,
    execution: false,
    busy: true,
    reconcile: true,
    capacity: { max: 1, in_use: 1 },
  };
  expect(workActivationBlock(state)).toBe("pending");
  state.pending = false;
  expect(workActivationBlock(state)).toBe("paused");
  state.status = "archived";
  expect(workActivationBlock(state)).toBe("retired");
  state.status = "active";
  expect(workActivationBlock(state)).toBe("policy");
  state.policy = true;
  expect(workActivationBlock(state)).toBe("execution");
  state.execution = true;
  state.busy = false;
  expect(workActivationBlock(state)).toBe("reconcile");
  state.reconcile = false;
  expect(workActivationBlock(state)).toBe("capacity");
  state.capacity.in_use = 0;
  expect(workActivationBlock(state)).toBeNull();
});
