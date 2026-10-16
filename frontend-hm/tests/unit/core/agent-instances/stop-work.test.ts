import { beforeEach, expect, test, rs } from "@rstest/core";

rs.mock("@/core/agent-instances/api", () => ({
  ...rs.requireActual<typeof api>("@/core/agent-instances/api"),
  commandWork: rs.fn(),
  getWork: rs.fn(),
}));
import * as api from "@/core/agent-instances/api";
import { stopWorkAttempt } from "@/core/agent-instances/stop-work";

const instance = "a".repeat(32);
function record(): api.WorkRecord {
  return {
    id: "c".repeat(32),
    instance_id: instance,
    creator_id: "reader",
    definition_revision: "revision",
    objective: "Review guide",
    success_criteria: "List gaps",
    priority: "normal",
    review_required: true,
    assignment_revision: 2,
    revision: 5,
    status: "open",
    progress: "",
    next_action: "",
    sources: [],
    review_state: "none",
    outcome: null,
    review: null,
    blocker: null,
    attempt: {
      id: "d".repeat(32),
      status: "running",
      thread_id: "thread",
      run_id: "run",
      requester_id: "reader",
    },
    execution_available: true,
    availability: "explicit_activation",
    work_enabled: true,
    needs_mandate_reconciliation: false,
    current_contents: "not_checked",
    created_at: "2026-10-16T07:00:00Z",
    updated_at: "2026-10-16T07:00:00Z",
  };
}
beforeEach(() => {
  rs.mocked(api.commandWork)
    .mockReset()
    .mockImplementation(async () => {
      const value = record();
      return { ...value, attempt: { ...value.attempt!, status: "stopping" } };
    });
  rs.mocked(api.getWork)
    .mockReset()
    .mockImplementation(async () => ({
      ...record(),
      revision: 6,
      assignment_revision: 3,
    }));
});
test("sends the strict stop intent with the captured assignment and attempt", async () => {
  const value = record();
  const result = await stopWorkAttempt(instance, value);
  expect(result.kind).toBe("stopping");
  const body = rs.mocked(api.commandWork).mock.calls[0]![2];
  expect(body).toEqual({
    operation_id: expect.stringMatching(/^[0-9a-f]{32}$/),
    action: "stop",
    expected_revision: 5,
    expected_assignment_revision: 2,
    attempt_id: value.attempt!.id,
  });
  expect(api.getWork).not.toHaveBeenCalled();
});
test("no-running-attempt refreshes once and never repeats the command", async () => {
  rs.mocked(api.commandWork).mockRejectedValue(
    new api.InstanceApiError(409, "private", "no-running-attempt"),
  );
  rs.mocked(api.getWork).mockResolvedValue({ ...record(), attempt: null });
  const result = await stopWorkAttempt(instance, record());
  expect(result.kind).toBe("nothing");
  expect(result.record.attempt).toBeNull();
  expect(api.commandWork).toHaveBeenCalledTimes(1);
  expect(api.getWork).toHaveBeenCalledTimes(1);
});
test("a revision conflict refreshes the guards while keeping the same operation and attempt", async () => {
  rs.mocked(api.commandWork).mockRejectedValueOnce(
    new api.InstanceApiError(409, "private", "revision-conflict"),
  );
  expect((await stopWorkAttempt(instance, record())).kind).toBe("stopping");
  const first = rs.mocked(api.commandWork).mock.calls[0]![2];
  expect(rs.mocked(api.commandWork).mock.calls[1]![2]).toEqual({
    ...first,
    expected_revision: 6,
    expected_assignment_revision: 3,
  });
});
test("a replacement attempt is never stopped by retrying an older intent", async () => {
  rs.mocked(api.commandWork).mockRejectedValueOnce(
    new api.InstanceApiError(409, "private", "revision-conflict"),
  );
  const value = record();
  rs.mocked(api.getWork).mockResolvedValue({
    ...value,
    attempt: { ...value.attempt!, id: "e".repeat(32) },
  });
  expect((await stopWorkAttempt(instance, value)).kind).toBe("failed");
  expect(api.commandWork).toHaveBeenCalledTimes(1);
});
test("conflicts allow at most three guarded resends", async () => {
  rs.mocked(api.commandWork).mockRejectedValue(
    new api.InstanceApiError(409, "private", "revision-conflict"),
  );
  const result = await stopWorkAttempt(instance, record());
  expect(result.kind).toBe("failed");
  expect(result.retry).toBeUndefined();
  expect(api.commandWork).toHaveBeenCalledTimes(4);
  expect(api.getWork).toHaveBeenCalledTimes(4);
});
test("an unknown outcome keeps the exact command for a manual retry and performs no automatic write retry", async () => {
  rs.mocked(api.commandWork).mockRejectedValueOnce(
    new TypeError("private transport diagnostic"),
  );
  const value = record();
  const first = await stopWorkAttempt(instance, value);
  expect(first.kind).toBe("failed");
  expect(first.retry).toEqual(rs.mocked(api.commandWork).mock.calls[0]![2]);
  expect(api.commandWork).toHaveBeenCalledTimes(1);
  expect(api.getWork).not.toHaveBeenCalled();
  expect(
    (
      await stopWorkAttempt(
        instance,
        { ...value, revision: 99 },
        { retry: first.retry },
      )
    ).kind,
  ).toBe("stopping");
  expect(rs.mocked(api.commandWork).mock.calls[1]![2]).toEqual(first.retry);
});
test("retirement during conflict refresh prohibits a follow-up command", async () => {
  const owner = new AbortController();
  rs.mocked(api.commandWork).mockRejectedValueOnce(
    new api.InstanceApiError(409, "private", "revision-conflict"),
  );
  rs.mocked(api.getWork).mockImplementation(async () => {
    owner.abort();
    return record();
  });
  await expect(
    stopWorkAttempt(instance, record(), { signal: owner.signal }),
  ).rejects.toMatchObject({ name: "AbortError" });
  expect(api.commandWork).toHaveBeenCalledTimes(1);
});
test("a settled assignment offers no stop command", async () => {
  expect(
    (await stopWorkAttempt(instance, { ...record(), attempt: null })).kind,
  ).toBe("nothing");
  expect(api.commandWork).not.toHaveBeenCalled();
});
