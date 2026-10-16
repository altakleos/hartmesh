import { expect, test } from "@rstest/core";

import {
  agentRunEnding,
  hasFinalAgentAnswer,
} from "@/core/agent-instances/newest-run";

test.each([
  "busy",
  "run-limit",
  "restarting",
  "starting-elsewhere",
  "start-failed",
  "files-full",
  "storage-busy",
])("recovers the fixed notice for a settled run with %s", (reason) => {
  expect(
    agentRunEnding({
      run_id: "run",
      thread_id: "thread",
      status: "error",
      stop_reason: reason,
    }),
  ).toEqual({ kind: "refusal", refusal: { reason, extra: {} } });
});
test("service shutdown requires interrupted status, while orphan recovery records its own interrupted ending", () => {
  expect(
    agentRunEnding({
      run_id: "run",
      thread_id: "thread",
      status: "interrupted",
      stop_reason: "gateway_stopped",
    }),
  ).toEqual({ kind: "service" });
  expect(
    agentRunEnding({
      run_id: "run",
      thread_id: "thread",
      status: "success",
      stop_reason: "gateway_stopped",
    }),
  ).toBeNull();
  expect(
    agentRunEnding({
      run_id: "run",
      thread_id: "thread",
      status: "error",
      stop_reason: "orphan_recovered",
    }),
  ).toEqual({ kind: "service" });
});
test.each([null, "cancelled", "unknown-private-reason"])(
  "does not invent a notice for %s",
  (stop_reason) => {
    expect(
      agentRunEnding({
        run_id: "run",
        thread_id: "thread",
        status: "interrupted",
        stop_reason,
      }),
    ).toBeNull();
  },
);
test("only a terminal visible assistant text counts as the current final answer", () => {
  expect(
    hasFinalAgentAnswer([
      { type: "human", content: "Question" },
      { type: "ai", content: "Answer" },
    ]),
  ).toBe(true);
  expect(
    hasFinalAgentAnswer([
      { type: "ai", content: "Old answer" },
      { type: "human", content: "New question" },
    ]),
  ).toBe(false);
  expect(
    hasFinalAgentAnswer([
      {
        type: "ai",
        content: "Step",
        tool_calls: [{ id: "tool", name: "bash", args: {} }],
      },
    ]),
  ).toBe(false);
  expect(
    agentRunEnding({
      run_id: "run",
      thread_id: "thread",
      status: "running",
      stop_reason: "start-failed",
    }),
  ).toBeNull();
});
