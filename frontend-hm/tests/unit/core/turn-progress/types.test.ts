import { describe, expect, test } from "@rstest/core";

import { parseTurnProgress } from "@/core/turn-progress";

describe("parseTurnProgress", () => {
  test("reads a well-formed frame", () => {
    expect(
      parseTurnProgress({
        type: "turn_progress",
        run_id: "run-1",
        stage: "preparing",
        at_ms: 12,
      }),
    ).toEqual({ runId: "run-1", stage: "preparing", atMs: 12 });
  });

  test.each([
    ["another event", { type: "artifact_delivery_incomplete", run_id: "r" }],
    ["no run", { type: "turn_progress", stage: "thinking", at_ms: 1 }],
    [
      "empty run",
      { type: "turn_progress", run_id: "", stage: "thinking", at_ms: 1 },
    ],
    [
      "a stage this client cannot name",
      { type: "turn_progress", run_id: "r", stage: "rendering", at_ms: 1 },
    ],
    [
      "a missing offset",
      { type: "turn_progress", run_id: "r", stage: "thinking" },
    ],
    [
      "a negative offset",
      { type: "turn_progress", run_id: "r", stage: "thinking", at_ms: -1 },
    ],
    [
      "a string offset",
      { type: "turn_progress", run_id: "r", stage: "thinking", at_ms: "1" },
    ],
    ["not an object", "turn_progress"],
    ["null", null],
  ])("refuses %s", (_label, event) => {
    expect(parseTurnProgress(event)).toBeNull();
  });
});
