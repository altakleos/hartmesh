import { describe, expect, it } from "@rstest/core";

import { parseArtifactDeliveryFailure } from "@/core/artifact-delivery";

const validEvent = {
  type: "artifact_delivery_incomplete",
  run_id: "run-1",
  message:
    "Artifact delivery incomplete: no produced output artifact was presented",
  undelivered_paths: ["/mnt/user-data/outputs/report.md"],
  undelivered_count: 1,
};

describe("parseArtifactDeliveryFailure", () => {
  it("reads the run, the reason, and the files left behind", () => {
    expect(parseArtifactDeliveryFailure(validEvent)).toEqual({
      runId: "run-1",
      message:
        "Artifact delivery incomplete: no produced output artifact was presented",
      undeliveredPaths: ["/mnt/user-data/outputs/report.md"],
      undeliveredCount: 1,
    });
  });

  it("keeps the exact total when the backend bounded the list", () => {
    const failure = parseArtifactDeliveryFailure({
      ...validEvent,
      undelivered_count: 34,
    });
    expect(failure?.undeliveredCount).toBe(34);
    expect(failure?.undeliveredPaths).toHaveLength(1);
  });

  it.each([
    ["another custom event", { type: "llm_retry", message: "retrying" }],
    ["a non-object", "artifact_delivery_incomplete"],
    ["null", null],
  ])("ignores %s", (_label, event) => {
    expect(parseArtifactDeliveryFailure(event)).toBeNull();
  });

  it.each([
    ["no run id", { run_id: "" }],
    ["no message", { message: "" }],
    ["no paths", { undelivered_paths: [] }],
    ["a non-string path", { undelivered_paths: [42] }],
    ["a fractional count", { undelivered_count: 1.5 }],
    ["a count below the disclosed paths", { undelivered_count: 0 }],
  ])("refuses an event with %s rather than guess", (_label, override) => {
    expect(
      parseArtifactDeliveryFailure({ ...validEvent, ...override }),
    ).toBeNull();
  });
});
