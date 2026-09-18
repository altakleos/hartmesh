import { describe, expect, it } from "@rstest/core";

import {
  parseArtifactDeliveryFailure,
  parseArtifactDeliveryRecord,
  parseArtifactDeliveryUnverified,
} from "@/core/artifact-delivery";

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

describe("parseArtifactDeliveryUnverified", () => {
  const unverified = {
    type: "artifact_delivery_unverified",
    run_id: "run-1",
    message:
      "Artifact delivery verification failed: terminal delivery receipt could not be persisted",
  };

  it("reads the run and the reason, and carries no paths", () => {
    expect(parseArtifactDeliveryUnverified(unverified)).toEqual({
      runId: "run-1",
      message:
        "Artifact delivery verification failed: terminal delivery receipt could not be persisted",
    });
  });

  it("does not answer for the incomplete verdict", () => {
    expect(parseArtifactDeliveryUnverified(validEvent)).toBeNull();
    expect(parseArtifactDeliveryFailure(unverified)).toBeNull();
  });

  it.each([
    ["no run id", { run_id: "" }],
    ["no message", { message: "" }],
  ])("refuses an event with %s", (_label, override) => {
    expect(
      parseArtifactDeliveryUnverified({ ...unverified, ...override }),
    ).toBeNull();
  });
});

// The durable record carries the same fields as the live
// frame, read by the same code, so a reload cannot change what the notice says.
const validRecord = {
  available: true,
  version: 1,
  run_id: "run-1",
  message:
    "Artifact delivery incomplete: no produced output artifact was presented",
  undelivered_paths: ["/mnt/user-data/outputs/report.md"],
  undelivered_count: 1,
};

describe("parseArtifactDeliveryRecord", () => {
  it("reads the same verdict the stream frame carried", () => {
    expect(parseArtifactDeliveryRecord(validRecord)).toEqual(
      parseArtifactDeliveryFailure(validEvent),
    );
  });

  it.each([
    ["a run that delivered what it produced", { available: false, version: 1 }],
    [
      "a record with no availability claim",
      { ...validRecord, available: undefined },
    ],
    ["a non-object", "available"],
    ["null", null],
  ])("reports nothing for %s", (_label, record) => {
    expect(parseArtifactDeliveryRecord(record)).toBeNull();
  });

  it.each([
    ["no files", { ...validRecord, undelivered_paths: [] }],
    [
      "a count below the disclosed list",
      { ...validRecord, undelivered_count: 0 },
    ],
    ["a non-string path", { ...validRecord, undelivered_paths: [1] }],
    ["no message", { ...validRecord, message: "" }],
    ["no run", { ...validRecord, run_id: "" }],
  ])(
    "refuses a record with %s rather than render a partial notice",
    (_label, record) => {
      expect(parseArtifactDeliveryRecord(record)).toBeNull();
    },
  );
});
