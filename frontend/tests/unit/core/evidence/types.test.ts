import { describe, expect, it } from "@rstest/core";

import { parseEvidenceSummary } from "@/core/evidence/types";

function summary(overrides: Record<string, unknown> = {}) {
  return {
    schema: "hartmesh.run-evidence-summary",
    schema_version: 1,
    overview: {
      run_ref: "run-public-ref",
      thread_ref: "thread-public-ref",
      status: "success",
      accepted_at: "2026-09-04T12:00:00Z",
      updated_at: "2026-09-04T12:01:00Z",
      terminal_reason: null,
      policy: null,
      completeness: "complete",
    },
    timeline: [],
    sections: {},
    qualification: { state: "legacy" },
    ...overrides,
  };
}

const diagnostic = {
  seq: 4,
  at: "2026-09-04T12:00:40Z",
  kind: "session.refused",
  session_kind: "accepted",
  facts: { requester: "gateway:upload", reason: "sandbox_session_conflict" },
  dropped: 0,
};

describe("parseEvidenceSummary sandbox diagnostics", () => {
  it("accepts a summary without the list and one with valid items", () => {
    expect(parseEvidenceSummary(summary()).sandbox_diagnostics).toBeUndefined();
    expect(
      parseEvidenceSummary(summary({ sandbox_diagnostics: [diagnostic] }))
        .sandbox_diagnostics,
    ).toEqual([diagnostic]);
  });

  it("rejects items that are not bounded, namespaced, scalar facts", () => {
    const rejected = [
      { ...diagnostic, kind: "not namespaced" },
      { ...diagnostic, session_kind: "container" },
      { ...diagnostic, facts: { "Bad Key": "x" } },
      { ...diagnostic, facts: { nested: { payload: "x" } } },
      { ...diagnostic, facts: { text: "x".repeat(257) } },
      { ...diagnostic, dropped: -1 },
      { ...diagnostic, at: null },
    ];
    for (const item of rejected) {
      expect(() =>
        parseEvidenceSummary(summary({ sandbox_diagnostics: [item] })),
      ).toThrow("Invalid evidence summary");
    }
    expect(() =>
      parseEvidenceSummary(
        summary({ sandbox_diagnostics: Array(129).fill(diagnostic) }),
      ),
    ).toThrow("Invalid evidence summary");
  });
});
