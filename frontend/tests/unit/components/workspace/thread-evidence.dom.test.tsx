import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const evidenceState = rs.hoisted(() => ({
  data: undefined as unknown,
  isLoading: false,
  isError: false,
  error: null as Error | null,
  refetch: rs.fn(),
}));
const fetchEvidenceBundle = rs.hoisted(() => rs.fn());
const toast = rs.hoisted(() => ({ success: rs.fn(), error: rs.fn() }));

rs.mock("@/core/evidence", () => ({
  useThreadEvidence: () => evidenceState,
  fetchEvidenceBundle,
}));

rs.mock("sonner", () => ({ toast }));

rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({
    t: {
      common: { loading: "Loading" },
      evidence: {
        label: "Evidence",
        title: "Run evidence",
        description: "Bounded run evidence",
        empty: "No run evidence",
        loadFailed: "Load failed",
        retry: "Retry",
        overview: "Overview",
        timeline: "Policy timeline",
        sections: "Evidence sections",
        status: "Status",
        accepted: "Accepted",
        terminalReason: "Terminal reason",
        policy: "Policy",
        completeness: "Completeness",
        copy: "Copy public reference",
        copied: "Copied",
        downloadBundle: "Download evidence bundle",
        generatingBundle: "Generating",
        cancelBundle: "Cancel bundle generation",
        bundleFailed: "Bundle failed",
        stop: "Stop",
        warning: "Warning",
        noDecisions: "No policy decisions",
        sandboxDiagnostics: "Sandbox diagnostics",
        noDiagnostics: "No sandbox diagnostics",
        sessionKind: {
          ordinary: "Ordinary session",
          accepted: "Accepted session",
        },
        diagnostic: {
          "egress.bound": "Egress allowance bound to the run",
          "session.refused": "Sync refused: an accepted run holds the sandbox",
        },
        state: {
          available: "Available",
          not_applicable: "Not applicable",
          unsupported: "Unsupported",
          legacy: "Legacy",
          pruned: "Pruned",
          unqualified: "Unqualified",
          error: "Error",
        },
        qualification: { unverified: "Unverified" },
        section: {
          policy: "Execution policy",
          artifacts: "Artifacts",
          batches: "Subagent batches",
          retrieval: "Retrieval",
        },
        reason: { repeated_tool_loop: "Repeated equivalent tool loop" },
      },
    },
  }),
}));

import { ThreadEvidence } from "@/components/workspace/thread-evidence";

afterEach(() => {
  cleanup();
  evidenceState.data = undefined;
  evidenceState.isLoading = false;
  evidenceState.isError = false;
  evidenceState.error = null;
  evidenceState.refetch.mockReset();
  fetchEvidenceBundle.mockReset();
  toast.success.mockReset();
  toast.error.mockReset();
});

function summaryFixture(
  overrides: Partial<{
    terminal_reason: string | null;
    timeline: unknown[];
    sandbox_diagnostics: unknown[];
    sections: Record<string, unknown>;
  }> = {},
) {
  return {
    runId: "private-run-id",
    summary: {
      schema: "hartmesh.run-evidence-summary",
      schema_version: 1,
      overview: {
        run_ref: "run-public-ref",
        thread_ref: "thread-public-ref",
        status: "success",
        accepted_at: "2026-09-04T12:00:00Z",
        updated_at: "2026-09-04T12:01:00Z",
        terminal_reason: overrides.terminal_reason ?? null,
        policy: { profile: "interactive", digest: "a".repeat(64) },
        completeness: "complete",
      },
      timeline: overrides.timeline ?? [],
      sandbox_diagnostics: overrides.sandbox_diagnostics,
      sections: overrides.sections ?? {
        artifacts: {
          state: "available",
          data: { file_count: 1, bundle_state: "available" },
        },
      },
      qualification: { state: "unverified" },
    },
  };
}

describe("ThreadEvidence", () => {
  it("renders safe policy reasons, counters, qualification, and native expandable sections", async () => {
    evidenceState.data = {
      runId: "private-run-id",
      summary: {
        schema: "hartmesh.run-evidence-summary",
        schema_version: 1,
        overview: {
          run_ref: "run-public-ref",
          thread_ref: "thread-public-ref",
          status: "success",
          accepted_at: "2026-09-04T12:00:00Z",
          updated_at: "2026-09-04T12:01:00Z",
          terminal_reason: "repeated_tool_loop",
          policy: { profile: "interactive", digest: "a".repeat(64) },
          completeness: "complete",
        },
        timeline: [
          {
            seq: 3,
            at: "2026-09-04T12:00:30Z",
            kind: "policy_decision",
            decision: "stop",
            reason_code: "repeated_tool_loop",
            current: 5,
            limit: 5,
            state_digest: "b".repeat(64),
          },
        ],
        sections: {
          policy: {
            state: "available",
            data: {
              counters: { turns: 4 },
              decision_count: 1,
              egress_profile: "team-egress-v1",
              egress_digest: "c".repeat(64),
              egress_rule_count: 2,
              egress_dns: false,
            },
          },
          artifacts: {
            state: "not_applicable",
            data: { file_count: 0, bundle_state: "available" },
          },
        },
        qualification: { state: "unverified" },
      },
    };

    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));

    expect(
      await screen.findByRole("heading", { name: "Overview" }),
    ).toBeDefined();
    expect(screen.getByText("Repeated equivalent tool loop")).toBeDefined();
    expect(screen.getByLabelText("Qualification: Unverified")).toBeDefined();
    expect(
      screen.getByText("Execution policy").closest("summary"),
    ).toBeDefined();
    expect(screen.getByText("egress profile")).toBeDefined();
    expect(screen.getByText("team-egress-v1")).toBeDefined();
    expect(screen.getByText("egress rule count")).toBeDefined();
    expect(
      screen.getByRole("button", { name: "Download evidence bundle" }),
    ).toBeDefined();
    expect(screen.queryByText("private-run-id")).toBeNull();
  });

  it("renders loading, empty, and retryable error states", async () => {
    evidenceState.isLoading = true;
    const view = render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));
    expect(await screen.findByRole("status")).toBeDefined();
    view.unmount();

    evidenceState.isLoading = false;
    evidenceState.isError = true;
    evidenceState.error = new Error("temporary outage");
    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));
    expect(await screen.findByRole("alert")).toBeDefined();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(evidenceState.refetch).toHaveBeenCalledTimes(1);
  });

  it("lets the operator cancel in-flight bundle generation", async () => {
    evidenceState.data = {
      runId: "private-run-id",
      summary: {
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
        sections: {
          artifacts: {
            state: "available",
            data: { file_count: 1, bundle_state: "available" },
          },
        },
        qualification: { state: "unverified" },
      },
    };
    fetchEvidenceBundle.mockImplementation(
      (_threadId: string, _runId: string, signal: AbortSignal) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () =>
            reject(new DOMException("Aborted", "AbortError")),
          );
        }),
    );

    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));
    fireEvent.click(
      await screen.findByRole("button", { name: "Download evidence bundle" }),
    );
    fireEvent.click(
      await screen.findByRole("button", { name: "Cancel bundle generation" }),
    );

    expect(fetchEvidenceBundle).toHaveBeenCalledTimes(1);
    expect(fetchEvidenceBundle.mock.calls[0]?.[2].aborted).toBe(true);
  });

  it("renders a warning decision distinctly from a stop", async () => {
    evidenceState.data = summaryFixture({
      timeline: [
        {
          seq: 2,
          at: null,
          kind: "policy_decision",
          decision: "warn",
          reason_code: "repeated_tool_loop",
          current: 4,
          limit: 5,
          state_digest: "c".repeat(64),
        },
      ],
    });

    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));

    expect(
      await screen.findByText(/^Warning: Repeated equivalent tool loop/),
    ).toBeDefined();
    expect(screen.queryByText(/^Stop:/)).toBeNull();
    expect(screen.getByText("4 / 5")).toBeDefined();
  });

  it("skips unknown server sections instead of rendering their data", async () => {
    evidenceState.data = summaryFixture({
      sections: {
        policy: { state: "available", data: { decision_count: 0 } },
        internal_diagnostics: {
          state: "available",
          data: { payload: "raw-secret-must-not-render" },
        },
      },
    });

    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));

    expect(await screen.findByText("Execution policy")).toBeDefined();
    expect(screen.queryByText("internal_diagnostics")).toBeNull();
    expect(screen.queryByText("raw-secret-must-not-render")).toBeNull();
  });

  it("renders typed batch and retrieval section data", async () => {
    evidenceState.data = summaryFixture({
      sections: {
        batches: {
          state: "available",
          data: { status: "completed", total_items: 3 },
        },
        retrieval: { state: "pruned", data: { observation_count: 2 } },
      },
    });

    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));

    const batchesSummary = (
      await screen.findByText("Subagent batches")
    ).closest("summary");
    const batchesDetails = batchesSummary?.closest("details");
    expect(batchesDetails?.open).toBe(false);
    fireEvent.click(batchesSummary!);
    expect(batchesDetails?.open).toBe(true);
    expect(screen.getByText("total items")).toBeDefined();
    expect(screen.getByText("completed")).toBeDefined();
    expect(screen.getByText("Retrieval")).toBeDefined();
    expect(screen.getByText("Pruned")).toBeDefined();
    expect(screen.getByText("observation count")).toBeDefined();
    expect(screen.getByText("2")).toBeDefined();
  });

  it("renders sandbox diagnostics with labels, session kind, and facts", async () => {
    evidenceState.data = summaryFixture({
      sandbox_diagnostics: [
        {
          seq: 4,
          at: "2026-09-04T12:00:40Z",
          kind: "session.refused",
          session_kind: "accepted",
          facts: {
            requester: "gateway:upload",
            reason: "sandbox_session_conflict",
          },
          dropped: 0,
        },
        {
          seq: 6,
          at: "2026-09-04T12:00:50Z",
          kind: "scope.opened",
          session_kind: "ordinary",
          facts: { scope_ref: "scope-1" },
          dropped: 2,
        },
        {
          seq: 8,
          at: "2026-09-04T12:00:55Z",
          kind: "egress.bound",
          session_kind: "accepted",
          facts: { profile: "team-egress-v1", rule_count: 2, dns: false },
          dropped: 3,
        },
      ],
    });

    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));

    expect(
      await screen.findByRole("heading", { name: "Sandbox diagnostics" }),
    ).toBeDefined();
    const items = screen.getAllByTestId("evidence-diagnostic");
    expect(items).toHaveLength(3);
    expect(screen.getByText("Egress allowance bound to the run")).toBeDefined();
    expect(
      screen.getByText("Sync refused: an accepted run holds the sandbox"),
    ).toBeDefined();
    expect(screen.getAllByText("Accepted session")).toHaveLength(2);
    expect(screen.getByText("gateway:upload")).toBeDefined();
    expect(screen.getByText("scope.opened")).toBeDefined();
    expect(screen.getByText(/2 dropped/)).toBeDefined();
  });

  it("says when no sandbox diagnostics were recorded", async () => {
    evidenceState.data = summaryFixture();

    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));

    expect(await screen.findByText("No sandbox diagnostics")).toBeDefined();
  });

  it("surfaces a bundle generation failure and recovers the download control", async () => {
    evidenceState.data = summaryFixture();
    fetchEvidenceBundle.mockRejectedValue(new Error("bundle generation busy"));

    render(<ThreadEvidence threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Evidence" }));
    fireEvent.click(
      await screen.findByRole("button", { name: "Download evidence bundle" }),
    );

    expect(
      await screen.findByRole("button", { name: "Download evidence bundle" }),
    ).toBeDefined();
    expect(toast.error).toHaveBeenCalledTimes(1);
    expect(toast.error).toHaveBeenCalledWith("bundle generation busy");
    expect(
      screen.queryByRole("button", { name: "Cancel bundle generation" }),
    ).toBeNull();
  });
});
