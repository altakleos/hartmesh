import { beforeEach, describe, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));

import { fetch } from "@/core/api/fetcher";
import { fetchEvidenceBundle, fetchThreadEvidence } from "@/core/evidence/api";

const mockedFetch = rs.mocked(fetch);

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const summary = {
  schema: "hartmesh.run-evidence-summary",
  schema_version: 1,
  overview: {
    run_ref: "run-public",
    thread_ref: "thread-public",
    status: "success",
    accepted_at: "2026-09-04T12:00:00Z",
    updated_at: "2026-09-04T12:01:00Z",
    terminal_reason: null,
    policy: { profile: "interactive", digest: "a".repeat(64) },
    completeness: "complete",
  },
  timeline: [],
  sections: {
    policy: { state: "available", data: { decision_count: 0 } },
  },
  qualification: { state: "unverified" },
};

beforeEach(() => {
  mockedFetch.mockReset();
});

describe("evidence API", () => {
  it("selects the newest run and validates the versioned summary", async () => {
    mockedFetch
      .mockResolvedValueOnce(
        response([
          { run_id: "older", created_at: "2026-09-04T10:00:00Z" },
          { run_id: "new / run", created_at: "2026-09-04T12:00:00Z" },
        ]),
      )
      .mockResolvedValueOnce(response(summary));

    const result = await fetchThreadEvidence("thread / 1");

    expect(result?.runId).toBe("new / run");
    expect(result?.summary.overview.run_ref).toBe("run-public");
    expect(mockedFetch).toHaveBeenLastCalledWith(
      "/api/threads/thread%20%2F%201/runs/new%20%2F%20run/evidence",
    );
  });

  it("returns no evidence for an empty thread and rejects unknown schemas", async () => {
    mockedFetch.mockResolvedValueOnce(response([]));
    await expect(fetchThreadEvidence("thread-1")).resolves.toBeNull();

    mockedFetch
      .mockResolvedValueOnce(
        response([{ run_id: "run-1", created_at: "2026-09-04" }]),
      )
      .mockResolvedValueOnce(response({ ...summary, schema_version: 2 }));
    await expect(fetchThreadEvidence("thread-1")).rejects.toThrow(
      "Unsupported evidence summary",
    );
  });

  it("rejects unvalidated or unbounded timeline entries", async () => {
    mockedFetch
      .mockResolvedValueOnce(
        response([{ run_id: "run-1", created_at: "2026-09-04" }]),
      )
      .mockResolvedValueOnce(
        response({
          ...summary,
          timeline: [{ kind: "policy_decision", reason_code: "raw value" }],
        }),
      );

    await expect(fetchThreadEvidence("thread-1")).rejects.toThrow(
      "Invalid evidence summary",
    );
  });

  it("accepts unknown future sections for forward compatibility", async () => {
    mockedFetch
      .mockResolvedValueOnce(
        response([{ run_id: "run-1", created_at: "2026-09-04" }]),
      )
      .mockResolvedValueOnce(
        response({
          ...summary,
          sections: {
            ...summary.sections,
            future_section: { state: "available", data: { count: 1 } },
          },
        }),
      );

    const result = await fetchThreadEvidence("thread-1");

    expect(result?.summary.sections.future_section?.state).toBe("available");
  });

  it("maps a forbidden response to the safe fallback message", async () => {
    mockedFetch
      .mockResolvedValueOnce(
        response([{ run_id: "run-1", created_at: "2026-09-04" }]),
      )
      .mockResolvedValueOnce(
        response({ detail: { internal: "stack-trace-must-not-leak" } }, 403),
      );

    await expect(fetchThreadEvidence("thread-1")).rejects.toThrow(
      "Failed to load run evidence",
    );
  });

  it("posts bundle generation without exposing it to chat state", async () => {
    mockedFetch.mockResolvedValueOnce(
      new Response(new Uint8Array([1, 2]), {
        headers: {
          "Content-Disposition": 'attachment; filename="run-evidence.zip"',
        },
      }),
    );
    const result = await fetchEvidenceBundle("thread-1", "run-1");
    expect(result.filename).toBe("run-evidence.zip");
    expect(result.blob.size).toBe(2);
    expect(mockedFetch).toHaveBeenCalledWith(
      "/api/threads/thread-1/runs/run-1/artifacts/evidence-bundle",
      { method: "POST" },
    );
  });
});
