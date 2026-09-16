import { afterEach, describe, expect, it, rs } from "@rstest/core";

const gatewayFetch = rs.fn();
rs.mock("@/core/api/fetcher", () => ({
  fetch: (...args: unknown[]) => gatewayFetch(...args),
}));
rs.mock("@/core/config", () => ({
  getBackendBaseURL: () => "https://example.test",
}));

import {
  fetchRunDelivery,
  fetchThreadDeliveryFailures,
} from "@/core/artifact-delivery/api";

const RECORD = {
  available: true,
  version: 1,
  run_id: "run-1",
  message:
    "Artifact delivery incomplete: no produced output artifact was presented",
  undelivered_paths: ["/mnt/user-data/outputs/report.md"],
  undelivered_count: 1,
};

function respond(body: unknown, { ok = true, status = 200 } = {}) {
  gatewayFetch.mockResolvedValue({
    ok,
    status,
    json: async () => body,
  });
}

afterEach(() => {
  gatewayFetch.mockReset();
});

describe("fetchRunDelivery", () => {
  it("reads the verdict for one run", async () => {
    respond(RECORD);

    await expect(
      fetchRunDelivery({ threadId: "t 1", runId: "r/1" }),
    ).resolves.toEqual({
      runId: "run-1",
      message: RECORD.message,
      undeliveredPaths: ["/mnt/user-data/outputs/report.md"],
      undeliveredCount: 1,
    });
    expect(gatewayFetch).toHaveBeenCalledWith(
      "https://example.test/api/threads/t%201/runs/r%2F1/delivery",
    );
  });

  it("reports nothing for a run that delivered what it produced", async () => {
    respond({ available: false, version: 1 });

    await expect(
      fetchRunDelivery({ threadId: "t1", runId: "r1" }),
    ).resolves.toBeNull();
  });

  // hartmesh-tenancy/DF14: `null` is the answer "this run delivered". A
  // request that never reached an answer must not resolve to it, or one 502 on
  // a refetch drops a standing correction into the silence it exists to end.
  it.each([
    ["a gateway error", 502],
    ["a forbidden read", 403],
    ["a pruned run", 404],
  ])("rejects rather than reporting nothing on %s", async (_label, status) => {
    respond({ detail: "nope" }, { ok: false, status });

    await expect(
      fetchRunDelivery({ threadId: "t1", runId: "r1" }),
    ).rejects.toThrow();
  });
});

describe("fetchThreadDeliveryFailures", () => {
  it("names only the runs the fence failed", async () => {
    respond([
      { run_id: "ok-1", stop_reason: null },
      { run_id: "fenced-1", stop_reason: "artifact_delivery_incomplete" },
      { run_id: "capped-1", stop_reason: "loop_capped" },
      { run_id: "fenced-2", stop_reason: "artifact_delivery_incomplete" },
      // The sibling verdict has no withheld files to offer.
      { run_id: "unverified-1", stop_reason: "delivery_receipt_failed" },
    ]);

    await expect(fetchThreadDeliveryFailures("t1")).resolves.toEqual(
      new Set(["fenced-1", "fenced-2"]),
    );
    expect(gatewayFetch).toHaveBeenCalledWith(
      "https://example.test/api/threads/t1/runs",
    );
  });

  it("is empty for a thread where every run delivered", async () => {
    respond([{ run_id: "ok-1", stop_reason: null }]);

    await expect(fetchThreadDeliveryFailures("t1")).resolves.toEqual(new Set());
  });

  it("rejects rather than reporting an all-clear it could not read", async () => {
    respond({ detail: "nope" }, { ok: false, status: 500 });

    await expect(fetchThreadDeliveryFailures("t1")).rejects.toThrow();
  });

  it("rejects a body that is not a run list", async () => {
    respond({ runs: [] });

    await expect(fetchThreadDeliveryFailures("t1")).rejects.toThrow(
      "Invalid run list",
    );
  });
});
