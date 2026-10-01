import { beforeEach, expect, rs, test } from "@rstest/core";

/**
 * The liveness probe: what it asks for, what it accepts, and what it refuses.
 *
 * The defect this guards is a card offering a download whose file a rebuild
 * deleted. The probe is the second of the card's two questions, so it has to
 * be cheap (a bounded range, body dropped), use the same authenticated route
 * the download link uses, and fail closed on everything that is not proof.
 */

const fetchWithAuth = rs.fn();

rs.mock("@/core/api/fetcher", () => ({
  fetch: fetchWithAuth,
}));

const THREAD = "thread-1";
const PDF = "/mnt/user-data/outputs/reports/august/august.pdf";

function respond(status: number) {
  const cancel = rs.fn(async () => undefined);
  return { response: { status, body: { cancel } }, cancel };
}

beforeEach(() => {
  fetchWithAuth.mockReset();
});

test("asks the authenticated artifact route for a single byte", async () => {
  const { response, cancel } = respond(206);
  fetchWithAuth.mockResolvedValue(response);

  const { probeReportRenderLive } = await import("@/core/business-report");

  await expect(
    probeReportRenderLive({ threadId: THREAD, path: PDF }),
  ).resolves.toBe(true);

  expect(fetchWithAuth).toHaveBeenCalledTimes(1);
  const [url, init] = fetchWithAuth.mock.calls[0] as [string, RequestInit];
  expect(url).toContain(`/api/threads/${THREAD}/artifacts`);
  expect(url).toContain("august.pdf");
  // The probe is not a download: no `download=true`, and one byte asked for.
  expect(url).not.toContain("download=true");
  expect((init.headers as Record<string, string>).Range).toBe("bytes=0-0");
  expect(init.method).toBe("GET");
  // The body is dropped unread, so proving a large PDF exists costs nothing.
  expect(cancel).toHaveBeenCalledTimes(1);
});

test("a 200 counts as live: the range was ignored, the file is still there", async () => {
  const { response, cancel } = respond(200);
  fetchWithAuth.mockResolvedValue(response);

  const { probeReportRenderLive } = await import("@/core/business-report");

  await expect(
    probeReportRenderLive({ threadId: THREAD, path: PDF }),
  ).resolves.toBe(true);
  expect(cancel).toHaveBeenCalledTimes(1);
});

test.each([[400], [403], [404], [500]])(
  "a %d is not a download link",
  async (status) => {
    const { response } = respond(status);
    fetchWithAuth.mockResolvedValue(response);

    const { probeReportRenderLive } = await import("@/core/business-report");

    await expect(
      probeReportRenderLive({ threadId: THREAD, path: PDF }),
    ).resolves.toBe(false);
  },
);

test("a network failure is not a download link", async () => {
  fetchWithAuth.mockRejectedValue(new Error("offline"));

  const { probeReportRenderLive } = await import("@/core/business-report");

  await expect(
    probeReportRenderLive({ threadId: THREAD, path: PDF }),
  ).resolves.toBe(false);
});

test("a body that refuses to cancel does not turn a live file dead", async () => {
  fetchWithAuth.mockResolvedValue({
    status: 206,
    body: {
      cancel: async () => {
        throw new Error("already settled");
      },
    },
  });

  const { probeReportRenderLive } = await import("@/core/business-report");

  await expect(
    probeReportRenderLive({ threadId: THREAD, path: PDF }),
  ).resolves.toBe(true);
});

test("the query identity separates two drafts that share every filename", async () => {
  const { reportRendersQueryKey } = await import("@/core/business-report");

  const shared = {
    threadId: THREAD,
    filepath: "/mnt/user-data/outputs/reports/august/august.report.json",
    kinds: ["pdf", "docx"] as const,
  };
  const draft2 = reportRendersQueryKey({ ...shared, revision: "sha-draft-2" });
  const draft3 = reportRendersQueryKey({ ...shared, revision: "sha-draft-3" });

  expect(draft2).not.toEqual(draft3);
});
