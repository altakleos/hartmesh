import { beforeEach, describe, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({
  fetch: rs.fn(),
}));

import { fetch as fetcher } from "@/core/api/fetcher";
import {
  canPublishToShared,
  publishToShared,
  sharedFolderFor,
  SharedRequestError,
  urlOfSharedFile,
} from "@/core/shared";

const mockedFetch = rs.mocked(fetcher);

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    statusText: status >= 400 ? "Error" : "OK",
    headers: { "Content-Type": "application/json" },
  });
}

const entry = {
  path: "Reports/august.pdf",
  name: "august.pdf",
  size: 1,
  modified: 0,
  virtual_path: "/mnt/user-data/shared/Reports/august.pdf",
  url: "/api/shared/Reports/august.pdf",
  published_by: "alex@example.com",
  published_at: "2026-09-19T10:00:00+00:00",
  from_thread_id: null,
  can_remove: true,
};

describe("publishToShared", () => {
  beforeEach(() => {
    mockedFetch.mockReset();
  });

  it("reports a fresh publication as newly shared", async () => {
    mockedFetch.mockResolvedValueOnce(jsonResponse(201, entry));
    await expect(
      publishToShared({ path: "/mnt/user-data/files/august.pdf" }),
    ).resolves.toEqual({ file: entry, alreadyShared: false });
  });

  it("reports the entry that already held the same bytes as already shared", async () => {
    // The server's word, not the page's memory: `200` says nothing was copied.
    mockedFetch.mockResolvedValueOnce(jsonResponse(200, entry));
    await expect(
      publishToShared({ path: "/mnt/user-data/files/august.pdf" }),
    ).resolves.toEqual({ file: entry, alreadyShared: true });
  });

  it("raises the server's reason when publishing is refused", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse(404, { detail: "Thread not found" }),
    );
    await expect(
      publishToShared({
        path: "/mnt/user-data/outputs/a.pdf",
        thread_id: "t",
      }),
    ).rejects.toBeInstanceOf(SharedRequestError);
  });
});

describe("urlOfSharedFile", () => {
  it("addresses a file whose name would otherwise cut the URL short", () => {
    // `#` starts a fragment and `?` a query: unencoded, the browser would ask
    // for a different file, or none.
    expect(urlOfSharedFile("Reports/Q3 report #2.pdf")).toBe(
      "/api/shared/Reports/Q3%20report%20%232.pdf",
    );
    expect(urlOfSharedFile("Exports/sales?final.csv")).toBe(
      "/api/shared/Exports/sales%3Ffinal.csv",
    );
  });

  it("keeps the folder separators, so the path still names the folder", () => {
    expect(urlOfSharedFile("a/b/c.txt")).toBe("/api/shared/a/b/c.txt");
  });

  it("asks for a download when told to", () => {
    expect(urlOfSharedFile("a.txt", { download: true })).toBe(
      "/api/shared/a.txt?download=true",
    );
  });
});

describe("canPublishToShared", () => {
  it("offers the person's own files and what a conversation was given or made", () => {
    expect(canPublishToShared("/mnt/user-data/files/august.pdf")).toBe(true);
    expect(canPublishToShared("/mnt/user-data/outputs/report.pdf")).toBe(true);
    expect(canPublishToShared("/mnt/user-data/uploads/sales.csv")).toBe(true);
  });

  it("refuses scratch, and what is already everyone's", () => {
    expect(canPublishToShared("/mnt/user-data/workspace/notes.md")).toBe(false);
    expect(canPublishToShared("/mnt/user-data/shared/august.pdf")).toBe(false);
    expect(canPublishToShared("/etc/passwd")).toBe(false);
  });
});

describe("sharedFolderFor", () => {
  const report =
    "/mnt/user-data/outputs/reports/2026-08-business-review/2026-08-business-review.report.json";
  const render =
    "/mnt/user-data/outputs/reports/2026-08-business-review/2026-08-business-review.pdf";

  it("files a report's render with the reports, wherever it was shared from", () => {
    // The card, the artifact panel and a row of My files all ask this, so the
    // same render lands in one place however the person reached it.
    expect(sharedFolderFor(render, { artifacts: [report, render] })).toBe(
      "Reports",
    );
    expect(sharedFolderFor(render, { artifacts: [], report })).toBe("Reports");
  });

  it("leaves an ordinary conversation file at the root", () => {
    expect(
      sharedFolderFor("/mnt/user-data/outputs/notes.txt", { artifacts: [] }),
    ).toBeUndefined();
    expect(
      sharedFolderFor("/mnt/user-data/uploads/jobs.csv", { artifacts: [] }),
    ).toBeUndefined();
  });

  it("carries a report the person keeps in their own Reports folder", () => {
    // Where this product files a report for them, in their own files and in
    // Shared, is the same folder, so keeping one and then sharing it lands
    // beside the copy the report card would have shared.
    expect(
      sharedFolderFor("/mnt/user-data/files/Reports/august.pdf", {
        artifacts: [],
      }),
    ).toBe("Reports");
  });

  it("never carries the rest of how a person keeps their files", () => {
    // A folder named for a customer or a deal is not the company's business,
    // and a folder each person names differently would put the same bytes in
    // Shared twice, which is what this rule exists to prevent.
    for (const own of [
      "/mnt/user-data/files/Acme-acquisition/terms.pdf",
      "/mnt/user-data/files/Invoices/2026/aug.pdf",
      "/mnt/user-data/files/Reports/2026/august.pdf",
      "/mnt/user-data/files/august.pdf",
      "/mnt/user-data/files/ReportsOfMine/august.pdf",
    ]) {
      expect(sharedFolderFor(own, { artifacts: [] })).toBeUndefined();
    }
  });
});
