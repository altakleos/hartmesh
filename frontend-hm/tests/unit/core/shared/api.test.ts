import { beforeEach, describe, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({
  fetch: rs.fn(),
}));

import { fetch as fetcher } from "@/core/api/fetcher";
import {
  canPublishToShared,
  publishToShared,
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
