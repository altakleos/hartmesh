import { beforeEach, describe, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({
  fetch: rs.fn(),
}));

rs.mock("@/core/config", () => ({
  getBackendBaseURL: () => "/backend",
}));

import { fetch as fetcher } from "@/core/api/fetcher";
import {
  canKeepInMyFiles,
  deleteMyFile,
  keepInMyFiles,
  listMyFiles,
  MyFilesRequestError,
  urlOfMyFile,
} from "@/core/files/api";

const mockedFetch = rs.mocked(fetcher);

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    statusText: status >= 400 ? "Error" : "OK",
    headers: { "Content-Type": "application/json" },
  });
}

const AUGUST = {
  path: "Reports/august.pdf",
  name: "august.pdf",
  size: 3,
  modified: 1_757_980_800,
  virtual_path: "/mnt/user-data/files/Reports/august.pdf",
  url: "/api/files/Reports/august.pdf",
};

beforeEach(() => {
  mockedFetch.mockReset();
});

describe("my files api", () => {
  it("addresses a file segment by segment", () => {
    expect(urlOfMyFile("Reports/Q3 report #2.pdf")).toBe(
      "/backend/api/files/Reports/Q3%20report%20%232.pdf",
    );
    expect(urlOfMyFile("a.pdf", { download: true })).toBe(
      "/backend/api/files/a.pdf?download=true",
    );
  });

  it("lists the person's files without caching", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse(200, { files: [AUGUST], count: 1, truncated: false }),
    );

    const listed = await listMyFiles();

    expect(listed.files).toEqual([AUGUST]);
    expect(mockedFetch).toHaveBeenCalledWith("/backend/api/files", {
      cache: "no-store",
    });
  });

  it("keeps a conversation's file in a folder", async () => {
    mockedFetch.mockResolvedValueOnce(jsonResponse(201, AUGUST));

    const kept = await keepInMyFiles("thread 1", {
      path: "/mnt/user-data/outputs/august.pdf",
      folder: "Reports",
    });

    expect(kept).toEqual(AUGUST);
    const [url, init] = mockedFetch.mock.calls[0]!;
    expect(url).toBe("/backend/api/threads/thread%201/files");
    expect(init?.method).toBe("POST");
    expect(JSON.parse(init?.body as string)).toEqual({
      path: "/mnt/user-data/outputs/august.pdf",
      folder: "Reports",
    });
  });

  it("deletes one file and surfaces the gateway's reason when it cannot", async () => {
    mockedFetch.mockResolvedValueOnce(new Response(null, { status: 200 }));
    await deleteMyFile("Reports/august.pdf");
    expect(mockedFetch).toHaveBeenCalledWith(
      "/backend/api/files/Reports/august.pdf",
      { method: "DELETE" },
    );

    mockedFetch.mockResolvedValueOnce(
      jsonResponse(404, { detail: "File not found: Reports/august.pdf" }),
    );
    let thrown: unknown;
    try {
      await deleteMyFile("Reports/august.pdf");
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(MyFilesRequestError);
    expect((thrown as MyFilesRequestError).status).toBe(404);
    expect((thrown as Error).message).toBe(
      "File not found: Reports/august.pdf",
    );
  });

  it("keeps what the conversation was given and what it made", () => {
    expect(canKeepInMyFiles("/mnt/user-data/outputs/report.pdf")).toBe(true);
    expect(canKeepInMyFiles("/mnt/user-data/uploads/jobs.xlsx")).toBe(true);
    expect(canKeepInMyFiles("/mnt/user-data/workspace/scratch.py")).toBe(false);
    expect(canKeepInMyFiles("/mnt/user-data/files/august.pdf")).toBe(false);
    expect(canKeepInMyFiles("/mnt/user-data/outputs")).toBe(false);
  });
});
