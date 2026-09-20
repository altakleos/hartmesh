import { describe, expect, it } from "@rstest/core";

import { canPublishToShared, urlOfSharedFile } from "@/core/shared";

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
