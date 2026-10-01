import { afterEach, describe, expect, it, rs } from "@rstest/core";

import {
  exportThread,
  filenameFromDisposition,
  ThreadExportEmptyError,
  urlOfThreadExport,
} from "@/core/threads/export";

// The transcript itself is the Gateway's (backend/tests/test_transcript.py);
// this page asks for it and saves what comes back.

describe("urlOfThreadExport", () => {
  it("names the conversation and the format", () => {
    expect(urlOfThreadExport("thread/1", "markdown")).toMatch(
      /\/api\/threads\/thread%2F1\/export\?format=markdown$/,
    );
    expect(urlOfThreadExport("thread-1", "json")).toMatch(
      /\/api\/threads\/thread-1\/export\?format=json$/,
    );
  });
});

describe("filenameFromDisposition", () => {
  it("prefers the UTF-8 name", () => {
    expect(
      filenameFromDisposition(
        "attachment; filename=\"conversation.md\"; filename*=UTF-8''%E6%9C%88%E5%BA%A6%20review.md",
      ),
    ).toBe("月度 review.md");
  });

  it("falls back to the plain name", () => {
    expect(
      filenameFromDisposition('attachment; filename="Monthly review.md"'),
    ).toBe("Monthly review.md");
    expect(
      filenameFromDisposition(
        "attachment; filename=\"Monthly review.md\"; filename*=UTF-8''%E0%A4%A",
      ),
    ).toBe("Monthly review.md");
  });

  it("has nothing to offer without a header", () => {
    expect(filenameFromDisposition(null)).toBeNull();
    expect(filenameFromDisposition("attachment")).toBeNull();
  });
});

describe("exportThread", () => {
  afterEach(() => {
    rs.unstubAllGlobals();
  });

  it("says a conversation with nothing recorded has no transcript", async () => {
    rs.stubGlobal(
      "fetch",
      rs.fn(async () => new Response("", { status: 404 })),
    );
    await expect(exportThread("thread-1", "markdown")).rejects.toBeInstanceOf(
      ThreadExportEmptyError,
    );
  });

  it("fails on any other refusal", async () => {
    rs.stubGlobal(
      "fetch",
      rs.fn(async () => new Response("", { status: 500 })),
    );
    await expect(exportThread("thread-1", "json")).rejects.toThrow(
      "Export failed with status 500",
    );
  });
});
