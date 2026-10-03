import { afterEach, describe, expect, it, rs } from "@rstest/core";

import {
  ARTIFACT_PREVIEW_MAX_BYTES,
  loadArtifactContent,
} from "@/core/artifacts/loader";
import { parseBusinessReport } from "@/core/business-report";

import reportFixture from "../../../fixtures/business-report/2026-08-business-review.report.json";

describe("loadArtifactContent", () => {
  it("refetches canonical source when CORS hides the projection marker", async () => {
    const fetchMock = rs
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        new Response('{"only":"card"}', {
          headers: { ETag: `"${"a".repeat(64)}"` },
        }),
      )
      .mockResolvedValueOnce(new Response('{"raw_rows":[1]}'));
    const loaded = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/month.report.json",
      threadId: "thread-1",
      reportPreview: true,
    });
    expect(loaded.projected).toBe(false);
    expect(loaded.content).toBe('{"raw_rows":[1]}');
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("loads a card projection with the canonical revision and fetches full bytes only on demand", async () => {
    const source = JSON.stringify({
      ...reportFixture,
      raw_rows: [{ private: "source rows" }],
    });
    const projection = JSON.stringify({
      ...Object.fromEntries(
        ["version", "kpis", "sections", "charts", "checks", "notes"].map(
          (key) => [key, (reportFixture as Record<string, unknown>)[key]],
        ),
      ),
      meta: Object.fromEntries(
        [
          "title",
          "period",
          "draft",
          "brand",
          "company",
          "currency",
          "inputs",
        ].map((key) => [
          key,
          (reportFixture.meta as Record<string, unknown>)[key],
        ]),
      ),
    });
    const requests: string[] = [];
    rs.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      requests.push(url);
      return new Response(
        url.includes("report_preview=true") ? projection : source,
        {
          headers: url.includes("report_preview=true")
            ? {
                ETag: `"${"a".repeat(64)}"`,
                "X-Artifact-Projection": "business-report-v1",
                "X-Artifact-Source-Bytes": String(source.length),
              }
            : {},
        },
      );
    });
    const args = {
      filepath: "/mnt/user-data/outputs/month.report.json",
      threadId: "thread-1",
      reportPreview: true,
    };
    const preview = await loadArtifactContent(args);
    expect(preview.projected).toBe(true);
    expect(preview.sha256).toBe("a".repeat(64));
    expect(preview.totalBytes).toBe(source.length);
    expect(parseBusinessReport(preview.content)).toEqual(
      parseBusinessReport(source),
    );
    expect(preview.content).not.toContain("source rows");
    expect(requests).toHaveLength(1);
    const full = await loadArtifactContent({ ...args, full: true });
    expect(full.content).toBe(source);
    expect(full.projected).toBe(false);
    expect(requests[1]).not.toContain("report_preview");
  });

  it.each([413, 415, 422, 501])(
    "retains a bounded source preview when projection returns %s",
    async (status) => {
      const fetchMock = rs
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(new Response("unavailable", { status }))
        .mockResolvedValueOnce(new Response("{malformed source"));
      const loaded = await loadArtifactContent({
        filepath: "/mnt/user-data/outputs/month.report.json",
        threadId: "thread-1",
        reportPreview: true,
      });
      expect(loaded.content).toBe("{malformed source");
      expect(loaded.projected).toBe(false);
      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(
        new Headers(fetchMock.mock.calls[1]?.[1]?.headers).get("Range"),
      ).toBe(`bytes=0-${ARTIFACT_PREVIEW_MAX_BYTES - 1}`);
    },
  );

  it("does not fall back around a denied projection", async () => {
    const fetchMock = rs
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("Forbidden", { status: 403 }));
    await expect(
      loadArtifactContent({
        filepath: "/mnt/user-data/outputs/month.report.json",
        threadId: "thread-1",
        reportPreview: true,
      }),
    ).rejects.toThrow("403");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  afterEach(() => {
    rs.restoreAllMocks();
    rs.unstubAllGlobals();
  });

  it("uses the server content revision when available", async () => {
    rs.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("content", {
        status: 200,
        headers: { ETag: `"${"a".repeat(64)}"` },
      }),
    );

    const loaded = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/report.md",
      threadId: "thread-1",
    });

    expect(loaded.sha256).toBe("a".repeat(64));
  });

  it("computes a revision for a complete response without a SHA-256 ETag", async () => {
    rs.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("content", {
        status: 200,
        headers: { ETag: '"starlette-file-etag"' },
      }),
    );

    const loaded = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/page.html",
      threadId: "thread-1",
    });

    expect(loaded.sha256).toBe(
      "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73",
    );
  });

  it("requests only the preview byte budget and reports truncation", async () => {
    const bytes = new TextEncoder().encode("preview");
    const fetchMock = rs.fn(async (_url: string, init?: RequestInit) => {
      expect(new Headers(init?.headers).get("Range")).toBe(
        `bytes=0-${ARTIFACT_PREVIEW_MAX_BYTES - 1}`,
      );
      expect(init?.credentials).toBe("include");
      return new Response(bytes, {
        status: 206,
        headers: {
          "Content-Range": `bytes 0-${bytes.length - 1}/2000000`,
        },
      });
    });
    rs.stubGlobal("fetch", fetchMock);

    const result = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/large.txt",
      threadId: "thread-1",
    });

    expect(result.content).toBe("preview");
    expect(result.truncated).toBe(true);
    expect(result.totalBytes).toBe(2_000_000);
    expect(result.sha256).toBeUndefined();
  });

  it("requests the caller's preview budget when one is given", async () => {
    const fetchMock = rs.fn(async (_url: string, init?: RequestInit) => {
      expect(new Headers(init?.headers).get("Range")).toBe("bytes=0-4095");
      return new Response("{}", { status: 200 });
    });
    rs.stubGlobal("fetch", fetchMock);

    const result = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/august.report.json",
      threadId: "thread-1",
      previewMaxBytes: 4096,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(result.truncated).toBe(false);
  });

  it("loads and revisions the full file only when explicitly requested", async () => {
    const fetchMock = rs.fn(async (_url: string, init?: RequestInit) => {
      expect(new Headers(init?.headers).has("Range")).toBe(false);
      return new Response("complete", {
        status: 200,
        headers: { "Content-Length": "8" },
      });
    });
    rs.stubGlobal("fetch", fetchMock);

    const result = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/large.txt",
      threadId: "thread-1",
      full: true,
    });

    expect(result).toMatchObject({
      content: "complete",
      truncated: false,
      totalBytes: 8,
      sha256:
        "eebbf6457e46a7f63acdf9b97390f790ba443d60cfa44b607da7e5c40aa1cc1d",
    });
  });

  it("does not render a replacement character for a split UTF-8 code point", async () => {
    const emojiBytes = new TextEncoder().encode("abc😀");
    const partial = emojiBytes.slice(0, -2);
    rs.stubGlobal(
      "fetch",
      rs.fn(
        async () =>
          new Response(partial, {
            status: 206,
            headers: {
              "Content-Range": `bytes 0-${partial.length - 1}/${emojiBytes.length + 10}`,
            },
          }),
      ),
    );

    const result = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/unicode.txt",
      threadId: "thread-1",
    });

    expect(result.content).toBe("abc");
  });

  it("treats an unsatisfied range on an empty file as empty content", async () => {
    rs.stubGlobal(
      "fetch",
      rs.fn(
        async () =>
          new Response(null, {
            status: 416,
            headers: { "Content-Range": "bytes */0" },
          }),
      ),
    );

    const result = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/empty.txt",
      threadId: "thread-1",
    });

    expect(result).toMatchObject({
      content: "",
      truncated: false,
      totalBytes: 0,
      sha256:
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    });
  });

  it("parses a weak (W/) SHA-256 ETag returned by a gzipped response", async () => {
    rs.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("content", {
        status: 200,
        headers: { ETag: `W/"${"b".repeat(64)}"` },
      }),
    );

    const loaded = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/report.md",
      threadId: "thread-1",
    });

    expect(loaded.sha256).toBe("b".repeat(64));
  });

  it("resolves without throwing when crypto.subtle is unavailable (non-secure context)", async () => {
    rs.stubGlobal("crypto", { subtle: undefined } as unknown as Crypto);

    const bytes = new TextEncoder().encode("complete");
    rs.stubGlobal(
      "fetch",
      rs.fn(async (_url: string, init?: RequestInit) => {
        expect(new Headers(init?.headers).has("Range")).toBe(false);
        return new Response(bytes, {
          status: 200,
          headers: { "Content-Length": String(bytes.length) },
        });
      }),
    );

    const loaded = await loadArtifactContent({
      filepath: "/mnt/user-data/outputs/non-secure.html",
      threadId: "thread-1",
      full: true,
    });

    // FNV-1a fallback keeps preview working and returns a string; because it
    // is not a 64-hex digest the UI treats it as non-editable (no 422 on save).
    expect(typeof loaded.sha256).toBe("string");
    expect(loaded.sha256).toHaveLength(8);
  });
});
