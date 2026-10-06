import { afterEach, describe, expect, it, rs } from "@rstest/core";

import { probeArtifactExport } from "@/core/artifact-views/exports";

const args = () => ({
  path: "/mnt/user-data/outputs/results/a.pdf",
  threadId: "thread-1",
  signal: new AbortController().signal,
});

describe("live explicit export availability", () => {
  afterEach(() => {
    rs.restoreAllMocks();
    rs.useRealTimers();
  });

  it.each([200, 206])(
    "uses a one-byte authenticated read and cancels the body at %s",
    async (status) => {
      const cancel = rs.fn();
      const fetch = rs
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(
          new Response(new ReadableStream({ cancel }), { status }),
        );
      expect(await probeArtifactExport(args())).toBe("present");
      expect(new Headers(fetch.mock.calls[0]?.[1]?.headers).get("Range")).toBe(
        "bytes=0-0",
      );
      expect(fetch.mock.calls[0]?.[1]?.credentials).toBe("include");
      expect(cancel).toHaveBeenCalledTimes(1);
    },
  );

  it.each([400, 403, 404, 410, 415, 416, 422])(
    "does not offer a refused/missing file at %s",
    async (status) => {
      rs.spyOn(globalThis, "fetch").mockResolvedValue(
        new Response(null, { status }),
      );
      expect(await probeArtifactExport(args())).toBe("absent");
    },
  );

  it.each([429, 500, 503])(
    "preserves retryable uncertainty at %s",
    async (status) => {
      rs.spyOn(globalThis, "fetch").mockResolvedValue(
        new Response(null, { status }),
      );
      expect(await probeArtifactExport(args())).toBe("error");
    },
  );

  it("turns a stalled metadata request into retryable uncertainty at its deadline", async () => {
    rs.useFakeTimers();
    let aborted = false;
    rs.spyOn(globalThis, "fetch").mockImplementation(
      (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init!.signal!.addEventListener(
            "abort",
            () => {
              aborted = true;
              reject(new DOMException("Timed out", "AbortError"));
            },
            { once: true },
          );
        }),
    );
    const pending = probeArtifactExport(args());
    await rs.advanceTimersByTimeAsync(10_000);
    expect(aborted).toBe(true);
    await expect(pending).resolves.toBe("error");
  });

  it("keeps viewer retirement terminal rather than reporting a missing file", async () => {
    const controller = new AbortController();
    controller.abort();
    const fetch = rs.spyOn(globalThis, "fetch");
    await expect(
      probeArtifactExport({ ...args(), signal: controller.signal }),
    ).rejects.toThrow();
    expect(fetch).not.toHaveBeenCalled();
  });
});
