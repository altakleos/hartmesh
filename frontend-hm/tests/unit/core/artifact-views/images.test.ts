import { afterEach, describe, expect, it, rs } from "@rstest/core";

import {
  ArtifactImageSession,
  inspectRasterImage,
} from "@/core/artifact-views/images";

// A complete synthetic 1x1 RGBA PNG, including its CRCs and closing chunk.
const PNG = Uint8Array.from(
  Buffer.from(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==",
    "base64",
  ),
);

describe("passive view image resources", () => {
  afterEach(() => {
    rs.restoreAllMocks();
    rs.unstubAllGlobals();
    rs.useRealTimers();
  });

  const args = () => ({
    filepath: "/mnt/user-data/outputs/chart.png",
    threadId: "thread-1",
    revision: "a".repeat(64),
    signal: new AbortController().signal,
  });

  it("uses authenticated bounded reads and owns decoded raster URLs until release", async () => {
    const fetch = rs
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () => new Response(PNG));
    const close = rs.fn();
    const decode = rs.fn().mockResolvedValue({ width: 1, height: 1, close });
    rs.stubGlobal("createImageBitmap", decode);
    const create = rs
      .spyOn(URL, "createObjectURL")
      .mockReturnValue("blob:raster");
    const revoke = rs
      .spyOn(URL, "revokeObjectURL")
      .mockImplementation(() => undefined);
    const session = new ArtifactImageSession();
    const url = await session.load(args());
    expect(fetch.mock.calls[0]?.[1]?.credentials).toBe("include");
    expect(new Headers(fetch.mock.calls[0]?.[1]?.headers).get("Range")).toBe(
      "bytes=0-2097151",
    );
    expect((decode.mock.calls[0]?.[0] as Blob).type).toBe("image/png");
    expect(close).toHaveBeenCalledTimes(1);
    expect(create).toHaveBeenCalledTimes(1);
    session.releaseURL(url);
    session.releaseURL(url);
    session.dispose();
    expect(revoke).toHaveBeenCalledExactlyOnceWith(url);
  });

  it.each(["rejected", "dimensions"])(
    "does not create a URL for a %s native decode",
    async (failure) => {
      rs.spyOn(globalThis, "fetch").mockResolvedValue(new Response(PNG));
      const close = rs.fn();
      const decode = rs.fn();
      if (failure === "rejected")
        decode.mockRejectedValue(new Error("Bad pixels"));
      else decode.mockResolvedValue({ width: 2, height: 1, close });
      rs.stubGlobal("createImageBitmap", decode);
      const create = rs.spyOn(URL, "createObjectURL");
      const session = new ArtifactImageSession();
      await expect(session.load(args())).rejects.toThrow();
      expect(create).not.toHaveBeenCalled();
      expect(close).toHaveBeenCalledTimes(failure === "dimensions" ? 1 : 0);
      // Failed decoding releases the lease, leaving the whole document budget.
      for (let index = 0; index < 4; index += 1)
        session.reserve(2 * 1024 * 1024, 4_000_000);
      session.dispose();
    },
  );

  it("settles stalled and queued images at the deadline while retaining outstanding decode ownership", async () => {
    rs.useFakeTimers();
    const fetch = rs
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () => new Response(PNG));
    const finish: ((bitmap: {
      width: number;
      height: number;
      close: () => void;
    }) => void)[] = [];
    const decode = rs
      .fn()
      .mockImplementation(() => new Promise((resolve) => finish.push(resolve)));
    rs.stubGlobal("createImageBitmap", decode);
    const create = rs.spyOn(URL, "createObjectURL");
    const close = rs.fn();
    const session = new ArtifactImageSession();
    const outcomes: string[] = [];
    const pending = Array.from({ length: 3 }, () =>
      session.load(args()).then(
        () => outcomes.push("loaded"),
        () => outcomes.push("failed"),
      ),
    );
    try {
      await rs.advanceTimersByTimeAsync(0);
      expect(decode).toHaveBeenCalledTimes(2);
      await rs.advanceTimersByTimeAsync(20_000);
      expect(outcomes).toEqual(["failed", "failed", "failed"]);
      expect(fetch).toHaveBeenCalledTimes(2);
      const leases = Array.from({ length: 3 }, () =>
        session.reserve(1, 4_000_000),
      );
      leases.push(session.reserve(1, 3_999_998));
      expect(() => session.reserve(1, 1)).toThrow();
      leases.forEach((release) => release());
      // A new live request still cannot start a third physical decoder.
      const next = session.load(args()).catch(() => undefined);
      await rs.advanceTimersByTimeAsync(20_000);
      await next;
      expect(decode).toHaveBeenCalledTimes(2);
    } finally {
      finish.forEach((resolve) => resolve({ width: 1, height: 1, close }));
      await rs.advanceTimersByTimeAsync(0);
      session.dispose();
    }
    await Promise.all(pending);
    expect(close).toHaveBeenCalledTimes(2);
    expect(create).not.toHaveBeenCalled();
  });

  it("settles viewer retirement promptly and closes a late native bitmap", async () => {
    rs.useFakeTimers();
    rs.spyOn(globalThis, "fetch").mockResolvedValue(new Response(PNG));
    let finish:
      | ((bitmap: { width: number; height: number; close: () => void }) => void)
      | undefined;
    rs.stubGlobal(
      "createImageBitmap",
      rs.fn().mockImplementation(
        () =>
          new Promise((resolve) => {
            finish = resolve;
          }),
      ),
    );
    const close = rs.fn();
    const create = rs.spyOn(URL, "createObjectURL");
    const session = new ArtifactImageSession();
    let retired = false;
    const pending = session.load(args()).catch(() => {
      retired = true;
    });
    await rs.advanceTimersByTimeAsync(0);
    session.dispose();
    await rs.advanceTimersByTimeAsync(0);
    try {
      expect(retired).toBe(true);
    } finally {
      finish!({ width: 1, height: 1, close });
      await rs.advanceTimersByTimeAsync(0);
    }
    await pending;
    expect(close).toHaveBeenCalledTimes(1);
    expect(create).not.toHaveBeenCalled();
  });
  it("recognizes raster content independently of filename and declared MIME", () => {
    expect(inspectRasterImage(PNG)).toEqual({
      mime: "image/png",
      width: 1,
      height: 1,
      pixels: 1,
    });
    expect(() =>
      inspectRasterImage(
        new TextEncoder().encode('<svg onload="alert(1)"></svg>'),
      ),
    ).toThrow();
    expect(() =>
      inspectRasterImage(new TextEncoder().encode("<html>image</html>")),
    ).toThrow();
  });

  it("refuses truncated raster data, trailing payloads and oversized dimensions", () => {
    expect(() => inspectRasterImage(PNG.slice(0, -1))).toThrow();
    expect(() => inspectRasterImage(new Uint8Array([...PNG, 1]))).toThrow();
    const enormous = PNG.slice();
    new DataView(enormous.buffer).setUint32(16, 16384);
    expect(() => inspectRasterImage(enormous)).toThrow();
  });

  it("requires a dimension-bearing JPEG header and its complete closing marker", () => {
    const jpeg = new Uint8Array([
      0xff, 0xd8, 0xff, 0xc0, 0, 11, 8, 0, 2, 0, 3, 1, 1, 0x11, 0, 0xff, 0xd9,
    ]);
    expect(inspectRasterImage(jpeg)).toEqual({
      mime: "image/jpeg",
      width: 3,
      height: 2,
      pixels: 6,
    });
    expect(() => inspectRasterImage(jpeg.slice(0, -1))).toThrow();
  });

  it("bounds aggregate retained bytes and decoded pixels", () => {
    const session = new ArtifactImageSession();
    const leases = Array.from({ length: 4 }, () =>
      session.reserve(2 * 1024 * 1024, 4_000_000),
    );
    expect(() => session.reserve(1, 1)).toThrow();
    leases[0]!();
    const release = session.reserve(1, 1);
    release();
    release();
    session.dispose();
    expect(() => session.reserve(1, 1)).toThrow();
  });

  it("limits load concurrency and never starts queued work after retirement", async () => {
    const session = new ArtifactImageSession();
    let finishFirst: (() => void) | undefined;
    let finishSecond: (() => void) | undefined;
    const started: number[] = [];
    const first = session.run(async () => {
      started.push(1);
      await new Promise<void>((resolve) => {
        finishFirst = resolve;
      });
    });
    const second = session.run(async () => {
      started.push(2);
      await new Promise<void>((resolve) => {
        finishSecond = resolve;
      });
    });
    const third = session.run(async () => {
      started.push(3);
    });
    // Attach terminal handlers before rejecting queued work.
    const completed = Promise.allSettled([first, second, third]);
    await Promise.resolve();
    expect(started).toEqual([1, 2]);
    session.dispose();
    finishFirst!();
    finishSecond!();
    expect(
      (await completed).every((result) => result.status === "rejected"),
    ).toBe(true);
    expect(started).toEqual([1, 2]);
    expect(session.signal.aborted).toBe(true);
  });
});
