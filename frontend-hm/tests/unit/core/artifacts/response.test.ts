import { describe, expect, it, rs } from "@rstest/core";

import { readBoundedArtifactBytes } from "@/core/artifacts/response";

describe("bounded artifact response streams", () => {
  it("reads complete small bodies without trusting length headers", async () => {
    const response = new Response("hello", {
      headers: { "Content-Length": "999999999" },
    });
    const result = await readBoundedArtifactBytes(response, 8);
    expect(new TextDecoder().decode(result.bytes)).toBe("hello");
    expect(result.truncated).toBe(false);
  });

  it("cancels an ignored-Range stream at the byte ceiling", async () => {
    let reads = 0;
    const cancelled = rs.fn();
    const stream = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          reads += 1;
          controller.enqueue(new Uint8Array(4).fill(65));
        },
        cancel: cancelled,
      },
      { highWaterMark: 0 },
    );
    const result = await readBoundedArtifactBytes(new Response(stream), 8);
    expect(result.bytes).toHaveLength(8);
    expect(result.truncated).toBe(true);
    expect(reads).toBe(3);
    expect(cancelled).toHaveBeenCalledTimes(1);
  });

  it("never retains an oversized first chunk or calls arrayBuffer", async () => {
    const cancelled = rs.fn();
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array(1024 * 1024));
        },
        cancel: cancelled,
      }),
    );
    const fullRead = rs.spyOn(response, "arrayBuffer");
    const result = await readBoundedArtifactBytes(response, 16);
    expect(result.bytes).toHaveLength(16);
    expect(result.truncated).toBe(true);
    expect(fullRead).not.toHaveBeenCalled();
    expect(cancelled).toHaveBeenCalledTimes(1);
  });

  it("treats an exact ceiling with a completed body as complete", async () => {
    expect(
      (await readBoundedArtifactBytes(new Response("12345678"), 8)).truncated,
    ).toBe(false);
    expect(
      (await readBoundedArtifactBytes(new Response(null), 8)).bytes,
    ).toHaveLength(0);
  });

  it("aborts a stalled body even if its underlying source ignores abort", async () => {
    const cancelled = rs.fn();
    const controller = new AbortController();
    const response = new Response(
      new ReadableStream<Uint8Array>({ cancel: cancelled }),
    );
    const pending = readBoundedArtifactBytes(response, 8, controller.signal);
    controller.abort();
    await expect(pending).rejects.toThrow();
    expect(cancelled).toHaveBeenCalledTimes(1);
  });

  it("propagates a broken stream rather than claiming a complete document", async () => {
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.error(new Error("Broken transport"));
        },
      }),
    );
    await expect(readBoundedArtifactBytes(response, 8)).rejects.toThrow(
      "Broken transport",
    );
  });
});
