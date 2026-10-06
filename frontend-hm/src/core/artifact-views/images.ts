import { fetch } from "@/core/api/fetcher";
import { readBoundedArtifactBytes } from "@/core/artifacts/response";
import { urlOfArtifact } from "@/core/artifacts/utils";

export const VIEW_IMAGE_MAX_BYTES = 2 * 1024 * 1024;
export const VIEW_IMAGE_MAX_PIXELS = 4_000_000;
export const VIEW_IMAGE_TOTAL_BYTES = 8 * 1024 * 1024;
export const VIEW_IMAGE_TOTAL_PIXELS = 16_000_000;
export const VIEW_IMAGE_CONCURRENCY = 2;

type Raster = {
  mime: "image/png" | "image/jpeg";
  width: number;
  height: number;
  pixels: number;
};

function raster(mime: Raster["mime"], width: number, height: number): Raster {
  const pixels = width * height;
  if (
    width <= 0 ||
    height <= 0 ||
    width > 8192 ||
    height > 8192 ||
    pixels > VIEW_IMAGE_MAX_PIXELS
  )
    throw new Error("Image dimensions exceed the view budget.");
  return { mime, width, height, pixels };
}

/** Inspect complete PNG/JPEG framing before allocating decoded image pixels. */
export function inspectRasterImage(bytes: Uint8Array): Raster {
  if (bytes.byteLength > VIEW_IMAGE_MAX_BYTES)
    throw new Error("Image byte budget exceeded.");
  const data = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const png = [137, 80, 78, 71, 13, 10, 26, 10];
  if (
    bytes.length >= 33 &&
    png.every((value, index) => bytes[index] === value)
  ) {
    if (
      data.getUint32(8) !== 13 ||
      String.fromCharCode(...bytes.subarray(12, 16)) !== "IHDR"
    )
      throw new Error("Invalid PNG header.");
    const result = raster("image/png", data.getUint32(16), data.getUint32(20));
    let offset = 8;
    let sawData = false;
    while (offset + 12 <= bytes.length) {
      const size = data.getUint32(offset);
      const type = String.fromCharCode(
        ...bytes.subarray(offset + 4, offset + 8),
      );
      const end = offset + size + 12;
      if (
        end > bytes.length ||
        ["acTL", "fcTL", "fdAT"].includes(type) ||
        (type === "IHDR" && offset !== 8)
      )
        throw new Error("Unsupported or incomplete PNG.");
      if (type === "IDAT") sawData = true;
      if (type === "IEND") {
        if (size !== 0 || end !== bytes.length || !sawData)
          throw new Error("Incomplete PNG.");
        return result;
      }
      offset = end;
    }
    throw new Error("Incomplete PNG.");
  }
  if (
    bytes.length >= 4 &&
    bytes[0] === 0xff &&
    bytes[1] === 0xd8 &&
    bytes.at(-2) === 0xff &&
    bytes.at(-1) === 0xd9
  ) {
    let offset = 2;
    while (offset + 4 <= bytes.length) {
      if (bytes[offset] !== 0xff) throw new Error("Invalid JPEG header.");
      while (bytes[offset] === 0xff) offset += 1;
      const marker = bytes[offset++]!;
      if (marker === 0xd9 || marker === 0xda) break;
      if (marker === 1 || (marker >= 0xd0 && marker <= 0xd7)) continue;
      if (offset + 2 > bytes.length) break;
      const size = data.getUint16(offset);
      if (size < 2 || offset + size > bytes.length) break;
      if ([0xc0, 0xc1, 0xc2].includes(marker) && size >= 8)
        return raster(
          "image/jpeg",
          data.getUint16(offset + 5),
          data.getUint16(offset + 3),
        );
      offset += size;
    }
  }
  throw new Error("Only complete PNG or JPEG images are supported.");
}

/** One displayed document owns its finite queue, reservations and object URLs. */
export class ArtifactImageSession {
  private readonly controller = new AbortController();
  private active = 0;
  private readonly queue: {
    start: () => void;
    reject: (error: unknown) => void;
  }[] = [];
  private retainedBytes = 0;
  private retainedPixels = 0;
  private readonly urls = new Map<string, () => void>();

  get signal() {
    return this.controller.signal;
  }

  reserve(bytes: number, pixels: number): () => void {
    this.signal.throwIfAborted();
    if (
      !Number.isSafeInteger(bytes) ||
      !Number.isSafeInteger(pixels) ||
      bytes <= 0 ||
      pixels <= 0 ||
      bytes > VIEW_IMAGE_MAX_BYTES ||
      pixels > VIEW_IMAGE_MAX_PIXELS ||
      this.retainedBytes + bytes > VIEW_IMAGE_TOTAL_BYTES ||
      this.retainedPixels + pixels > VIEW_IMAGE_TOTAL_PIXELS
    )
      throw new Error("Document image budget exceeded.");
    this.retainedBytes += bytes;
    this.retainedPixels += pixels;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.retainedBytes = Math.max(0, this.retainedBytes - bytes);
      this.retainedPixels = Math.max(0, this.retainedPixels - pixels);
    };
  }

  run<T>(task: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    if (this.signal.aborted || signal?.aborted)
      return Promise.reject(new DOMException("View retired.", "AbortError"));
    if (this.queue.length >= 32)
      return Promise.reject(new Error("Image queue budget exceeded."));
    return new Promise<T>((resolve, reject) => {
      const cleanup = () => {
        this.signal.removeEventListener("abort", abort);
        signal?.removeEventListener("abort", abort);
      };
      const abort = () => {
        // Settle the caller now, but keep a running task's physical slot and
        // resource leases until even an unabortable native decoder finishes.
        const index = this.queue.indexOf(job);
        if (index >= 0) this.queue.splice(index, 1);
        reject(
          new DOMException("Image loading retired or timed out.", "AbortError"),
        );
        cleanup();
        this.pump();
      };
      const job = {
        reject,
        start: () => {
          this.active += 1;
          void (async () => {
            try {
              this.signal.throwIfAborted();
              signal?.throwIfAborted();
              const value = await task();
              this.signal.throwIfAborted();
              signal?.throwIfAborted();
              resolve(value);
            } catch (error) {
              reject(
                error instanceof Error
                  ? error
                  : new Error("Image loading failed."),
              );
            } finally {
              cleanup();
              this.active -= 1;
              this.pump();
            }
          })();
        },
      };
      this.signal.addEventListener("abort", abort, { once: true });
      signal?.addEventListener("abort", abort, { once: true });
      this.queue.push(job);
      this.pump();
    });
  }

  private pump() {
    while (
      !this.signal.aborted &&
      this.active < VIEW_IMAGE_CONCURRENCY &&
      this.queue.length > 0
    )
      this.queue.shift()!.start();
  }

  releaseURL(url: string) {
    const release = this.urls.get(url);
    if (!release) return;
    this.urls.delete(url);
    URL.revokeObjectURL(url);
    release();
  }

  dispose() {
    if (this.signal.aborted) return;
    this.controller.abort(new DOMException("View retired.", "AbortError"));
    for (const queued of this.queue.splice(0))
      queued.reject(this.signal.reason);
    for (const url of this.urls.keys()) this.releaseURL(url);
  }

  load({
    filepath,
    threadId,
    revision,
    isMock,
    signal,
  }: {
    filepath: string;
    threadId: string;
    revision: string;
    isMock?: boolean;
    signal: AbortSignal;
  }): Promise<string> {
    const controller = new AbortController();
    const abort = () => controller.abort();
    if (signal.aborted || this.signal.aborted) abort();
    signal.addEventListener("abort", abort, { once: true });
    this.signal.addEventListener("abort", abort, { once: true });
    // Queue time belongs to the image deadline too. Stalled decoders cannot
    // leave later placeholders pending forever or release their owned pixels.
    const timeout = setTimeout(abort, 20_000);
    return this.run(async () => {
      let release: (() => void) | undefined;
      try {
        controller.signal.throwIfAborted();
        const response = await fetch(
          `${urlOfArtifact({ filepath, threadId, isMock })}?revision=${encodeURIComponent(revision)}`,
          {
            cache: "no-store",
            headers: { Range: `bytes=0-${VIEW_IMAGE_MAX_BYTES - 1}` },
            signal: controller.signal,
          },
        );
        if (!response.ok) {
          void response.body?.cancel().catch(() => undefined);
          throw new Error("Image unavailable.");
        }
        const result = await readBoundedArtifactBytes(
          response,
          VIEW_IMAGE_MAX_BYTES,
          controller.signal,
        );
        const range = response.headers.get("Content-Range");
        const match = range?.match(/^bytes 0-(\d+)\/(\d+)$/);
        if (
          result.truncated ||
          (response.status === 206 &&
            (!match ||
              Number(match[2]) !== result.bytes.length ||
              Number(match[1]) + 1 !== result.bytes.length))
        )
          throw new Error("Image response is incomplete.");
        const image = inspectRasterImage(result.bytes);
        release = this.reserve(result.bytes.byteLength, image.pixels);
        const blob = new Blob([result.bytes as Uint8Array<ArrayBuffer>], {
          type: image.mime,
        });
        if (!globalThis.createImageBitmap)
          throw new Error("Raster decoding is unavailable.");
        const bitmap = await createImageBitmap(blob);
        try {
          if (
            bitmap.width * bitmap.height !== image.pixels ||
            bitmap.width > 8192 ||
            bitmap.height > 8192
          )
            throw new Error("Image dimensions do not match its content.");
        } finally {
          bitmap.close();
        }
        controller.signal.throwIfAborted();
        this.signal.throwIfAborted();
        const url = URL.createObjectURL(blob);
        this.urls.set(url, release);
        release = undefined;
        return url;
      } finally {
        release?.();
      }
    }, controller.signal).finally(() => {
      clearTimeout(timeout);
      signal.removeEventListener("abort", abort);
      this.signal.removeEventListener("abort", abort);
    });
  }
}
