/** Read a preview without relying on Range or Content-Length being honored. */
export async function readBoundedArtifactBytes(
  response: Response,
  maxBytes: number,
  signal?: AbortSignal,
): Promise<{ bytes: Uint8Array; truncated: boolean }> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
    throw new Error("Invalid artifact byte budget.");
  }
  signal?.throwIfAborted();
  if (!response.body) return { bytes: new Uint8Array(), truncated: false };
  const reader = response.body.getReader();
  const cancel = () => {
    void reader.cancel().catch(() => undefined);
  };
  signal?.addEventListener("abort", cancel, { once: true });
  let buffer = new Uint8Array(0);
  let length = 0;
  let truncated = false;
  try {
    while (true) {
      signal?.throwIfAborted();
      const next = await reader.read();
      signal?.throwIfAborted();
      if (next.done) break;
      const remaining = maxBytes - length;
      const retained = next.value.subarray(0, remaining);
      const nextLength = length + retained.byteLength;
      if (nextLength > buffer.byteLength) {
        const grown = new Uint8Array(
          Math.min(maxBytes, Math.max(nextLength, buffer.byteLength * 2, 8192)),
        );
        grown.set(buffer);
        buffer = grown;
      }
      buffer.set(retained, length);
      length = nextLength;
      if (next.value.byteLength > remaining) {
        truncated = true;
        cancel();
        break;
      }
    }
    return { bytes: buffer.slice(0, length), truncated };
  } catch (error) {
    cancel();
    throw error;
  } finally {
    signal?.removeEventListener("abort", cancel);
    reader.releaseLock();
  }
}
