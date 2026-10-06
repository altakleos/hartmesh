/** Settle host waiting on cancellation; native evaluation itself is trusted code. */
export function awaitPluginOperation<T>(
  pending: Promise<T>,
  signal: AbortSignal,
  discard?: (value: T) => void,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = () =>
      reject(
        new DOMException("Plugin work retired or timed out", "AbortError"),
      );
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
    void pending.then(
      (value) => {
        signal.removeEventListener("abort", abort);
        if (signal.aborted) {
          discard?.(value);
          abort();
        } else resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", abort);
        reject(
          error instanceof Error ? error : new Error("Plugin operation failed"),
        );
      },
    );
  });
}
