/** Retire host waiting while still observing late results and rejections. */
export function awaitAbortable<T>(
  pending: Promise<T>,
  signal: AbortSignal,
  discard?: (value: T) => void,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = () =>
      reject(new DOMException("Operation retired or timed out", "AbortError"));
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
    void pending.then(
      (value) => {
        signal.removeEventListener("abort", abort);
        if (signal.aborted) {
          try {
            discard?.(value);
          } catch {
            /* Contain disposal errors. */
          }
          abort();
        } else resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", abort);
        reject(error instanceof Error ? error : new Error("Operation failed"));
      },
    );
  });
}
