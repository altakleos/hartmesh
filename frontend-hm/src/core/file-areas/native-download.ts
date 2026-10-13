/** Observe small JSON refusals while the browser owns the streamed download.
 * An inconclusive observation says nothing about whether the transfer succeeded.
 */
export function observeNativeDownload({
  url,
  signal,
  onRefusal,
  onInconclusive,
  timeoutMs = 30_000,
}: {
  url: string;
  signal: AbortSignal;
  onRefusal: (envelope: unknown) => void;
  onInconclusive: () => void;
  timeoutMs?: number;
}): boolean {
  const destination = new URL(url, window.location.href);
  if (signal.aborted || destination.origin !== window.location.origin)
    return false;
  const frame = document.createElement("iframe");
  frame.hidden = true;
  frame.title = "";
  frame.setAttribute("sandbox", "allow-downloads allow-same-origin");
  let observing = true;
  const stopObserving = () => {
    observing = false;
    clearTimeout(timer);
    frame.removeEventListener("load", loaded);
  };
  const retired = () => {
    stopObserving();
    frame.remove();
  };
  const inconclusive = () => {
    if (!observing || signal.aborted) return;
    stopObserving();
    // Keep the frame until owner retirement: removing it could cancel delayed
    // response headers or an ongoing native transfer. Never issue a second GET.
    onInconclusive();
  };
  const loaded = () => {
    if (!observing || signal.aborted) return;
    try {
      const response = frame.contentDocument;
      if (!response || response.URL === "about:blank") return;
      if (
        response.URL !== destination.href ||
        response.contentType !== "application/json"
      ) {
        inconclusive();
        return;
      }
      const root = response.body;
      if (!root) {
        inconclusive();
        return;
      }
      const walker = response.createTreeWalker(root, NodeFilter.SHOW_TEXT);
      let text = "";
      let count = 0;
      let node: Node | null;
      while ((node = walker.nextNode())) {
        const part = node.nodeValue ?? "";
        if (++count > 64 || part.length > 8192 - text.length) {
          inconclusive();
          return;
        }
        text += part;
      }
      const body: unknown = JSON.parse(text);
      const detail =
        body && typeof body === "object" && "detail" in body
          ? body.detail
          : body;
      const code =
        detail && typeof detail === "object" && "code" in detail
          ? detail.code
          : null;
      const status =
        code === "storage-busy"
          ? 409
          : code === "files-full"
            ? 507
            : code === "storage-unavailable"
              ? 503
              : null;
      if (status === null) {
        inconclusive();
        return;
      }
      stopObserving();
      frame.remove();
      signal.removeEventListener("abort", retired);
      // The JSON reason is a presentation hint, never authorization or proof
      // of a successful read. Browsers do not expose iframe HTTP status here.
      onRefusal({ status, detail });
    } catch {
      inconclusive();
    }
  };
  const timer = setTimeout(inconclusive, timeoutMs);
  frame.addEventListener("load", loaded);
  signal.addEventListener("abort", retired, { once: true });
  frame.src = destination.href;
  document.body.append(frame);
  return true;
}
