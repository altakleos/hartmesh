import { useCallback, useEffect, useRef } from "react";

import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";

/** Account ownership survives navigation; a storage page action must not. */
export function useSpaceActionSignal() {
  const account = useFileActionLifetime();
  const controller = useRef(new AbortController());
  useEffect(() => {
    if (controller.current.signal.aborted)
      controller.current = new AbortController();
    const owned = controller.current;
    return () => owned.abort();
  }, []);
  // Each action captures its own combined signal. StrictMode replay cannot
  // make a retired action live when the component gets a fresh controller.
  return useCallback(
    () => AbortSignal.any([controller.current.signal, account.signal]),
    [account],
  );
}
