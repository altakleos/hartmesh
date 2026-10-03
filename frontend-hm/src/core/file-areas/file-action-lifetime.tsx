"use client";

import {
  createContext,
  useContext,
  useEffect,
  useRef,
  type ReactNode,
} from "react";
import { toast } from "sonner";

interface FileActionLifetime {
  active: boolean;
  toasts: Set<string | number>;
  signal: AbortSignal;
}

const FileActionContext = createContext<FileActionLifetime | null>(null);

/** Sequential file actions, conversation deletion and filename-bearing toasts belong to the
 * mounted account subtree. An already-sent request can finish, but retiring
 * its caller must prevent another request or a toast in the next account.
 * Mount at the workspace layout so ordinary page navigation keeps Undo and
 * pending batches alive for the same account.
 */
export function FileActionLifetimeProvider({
  children,
}: {
  children: ReactNode;
}) {
  const controller = useRef(new AbortController());
  const lifetime = useRef({
    active: true,
    toasts: new Set<string | number>(),
    signal: controller.current.signal,
  });
  useEffect(() => {
    const current = lifetime.current;
    // StrictMode replays setup after cleanup on the same provider instance.
    // Future operations read the fresh signal; old requests stay aborted.
    if (controller.current.signal.aborted)
      controller.current = new AbortController();
    current.signal = controller.current.signal;
    current.active = true;
    return () => {
      current.active = false;
      controller.current.abort();
      current.toasts.forEach((id) => toast.dismiss(id));
      current.toasts.clear();
    };
  }, []);
  return (
    <FileActionContext.Provider value={lifetime.current}>
      {children}
    </FileActionContext.Provider>
  );
}

export function useFileActionLifetime() {
  const lifetime = useContext(FileActionContext);
  if (!lifetime)
    throw new Error("File actions require FileActionLifetimeProvider");
  return lifetime;
}
