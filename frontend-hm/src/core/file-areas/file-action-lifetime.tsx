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
}

const FileActionContext = createContext<FileActionLifetime | null>(null);

/** Sequential file actions and their filename-bearing toasts belong to the
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
  const lifetime = useRef({ active: true, toasts: new Set<string | number>() });
  useEffect(() => {
    const current = lifetime.current;
    current.active = true;
    return () => {
      current.active = false;
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
