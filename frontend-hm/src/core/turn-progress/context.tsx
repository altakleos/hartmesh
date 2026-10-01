"use client";

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";

import type { TurnProgress } from "./types";

export interface TurnProgressContextValue {
  /** The latest stage heard for each thread that is streaming. */
  byThread: Readonly<Record<string, TurnProgress>>;
  record: (threadId: string, progress: TurnProgress) => void;
  clear: (threadId: string) => void;
}

export const TurnProgressContext = createContext<TurnProgressContextValue>({
  byThread: {},
  record: () => {
    /* noop */
  },
  clear: () => {
    /* noop */
  },
});

/**
 * Holds the latest progress frame this client heard, per thread.
 *
 * Keyed by thread rather than kept as "the last frame": the chat and the
 * sidecar each stream their own thread under the same providers, and the
 * label belongs to the activity row of the turn that reported it. A frame
 * replaces whatever the thread held, whichever run it names — frames arrive
 * in stream order, and a run that re-enters the worker restarts its clock,
 * so an `atMs` comparison would keep a stale stage. The stream hook clears
 * the thread's slot when its run finishes, fails, is stopped or is replayed,
 * so a stale "Thinking" cannot outlive its turn.
 */
export function TurnProgressProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [byThread, setByThread] = useState<Record<string, TurnProgress>>({});

  const record = useCallback((threadId: string, progress: TurnProgress) => {
    setByThread((previous) => ({ ...previous, [threadId]: progress }));
  }, []);

  const clear = useCallback((threadId: string) => {
    setByThread((previous) => {
      if (!(threadId in previous)) return previous;
      const next = { ...previous };
      delete next[threadId];
      return next;
    });
  }, []);

  const value = useMemo(
    () => ({ byThread, record, clear }),
    [byThread, record, clear],
  );

  return (
    <TurnProgressContext.Provider value={value}>
      {children}
    </TurnProgressContext.Provider>
  );
}

export function useTurnProgressContext() {
  return useContext(TurnProgressContext);
}

/** The latest stage the given thread's run reported, or null. */
export function useTurnProgress(
  threadId: string | null | undefined,
): TurnProgress | null {
  const { byThread } = useTurnProgressContext();
  return threadId ? (byThread[threadId] ?? null) : null;
}
