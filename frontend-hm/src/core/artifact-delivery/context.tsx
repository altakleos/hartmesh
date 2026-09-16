"use client";

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";

import type { ArtifactDeliveryFailure } from "./types";

export interface ArtifactDeliveryContextValue {
  failuresByRunId: Record<string, ArtifactDeliveryFailure>;
  recordFailure: (failure: ArtifactDeliveryFailure) => void;
}

const EMPTY_FAILURES: Record<string, ArtifactDeliveryFailure> = {};

export const ArtifactDeliveryContext =
  createContext<ArtifactDeliveryContextValue>({
    failuresByRunId: EMPTY_FAILURES,
    recordFailure: () => {
      /* noop */
    },
  });

/**
 * Holds the delivery verdicts this client heard on the stream, keyed by run.
 *
 * Keyed by run rather than kept as "the last failure" because a thread can hold
 * several failed turns, and the notice belongs under the turn that failed. Run
 * keys are unique, so nothing is cleared while the chat is mounted — not on a
 * thread switch and not on a replay gap, since no durable source can hand the
 * verdict back. A reload does end it, leaving the workspace-changes card as the
 * way to the files until durable rehydration lands.
 */
export function ArtifactDeliveryProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [failuresByRunId, setFailuresByRunId] =
    useState<Record<string, ArtifactDeliveryFailure>>(EMPTY_FAILURES);

  const recordFailure = useCallback((failure: ArtifactDeliveryFailure) => {
    setFailuresByRunId((current) =>
      current[failure.runId] === failure
        ? current
        : { ...current, [failure.runId]: failure },
    );
  }, []);

  const value = useMemo(
    () => ({ failuresByRunId, recordFailure }),
    [failuresByRunId, recordFailure],
  );

  return (
    <ArtifactDeliveryContext.Provider value={value}>
      {children}
    </ArtifactDeliveryContext.Provider>
  );
}

export function useArtifactDeliveryContext() {
  return useContext(ArtifactDeliveryContext);
}

export function useArtifactDeliveryFailure(
  runId: string | undefined,
): ArtifactDeliveryFailure | undefined {
  const { failuresByRunId } = useArtifactDeliveryContext();
  return runId ? failuresByRunId[runId] : undefined;
}
