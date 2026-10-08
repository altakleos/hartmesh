"use client";

import type { Message } from "@langchain/langgraph-sdk";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import {
  appendSidecarReference,
  buildMessageSidecarContext,
  getNextSidecarOpenState,
  type SidecarContext,
  type SidecarReferenceStateItem,
} from "@/core/sidecar";
import { findLatestSidecarThread } from "@/core/sidecar/api";
import type { ThreadStreamOptions } from "@/core/threads/hooks";

export type SidecarReference = SidecarReferenceStateItem;

type SidecarContextValue = {
  open: boolean;
  activeReferences: SidecarReference[];
  conversationQuotes: SidecarReference[];
  parentThreadId: string;
  context: ThreadStreamOptions["context"];
  setContext: (context: ThreadStreamOptions["context"]) => void;
  isMock?: boolean;
  sidecarThreadId: string | null;
  setSidecarThreadId: (threadId: string | null) => void;
  restoreSidecarThread: (options?: {
    force?: boolean;
  }) => Promise<string | null>;
  addContextToConversation: (context: SidecarContext) => void;
  clearConversationQuotes: (ids?: number[]) => void;
  clearActiveReferences: () => void;
  openSidecar: () => void;
  openContext: (context: SidecarContext) => void;
  openSelectedText: (
    message: Message,
    selectedText: string,
    displayIndex?: number,
  ) => void;
  close: () => void;
};

const SidecarContextObject = createContext<SidecarContextValue | null>(null);

export function SidecarProvider({
  children,
  parentThreadId,
  context,
  isMock,
  enabled = true,
}: {
  children: ReactNode;
  parentThreadId: string;
  context: ThreadStreamOptions["context"];
  isMock?: boolean;
  enabled?: boolean;
}) {
  const capability = useRef({ enabled, epoch: 0 });
  if (capability.current.enabled !== enabled)
    capability.current = { enabled, epoch: capability.current.epoch + 1 };
  const [open, setOpen] = useState(false);
  const [activeReferences, setActiveReferences] = useState<SidecarReference[]>(
    [],
  );
  const [sidecarThreadId, setSidecarThreadId] = useState<string | null>(null);
  const [sidecarContext, setSidecarContext] =
    useState<ThreadStreamOptions["context"]>(context);
  const [conversationQuotes, setConversationQuotes] = useState<
    SidecarReference[]
  >([]);
  const referenceIdRef = useRef(0);
  const parentThreadIdRef = useRef(parentThreadId);
  const sidecarThreadIdRef = useRef<string | null>(null);
  const restoreRequestRef = useRef<{
    parentThreadId: string;
    epoch: number;
    promise: Promise<string | null>;
  } | null>(null);

  const updateSidecarThreadId = useCallback((threadId: string | null) => {
    if (threadId && !capability.current.enabled) return;
    sidecarThreadIdRef.current = threadId;
    setSidecarThreadId(threadId);
  }, []);

  const createReference = useCallback((nextContext: SidecarContext) => {
    referenceIdRef.current += 1;
    return {
      id: referenceIdRef.current,
      context: nextContext,
    };
  }, []);

  useEffect(() => {
    if (parentThreadIdRef.current === parentThreadId) {
      return;
    }
    parentThreadIdRef.current = parentThreadId;
    setOpen(false);
    setActiveReferences([]);
    setSidecarContext(context);
    updateSidecarThreadId(null);
    setConversationQuotes([]);
  }, [context, parentThreadId, updateSidecarThreadId]);

  useEffect(() => {
    if (!enabled) {
      setOpen(false);
      setActiveReferences([]);
      setConversationQuotes([]);
      updateSidecarThreadId(null);
      restoreRequestRef.current = null;
    }
  }, [enabled, updateSidecarThreadId]);

  const restoreSidecarThread = useCallback(
    async (options?: { force?: boolean }) => {
      if (!enabled || !capability.current.enabled) return null;
      const epoch = capability.current.epoch;
      // A non-forced restore trusts the cached id; a forced restore always
      // re-queries the backend so a sidecar deleted elsewhere reconciles to
      // null instead of pointing the trigger at a dead thread (#3555).
      if (!options?.force && sidecarThreadIdRef.current) {
        return sidecarThreadIdRef.current;
      }

      const restoreRequest = restoreRequestRef.current;
      if (
        restoreRequest?.parentThreadId === parentThreadId &&
        restoreRequest.epoch === epoch
      ) {
        return restoreRequest.promise;
      }

      const promise = findLatestSidecarThread({
        parentThreadId,
        isMock,
      })
        .then((thread) => {
          const threadId = thread?.thread_id ?? null;
          if (
            parentThreadIdRef.current !== parentThreadId ||
            !capability.current.enabled ||
            capability.current.epoch !== epoch
          ) {
            return null;
          }
          // Reconcile the cache with the backend: adopt a freshly found
          // thread, and on a forced refresh clear a stale id when the backend
          // no longer has a matching sidecar thread.
          if (threadId) {
            if (!sidecarThreadIdRef.current) {
              updateSidecarThreadId(threadId);
            }
          } else if (options?.force && sidecarThreadIdRef.current) {
            updateSidecarThreadId(null);
          }
          return threadId;
        })
        .catch(() => null)
        .finally(() => {
          if (restoreRequestRef.current?.promise === promise) {
            restoreRequestRef.current = null;
          }
        });

      restoreRequestRef.current = {
        parentThreadId,
        epoch,
        promise,
      };

      return promise;
    },
    [enabled, isMock, parentThreadId, updateSidecarThreadId],
  );

  useEffect(() => {
    void restoreSidecarThread();
  }, [restoreSidecarThread]);

  const openContext = useCallback(
    (nextContext: SidecarContext) => {
      if (!capability.current.enabled) return;
      const nextReference = createReference(nextContext);

      setActiveReferences(
        (references) =>
          getNextSidecarOpenState({
            open,
            sidecarThreadId,
            activeReferences: references,
            nextReference,
          }).activeReferences,
      );
      setOpen(true);
    },
    [createReference, open, sidecarThreadId],
  );

  const addContextToConversation = useCallback(
    (nextContext: SidecarContext) => {
      if (!capability.current.enabled) return;
      const nextReference = createReference(nextContext);
      setConversationQuotes((references) =>
        appendSidecarReference(references, nextReference),
      );
    },
    [createReference],
  );

  const clearConversationQuotes = useCallback((ids?: number[]) => {
    if (!ids) {
      setConversationQuotes([]);
      return;
    }
    const idsToClear = new Set(ids);
    setConversationQuotes((quotes) =>
      quotes.filter((quote) => !idsToClear.has(quote.id)),
    );
  }, []);

  const clearActiveReferences = useCallback(() => {
    setActiveReferences([]);
  }, []);

  const openSidecar = useCallback(() => {
    if (!capability.current.enabled) return;
    setOpen(true);
  }, []);

  const openSelectedText = useCallback(
    (message: Message, selectedText: string, displayIndex?: number) => {
      const nextContext = buildMessageSidecarContext(message, displayIndex, {
        selectedText,
      });
      if (!nextContext) {
        return;
      }
      openContext(nextContext);
    },
    [openContext],
  );

  const close = useCallback(() => {
    setOpen(false);
  }, []);

  const value = useMemo<SidecarContextValue>(
    () => ({
      open,
      activeReferences,
      conversationQuotes,
      parentThreadId,
      context: sidecarContext,
      setContext: setSidecarContext,
      isMock,
      sidecarThreadId,
      setSidecarThreadId: updateSidecarThreadId,
      restoreSidecarThread,
      addContextToConversation,
      clearConversationQuotes,
      clearActiveReferences,
      openSidecar,
      openContext,
      openSelectedText,
      close,
    }),
    [
      activeReferences,
      addContextToConversation,
      clearActiveReferences,
      clearConversationQuotes,
      close,
      conversationQuotes,
      isMock,
      open,
      openContext,
      openSelectedText,
      openSidecar,
      parentThreadId,
      restoreSidecarThread,
      sidecarContext,
      sidecarThreadId,
      updateSidecarThreadId,
    ],
  );

  return (
    <SidecarContextObject.Provider value={enabled ? value : null}>
      {children}
    </SidecarContextObject.Provider>
  );
}

export function useMaybeSidecar() {
  return useContext(SidecarContextObject);
}

export function useSidecar() {
  const context = useMaybeSidecar();
  if (!context) {
    throw new Error("useSidecar must be used within a SidecarProvider");
  }
  return context;
}
