"use client";

import {
  type QueryClient,
  useQueries,
  useQueryClient,
} from "@tanstack/react-query";

import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";

import { getPeople, type Person } from "./api";

export interface PersonState {
  status: "loading" | "ready" | "error";
  person: Person | null;
}

interface Lookup {
  id: string;
  signal: AbortSignal;
  resolve: (person: Person | null) => void;
  reject: (error: unknown) => void;
}

// Coalesce the per-id queries of one render, also across component boundaries.
// The QueryClient owns the answers; the batcher retains only pending requests.
const batches = new WeakMap<QueryClient, Map<string, Lookup[]>>();

function lookup(
  client: QueryClient,
  identity: string,
  id: string,
  signal: AbortSignal,
): Promise<Person | null> {
  let identities = batches.get(client);
  if (!identities) {
    identities = new Map();
    batches.set(client, identities);
  }
  let pending = identities.get(identity);
  if (!pending) {
    pending = [];
    identities.set(identity, pending);
    const batch = pending;
    queueMicrotask(() => {
      identities.delete(identity);
      const live = batch.filter((item) => !item.signal.aborted);
      for (let offset = 0; offset < live.length; offset += 50) {
        const entries = live.slice(offset, offset + 50);
        const controller = new AbortController();
        const abortIfRetired = () => {
          if (entries.every((entry) => entry.signal.aborted))
            controller.abort();
        };
        for (const entry of entries)
          entry.signal.addEventListener("abort", abortIfRetired, {
            once: true,
          });
        void getPeople(
          entries.map((entry) => entry.id),
          controller.signal,
        )
          .then(
            ({ users }) => {
              const people = new Map(
                users.map((person) => [person.id, person]),
              );
              for (const entry of entries) {
                if (!entry.signal.aborted)
                  entry.resolve(people.get(entry.id) ?? null);
              }
            },
            (error: unknown) => {
              for (const entry of entries) entry.reject(error);
            },
          )
          .finally(() => {
            for (const entry of entries)
              entry.signal.removeEventListener("abort", abortIfRetired);
          });
      }
    });
  }
  const result = new Promise<Person | null>((resolve, reject) => {
    pending.push({ id, signal, resolve, reject });
  });
  return awaitAbortable(result, signal);
}

/** Resolve each id once per page lifetime and current account/role. */
export function usePeople(ids: readonly string[]): Map<string, PersonState> {
  const { user } = useAuth();
  const client = useQueryClient();
  const unique = [...new Set(ids.filter(Boolean))];
  const identity = JSON.stringify([user?.id, user?.system_role]);
  const queries = useQueries({
    queries: unique.map((id) => ({
      queryKey: ["people", user?.id, user?.system_role, id],
      queryFn: ({ signal }: { signal: AbortSignal }) =>
        lookup(client, identity, id, signal),
      enabled: !!user,
      staleTime: Infinity,
      gcTime: Infinity,
      retry: false,
      retryOnMount: false,
      refetchOnMount: false,
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
    })),
  });
  return new Map(
    unique.map((id, index) => {
      const query = queries[index]!;
      return [
        id,
        {
          status: query.isError
            ? "error"
            : query.isSuccess
              ? "ready"
              : "loading",
          person: query.data ?? null,
        },
      ];
    }),
  );
}
