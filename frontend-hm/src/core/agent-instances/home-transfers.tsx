"use client";

import { useQueryClient } from "@tanstack/react-query";
import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";

import { UnauthorizedError } from "@/core/api/errors";
import { useAuth } from "@/core/auth/AuthProvider";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { StorageRequestError } from "@/core/spaces/api";

import { InstanceApiError, type AgentInstanceDetail } from "./api";
import {
  createFileTransfers,
  isFinalTransfer,
  type AgentTransfer,
} from "./file-transfers";
import {
  checkHomeTransferAccess,
  type TransferHome as Home,
} from "./home-transfer-access";
import { hasCeiling, INSPECT } from "./presentation";

type Queue = ReturnType<typeof createFileTransfers>;
const emptyRows: AgentTransfer[] = [];

function homeKey(home: Home) {
  return JSON.stringify([
    home.instanceId,
    home.generation,
    home.homeId,
    home.homeGeneration,
  ]);
}
function accessEnded(error: unknown) {
  return (
    error instanceof UnauthorizedError ||
    ((error instanceof InstanceApiError ||
      error instanceof StorageRequestError) &&
      [401, 403, 404].includes(error.status))
  );
}

class HomeTransfers {
  private owners = new Map<
    string,
    {
      instanceId: string;
      queue: Queue;
      controller: AbortController;
      verify: () => Promise<boolean>;
    }
  >();
  private recent = new Map<string, Queue>();
  private listeners = new Set<() => void>();
  readonly subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  constructor(
    private readonly lifetime: ReturnType<typeof useFileActionLifetime>,
    private readonly changed: () => void,
  ) {}
  private update(key: string, queue: Queue) {
    if (this.owners.get(key)?.queue === queue) {
      for (const row of queue.snapshot()) {
        if (isFinalTransfer(row) && !this.recent.has(row.id))
          this.recent.set(row.id, queue);
      }
      if (!queue.snapshot().length) this.retire(key);
    }
    // Settled rows are receipts. Bound their history across the signed-in workspace.
    if (this.recent.size > 100) {
      const oldest = this.recent.entries().next().value;
      if (oldest) {
        this.recent.delete(oldest[0]);
        oldest[1].remove(oldest[0]);
      }
    }
    this.listeners.forEach((listener) => listener());
  }
  find(home: Home) {
    return this.owners.get(homeKey(home))?.queue;
  }
  forgetInstance(id: string) {
    for (const [key, owner] of this.owners)
      if (owner.instanceId === id) this.retire(key);
  }
  verifyRetained(home: Home) {
    return this.owners.get(homeKey(home))?.verify() ?? Promise.resolve(true);
  }
  private retire(key: string) {
    const owner = this.owners.get(key);
    if (!owner) return;
    this.owners.delete(key);
    for (const [id, queue] of this.recent)
      if (queue === owner.queue) this.recent.delete(id);
    owner.controller.abort();
    owner.queue.clear();
  }
  clear() {
    for (const key of this.owners.keys()) this.retire(key);
  }
  observe() {
    for (const owner of this.owners.values()) {
      if (
        owner.queue
          .snapshot()
          .some(
            (row) =>
              row.status === "uploading" ||
              (!!row.lookup && !isFinalTransfer(row)),
          )
      )
        void owner.verify();
    }
  }
  queue(home: Home) {
    const key = homeKey(home);
    const old = this.owners.get(key);
    if (old) return old.queue;
    const controller = new AbortController();
    const signal = AbortSignal.any([controller.signal, this.lifetime.signal]);
    const verify = async () => {
      if (signal.aborted) return false;
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(10000)]);
      try {
        if (await checkHomeTransferAccess(home, deadline))
          return !signal.aborted;
        if (signal.aborted) return false;
        this.retire(key);
      } catch (error) {
        if (accessEnded(error) && !signal.aborted) this.retire(key);
      }
      return false;
    };
    const queue = createFileTransfers({
      instanceId: home.instanceId,
      captureSignal: () => signal,
      enabled: () => !signal.aborted,
      changed: this.changed,
      confirmAccess: verify,
    });
    queue.subscribe(() => this.update(key, queue));
    this.owners.set(key, {
      instanceId: home.instanceId,
      queue,
      controller,
      verify,
    });
    return queue;
  }
}

const Context = createContext<HomeTransfers | null>(null);
export function HomeFileTransferProvider({
  children,
}: {
  children: ReactNode;
}) {
  const { user } = useAuth();
  const authority = JSON.stringify([
    user?.id,
    user?.system_role,
    user?.permissions,
  ]);
  const lifetime = useFileActionLifetime();
  const client = useQueryClient();
  const registry = useMemo(
    () =>
      new HomeTransfers(lifetime, () => {
        for (const group of ["agent-files", "agent-instances", "file-browser"])
          void client.invalidateQueries({ queryKey: [group] });
      }),
    // A permission change starts a fresh registry even when the route stays mounted.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [lifetime, client, authority],
  );
  useEffect(() => {
    const observe = () => registry.observe();
    const timer = setInterval(observe, 15000);
    window.addEventListener("focus", observe);
    return () => {
      clearInterval(timer);
      window.removeEventListener("focus", observe);
      registry.clear();
    };
  }, [registry]);
  return <Context.Provider value={registry}>{children}</Context.Provider>;
}

export function useHomeFileTransfers({
  instance,
  homeGeneration,
  homePermissions,
  accessDenied = false,
  folder,
  enabled,
}: {
  instance: AgentInstanceDetail;
  homeGeneration: number | undefined;
  homePermissions: number | undefined;
  accessDenied?: boolean;
  folder: string;
  enabled: boolean;
}) {
  const registry = useContext(Context);
  if (!registry)
    throw new Error("Home uploads require HomeFileTransferProvider");
  const { user } = useAuth();
  const userId = user?.id;
  const lostAccess =
    accessDenied ||
    !userId ||
    instance.status !== "active" ||
    !(instance.permissions & INSPECT) ||
    (homePermissions !== undefined && (homePermissions & 3) !== 3) ||
    !hasCeiling(user?.permissions, "agents:read") ||
    !hasCeiling(user?.permissions, "agents:write");
  const {
    id: instanceId,
    generation,
    home_id: homeId,
    home_generation: expected,
  } = instance;
  const home = useMemo<Home | null>(
    () =>
      !lostAccess &&
      userId &&
      homeId &&
      homePermissions !== undefined &&
      homeGeneration !== undefined &&
      (expected == null || homeGeneration === expected)
        ? { accountId: userId, instanceId, generation, homeId, homeGeneration }
        : null,
    [
      lostAccess,
      userId,
      instanceId,
      generation,
      homeId,
      homePermissions,
      homeGeneration,
      expected,
    ],
  );
  const [admitted, setAdmitted] = useState<Home | null>(null);
  useEffect(() => {
    if (lostAccess) registry.forgetInstance(instanceId);
    if (!home) {
      setAdmitted(null);
      return;
    }
    if (!registry.find(home)) {
      setAdmitted(home);
      return;
    }
    let current = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    async function verify() {
      const ok = await registry!.verifyRetained(home!);
      if (!current) return;
      setAdmitted(ok ? home : null);
      if (!ok && registry!.find(home!))
        timer = setTimeout(() => {
          void verify();
        }, 15000);
    }
    void verify();
    return () => {
      current = false;
      clearTimeout(timer);
    };
  }, [registry, instanceId, home, lostAccess]);
  const read = () =>
    (home && admitted === home ? registry.find(home)?.snapshot() : undefined) ??
    emptyRows;
  const rows = useSyncExternalStore(registry.subscribe, read, () => emptyRows);
  return {
    rows,
    add: (files: readonly File[]) =>
      home && enabled ? registry.queue(home).add(files, folder) : [],
    retry: (id: string) => {
      if (home && enabled) registry.find(home)?.retry(id);
    },
    remove: (id: string) => {
      if (home) registry.find(home)?.remove(id);
    },
    hasUnfinished: rows.some((row) => row.status !== "saved"),
  };
}
