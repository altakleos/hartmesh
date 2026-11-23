"use client";
import { useCallback, useLayoutEffect, useRef, useState } from "react";

import { useFileActionLifetime } from "./file-action-lifetime";
import type { FileListOrder } from "./selection";

interface ViewState {
  query: string;
  order: FileListOrder;
  count: number;
  scroll: number;
  control?: string;
}
const initial: ViewState = { query: "", order: "name", count: 100, scroll: 0 };
const sessions = new WeakMap<
  AbortSignal,
  Map<
    string,
    { source: string; grant: string; scope: string; value: ViewState }
  >
>();
/** When a mounted route observes a definite loss, retire that source's saved names and queries. */
export function useRetireBrowserHistory(source: string, denied: boolean) {
  const { signal } = useFileActionLifetime();
  useLayoutEffect(() => {
    const entries = sessions.get(signal);
    if (!denied || !entries) return;
    for (const [key, record] of entries)
      if (record.source === source) entries.delete(key);
  }, [source, denied, signal]);
}

/** History contains only an opaque ID. File names and search text live in the current session. */
export function useBrowserHistory(
  source: string,
  grant: string,
  folder: string,
  ready = true,
  searchMode?: boolean,
) {
  const { signal } = useFileActionLifetime();
  const scope = JSON.stringify([source, grant, folder]);
  const entry = useRef<string>("");
  const container = useRef<HTMLDivElement>(null);
  const heading = useRef<HTMLHeadingElement>(null);
  const [view, setView] = useState<ViewState>(initial);
  const value = useRef<ViewState>(initial);
  const restore = useRef(false);
  const readEntry = useCallback(() => {
    let map = sessions.get(signal);
    if (!map) {
      map = new Map();
      sessions.set(signal, map);
    }
    for (const [id, record] of map)
      if (record.source === source && record.grant !== grant) map.delete(id);
    const key: unknown = history.state?.fileBrowserEntry;
    const previous = typeof key === "string" ? map.get(key) : undefined;
    entry.current =
      typeof key === "string" && previous?.scope === scope
        ? key
        : crypto.randomUUID();
    const next = previous?.scope === scope ? previous.value : initial;
    map.set(entry.current, { source, grant, scope, value: next });
    if (map.size > 200) map.delete(map.keys().next().value!);
    history.replaceState(
      { ...history.state, fileBrowserEntry: entry.current },
      "",
    );
    value.current = next;
    setView(next);
    restore.current = true;
  }, [source, grant, scope, signal]);
  useLayoutEffect(() => {
    const href = window.location.href;
    readEntry();
    const onPopState = () => {
      const key: unknown = history.state?.fileBrowserEntry;
      if (key === entry.current) return;
      const matchingEntry =
        typeof key === "string" &&
        sessions.get(signal)?.get(key)?.scope === scope;
      if (matchingEntry || window.location.href === href) {
        readEntry();
      }
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [readEntry, scope, signal]);
  function openCurrentFolder() {
    // Search and its containing folder can share a URL but have distinct Back entries.
    history.pushState({ ...history.state, fileBrowserEntry: undefined }, "");
    readEntry();
  }
  const save = useCallback(
    (change: Partial<ViewState>, render = true) => {
      const record = sessions.get(signal)?.get(entry.current);
      if (signal.aborted || record?.scope !== scope) return;
      const next = { ...value.current, ...change };
      value.current = next;
      record.value = next;
      if (render) setView(next);
    },
    [signal, scope],
  );
  useLayoutEffect(() => {
    if (
      !ready ||
      (searchMode !== undefined && searchMode !== Boolean(view.query.trim())) ||
      !restore.current ||
      !container.current ||
      view !== value.current
    )
      return;
    restore.current = false;
    const active = value.current.control;
    const target = active
      ? Array.from(
          container.current.querySelectorAll<HTMLElement>(
            "[data-file-control]",
          ),
        ).find((el) => el.dataset.fileControl === active)
      : undefined;
    (target ?? heading.current)?.focus({ preventScroll: true });
    container.current.scrollTop = value.current.scroll;
  }, [view, scope, ready, searchMode]);
  return { view, save, container, heading, openCurrentFolder };
}
