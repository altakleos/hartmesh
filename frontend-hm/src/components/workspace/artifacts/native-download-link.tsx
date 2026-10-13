"use client";

import { useEffect, useRef, useState, type ComponentProps } from "react";

import { useAuth } from "@/core/auth/AuthProvider";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { observeNativeDownload } from "@/core/file-areas/native-download";
import { useStorageCopy } from "@/core/file-areas/storage-copy";
import {
  readStorageRefusal,
  type StorageRefusal,
} from "@/core/file-areas/storage-state";

/** Ordinary anchors remain available for modified clicks and inconclusive reads. */
export function NativeDownloadLink({
  area,
  onGlobalFailure,
  href,
  onClick,
  ...props
}: ComponentProps<"a"> & {
  href: string;
  area: "files" | "shared";
  onGlobalFailure: (refusal: StorageRefusal) => void;
}) {
  const { user } = useAuth();
  const identity = `${user?.id ?? ""}:${user?.system_role ?? ""}`;
  const lifetime = useFileActionLifetime();
  const observer = useRef<AbortController | null>(null);
  const [fallback, setFallback] = useState<string | null>(null);
  const fallbackKey = `${identity}:${href}`;
  const copy = useStorageCopy(area);
  useEffect(() => {
    const mounted = new AbortController();
    observer.current = mounted;
    return () => mounted.abort();
  }, [identity, href]);
  return (
    <a
      {...props}
      href={href}
      onClick={(event) => {
        onClick?.(event);
        const observed = observer.current?.signal;
        const signal = lifetime.signal;
        if (
          event.defaultPrevented ||
          event.button !== 0 ||
          event.metaKey ||
          event.ctrlKey ||
          event.shiftKey ||
          event.altKey ||
          fallback === fallbackKey ||
          !lifetime.active ||
          !observed ||
          observed.aborted
        )
          return;
        if (
          observeNativeDownload({
            url: href,
            signal,
            onInconclusive: () => {
              if (!observed.aborted && !signal.aborted)
                setFallback(fallbackKey);
            },
            onRefusal: (body) => {
              if (observed.aborted || signal.aborted) return;
              const refusal = readStorageRefusal(body);
              if (!refusal) {
                setFallback(fallbackKey);
                return;
              }
              lifetime.toasts.add(
                copy.error(body, copy.t.chat.filesUnavailable),
              );
              if (refusal.code === "storage-unavailable")
                onGlobalFailure(refusal);
            },
          })
        )
          event.preventDefault();
      }}
    />
  );
}
