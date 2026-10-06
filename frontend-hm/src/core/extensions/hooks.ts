"use client";
import { useQuery } from "@tanstack/react-query";
import { toast } from "sonner";

import { useAuth } from "@/core/auth/AuthProvider";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { isStaticWebsiteOnly } from "@/core/static-mode";

import { fetchFrontendExtensions, frontendExtensionsQueryKey } from "./api";
import { loadFrontendExtensions } from "./registry";
import { conversationText, latestVisibleAnswer } from "./services";

export function useFrontendExtensions() {
  const { user } = useAuth();
  return useQuery({
    queryKey: [...frontendExtensionsQueryKey, user?.id],
    queryFn: async ({ signal }) => {
      const entries = await fetchFrontendExtensions(signal);
      signal.throwIfAborted();
      return loadFrontendExtensions(entries, undefined, undefined, {
        signal,
        viewerId: user?.id,
      });
    },
    enabled: !!user && !isStaticWebsiteOnly(),
    staleTime: Infinity,
    gcTime: 0,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    retry: false,
  });
}
/** Eagerly capture the page's plugin set, even before its first chat is opened. */
export function ExtensionPageBootstrap() {
  useFrontendExtensions();
  return null;
}
export function useFrontendServices() {
  const lifetime = useFileActionLifetime();
  return {
    conversationText,
    latestVisibleAnswer,
    showMessage: (message: string) => {
      if (!lifetime.active || lifetime.signal.aborted)
        throw new DOMException("Plugin message retired", "AbortError");
      lifetime.toasts.add(toast.message(message));
    },
  };
}
