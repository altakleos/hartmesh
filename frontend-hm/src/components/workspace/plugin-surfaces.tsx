"use client";

import { useRouter } from "next/navigation";
import { useEffect, useRef, useState } from "react";

import { useAuth } from "@/core/auth/AuthProvider";
import type {
  PluginSurface,
  SurfaceSlot,
  StorageResourceContext,
} from "@/core/extensions/contracts";
import {
  useFrontendExtensions,
  useFrontendServices,
} from "@/core/extensions/hooks";
import {
  activeFrontendExtensions,
  type LoadedContribution,
} from "@/core/extensions/registry";
import {
  bindFrontendServices,
  openConversation,
} from "@/core/extensions/services";
import { mountSurface } from "@/core/extensions/surfaces";
import { useI18n } from "@/core/i18n/hooks";
import { getSpace } from "@/core/spaces/api";

function Surface({
  entry,
  surface,
  threadId,
  resourceId,
}: {
  entry: LoadedContribution;
  surface: PluginSurface;
  threadId?: string;
  resourceId?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const router = useRouter();
  const { locale, t } = useI18n();
  const { user } = useAuth();
  const services = useFrontendServices();
  const currentServices = useRef(services);
  currentServices.current = services;
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    if (!ref.current) return;
    setFailed(false);
    const abort = new AbortController();
    let cleanup: (() => void) | undefined;
    const mount = (resource?: StorageResourceContext) => {
      if (abort.signal.aborted || !ref.current) return;
      cleanup = mountSurface(
        ref.current,
        surface,
        {
          namespace: entry.namespace,
          locale,
          settings: entry.settings,
          threadId,
          resource,
          storageCapabilities: entry.storage_capabilities ?? undefined,
          openConversation: (id, signal) =>
            openConversation(id, (path) => router.push(path), signal),
          callBackend: bindFrontendServices(
            currentServices.current,
            entry,
            abort.signal,
            resource?.id,
          ).callBackend,
        },
        () => setFailed(true),
      );
    };
    if (resourceId !== undefined) {
      if (
        entry.extension?.resourceApiVersion !== 1 ||
        entry.storage_api_version !== 1 ||
        !entry.storage_capabilities?.available
      ) {
        setFailed(true);
      } else {
        void getSpace(resourceId, abort.signal)
          .then((space) => {
            if (abort.signal.aborted) return;
            mount(
              Object.freeze({
                id: space.id,
                name: space.name,
                mode: space.mode,
                generation: space.generation,
                permissions: space.permissions,
              }),
            );
          })
          .catch(() => {
            if (!abort.signal.aborted) setFailed(true);
          });
      }
    } else mount();
    return () => {
      abort.abort();
      cleanup?.();
    };
  }, [entry, surface, locale, threadId, resourceId, user?.id, router]);
  return (
    <section aria-label={surface.title}>
      {failed && <p role="alert">{t.extensions.viewFailed}</p>}
      <div ref={ref} />
    </section>
  );
}

export function PluginSurfaces({
  slot,
  namespace,
  surfaceId,
  threadId,
  resourceId,
}: {
  slot: SurfaceSlot;
  namespace?: string;
  surfaceId?: string;
  threadId?: string;
  resourceId?: string;
}) {
  const query = useFrontendExtensions();
  const { user } = useAuth();
  return activeFrontendExtensions(query.data ?? [])
    .filter(
      ({ contribution }) => !namespace || contribution.namespace === namespace,
    )
    .flatMap(({ contribution: entry, extension }) =>
      (extension.surfaces ?? [])
        .filter(
          (surface) =>
            surface.slot === slot && (!surfaceId || surface.id === surfaceId),
        )
        .map((surface) => (
          <Surface
            key={`${user?.id}:${threadId}:${resourceId}:${entry.namespace}:${entry.entry}:${surface.id}`}
            entry={entry}
            surface={surface}
            threadId={threadId}
            resourceId={resourceId}
          />
        )),
    );
}
