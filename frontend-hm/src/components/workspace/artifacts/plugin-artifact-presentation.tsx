"use client";

import { useTheme } from "next-themes";
import { useEffect, useRef, useState } from "react";

import {
  isArtifactViewRevision,
  parseArtifactView,
  resolveViewReference,
  type ArtifactViewDocument,
} from "@/core/artifact-views/contract";
import { ArtifactImageSession } from "@/core/artifact-views/images";
import type { InstalledArtifactPresentation } from "@/core/extensions/artifacts";
import type { ArtifactSurfaceContext } from "@/core/extensions/contracts";
import { useFrontendServices } from "@/core/extensions/hooks";
import { awaitPluginOperation } from "@/core/extensions/operations";
import { bindFrontendServices } from "@/core/extensions/services";
import { mountSurface } from "@/core/extensions/surfaces";
import { useI18n } from "@/core/i18n/hooks";

import { ArtifactFileControls } from "./artifact-file-controls";
import { ArtifactView } from "./artifact-view";

type Props = {
  installation: InstalledArtifactPresentation;
  filepath: string;
  threadId: string;
  content: string;
  revision: string;
  projected: boolean;
  artifacts: readonly string[];
  presentedKnown: boolean;
  runSettled: boolean;
  viewerId: string;
  isMock?: boolean;
  onUnavailable: () => void;
};

export function PluginArtifactPresentation(props: Props) {
  const identity = JSON.stringify([
    props.viewerId,
    props.threadId,
    props.filepath,
    props.revision,
    props.installation.contribution.namespace,
    props.installation.contribution.entry,
    props.installation.surface.id,
    props.installation.surface.kind,
  ]);
  return <PresentationSession key={identity} {...props} />;
}

function PresentationSession(props: Props) {
  const { t, locale } = useI18n();
  const { resolvedTheme } = useTheme();
  const theme = resolvedTheme === "dark" ? "dark" : "light";
  const root = useRef<HTMLDivElement>(null);
  const services = useFrontendServices();
  const current = useRef({ services, onUnavailable: props.onUnavailable });
  current.current = { services, onUnavailable: props.onUnavailable };
  const [view, setView] = useState<ArtifactViewDocument | null>(null);
  const [files, setFiles] = useState<ArtifactViewDocument | null>(null);
  const {
    installation,
    filepath,
    threadId,
    revision,
    content,
    projected,
    viewerId,
    artifacts,
    isMock,
  } = props;
  const presentedIdentity = JSON.stringify(artifacts);

  useEffect(() => {
    const controller = new AbortController();
    const images = new ArtifactImageSession();
    let active = true;
    let failed = false;
    let cleanup: () => void = () => undefined;
    const fail = () => {
      if (!active || failed) return;
      failed = true;
      controller.abort();
      cleanup();
      images.dispose();
      current.current.onUnavailable();
    };
    setView(null);
    setFiles(null);
    if (
      !isArtifactViewRevision(revision) ||
      installation.contribution.viewer_id !== viewerId
    ) {
      fail();
      return () => {
        active = false;
        images.dispose();
      };
    }
    const artifact = Object.freeze({
      filepath,
      threadId,
      content,
      revision,
      projected,
      presented: Object.freeze([...artifacts]),
    });
    const base = bindFrontendServices(
      current.current.services,
      installation.contribution,
      controller.signal,
    );
    const context: ArtifactSurfaceContext = {
      namespace: installation.contribution.namespace,
      settings: installation.contribution.settings,
      locale,
      threadId,
      signal: controller.signal,
      callBackend: base.callBackend,
      artifact,
      theme,
      async loadRaster(relative) {
        controller.signal.throwIfAborted();
        const resource = resolveViewReference(filepath, relative);
        if (!resource) throw new Error("Invalid local raster reference");
        return images.load({
          filepath: resource,
          threadId,
          revision,
          isMock,
          signal: controller.signal,
        });
      },
    };
    const surface = installation.surface;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    if (surface.kind === "passive") {
      timeout = setTimeout(fail, 30_000);
      try {
        void awaitPluginOperation(
          Promise.resolve(surface.present(context)),
          controller.signal,
        )
          .then((value) => {
            controller.signal.throwIfAborted();
            const parsed = parseArtifactView(JSON.stringify(value));
            if (!parsed) throw new Error("Unsupported passive presentation");
            if (active) setView(parsed);
            clearTimeout(timeout);
          })
          .catch(fail);
      } catch {
        fail();
      }
    } else if (root.current) {
      let nativeController:
        | {
            dispose: () => void;
            files?: {
              exports: { path: string; label: string }[];
              collection?: string;
            };
          }
        | undefined;
      cleanup = mountSurface(
        root.current,
        {
          id: surface.id,
          title: surface.title,
          slot: "page",
          mount(container, mounted) {
            // Only the existing lifecycle helper is reused; this is never a page registration.
            nativeController = surface.mount(container, {
              ...context,
              signal: mounted.signal,
              callBackend: mounted.callBackend,
            });
            return nativeController;
          },
        },
        context,
        fail,
      );
      if (failed) cleanup();
      if (nativeController && !failed) {
        try {
          const controls = nativeController.files;
          if (!controls)
            return () => {
              active = false;
              controller.abort();
              cleanup();
              images.dispose();
            };
          const parsed = parseArtifactView(
            JSON.stringify({
              format: "hartmesh.artifact-view",
              version: 1,
              title: surface.title,
              blocks: [],
              exports: controls.exports,
              ...(controls.collection === undefined
                ? {}
                : { destination: { collection: controls.collection } }),
            }),
          );
          if (!parsed) throw new Error("Invalid native file controls");
          setFiles(parsed);
        } catch {
          fail();
        }
      }
    }
    return () => {
      active = false;
      clearTimeout(timeout);
      controller.abort();
      cleanup();
      images.dispose();
    };
    // The recorded set is a mount input. New SDK array objects with the same
    // paths must not retire a presentation on every streamed message chunk.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    installation,
    filepath,
    threadId,
    revision,
    content,
    projected,
    viewerId,
    presentedIdentity,
    isMock,
    locale,
    theme,
  ]);

  const fileProps = {
    filepath,
    threadId,
    revision,
    viewerId,
    artifacts,
    isMock,
    presentedKnown: props.presentedKnown,
    runSettled: props.runSettled,
  };
  if (installation.surface.kind === "passive")
    return view ? (
      <ArtifactView {...fileProps} view={view} />
    ) : (
      <p role="status">{t.extensions.pageLoading}</p>
    );
  return (
    <section
      className="h-full min-w-0 overflow-y-auto"
      aria-label={installation.surface.title}
      data-testid="plugin-artifact-presentation"
    >
      <div ref={root} />
      {files && files.exports.length > 0 && (
        <div className="p-4">
          <ArtifactFileControls {...fileProps} view={files} />
        </div>
      )}
    </section>
  );
}
