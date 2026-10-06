import {
  BellIcon,
  BookmarkIcon,
  DownloadIcon,
  FileJsonIcon,
  FileTextIcon,
  PuzzleIcon,
  type LucideIcon,
} from "lucide-react";

import { fetch } from "@/core/api/fetcher";
import { readBoundedArtifactBytes } from "@/core/artifacts/response";
import { getBackendBaseURL } from "@/core/config";

import { importAssetModule } from "./asset-module";
import type {
  FrontendContribution,
  FrontendExtension,
  PluginArtifactSurface,
} from "./contracts";
import { awaitPluginOperation } from "./operations";

export function extensionIcon(name?: string): LucideIcon {
  const icons: Record<string, LucideIcon> = {
    bell: BellIcon,
    bookmark: BookmarkIcon,
    download: DownloadIcon,
    "file-json": FileJsonIcon,
    "file-text": FileTextIcon,
  };
  return name && Object.hasOwn(icons, name) ? icons[name]! : PuzzleIcon;
}
export type LoadedContribution = FrontendContribution & {
  extension?: FrontendExtension;
  error?: string;
};
export type ModuleImporter = (
  url: string,
  signal?: AbortSignal,
) => Promise<{ default: unknown }>;
const importModule: ModuleImporter = (url) =>
  import(/* webpackIgnore: true */ url) as Promise<{ default: unknown }>;

export async function loadFrontendExtensions(
  entries: FrontendContribution[],
  importer: ModuleImporter = importModule,
  assetImporter: ModuleImporter = importAssetModule,
  options: { signal?: AbortSignal; viewerId?: string } = {},
): Promise<LoadedContribution[]> {
  options.signal?.throwIfAborted();
  if (entries.length > 64) throw new Error("Plugin discovery budget exceeded");
  return Promise.all(
    entries.map(async (entry) => {
      if (entry.settings.enabled !== true || entry.module === null)
        return entry;
      const controller = new AbortController();
      const abort = () => controller.abort();
      if (options.signal?.aborted) abort();
      options.signal?.addEventListener("abort", abort, { once: true });
      const timeout = setTimeout(abort, 30_000);
      try {
        controller.signal.throwIfAborted();
        if (
          options.viewerId !== undefined &&
          entry.viewer_id !== options.viewerId
        )
          throw new Error("Plugin descriptor belongs to another account");
        let loadedModule: FrontendExtension;
        const transport = entry.transport ?? "inline-v1";
        if (transport === "assets-v1") {
          const expected = `/api/plugins/${entry.namespace}/assets/`;
          if (
            !entry.entry?.startsWith(expected) ||
            !/^[a-f0-9]{64}\/(?:[A-Za-z0-9_-][A-Za-z0-9_.-]*\/)*[A-Za-z0-9_-][A-Za-z0-9_.-]*\.m?js$/.test(
              entry.entry.slice(expected.length),
            )
          )
            throw new Error("Invalid installed asset entry");
          loadedModule = (
            await awaitPluginOperation(
              assetImporter(
                `${getBackendBaseURL()}${entry.entry}`,
                controller.signal,
              ),
              controller.signal,
            )
          ).default as FrontendExtension;
        } else if (transport === "inline-v1") {
          const expected = `/api/plugins/modules/${entry.module}/`;
          if (
            !entry.entry?.startsWith(expected) ||
            !/^[a-f0-9]{64}\.mjs$/.test(entry.entry.slice(expected.length))
          )
            throw new Error("Invalid installed module entry");
          const response = await awaitPluginOperation(
            fetch(`${getBackendBaseURL()}${entry.entry}`, {
              cache: "no-store",
              signal: controller.signal,
              headers: { Range: `bytes=0-${512 * 1024 - 1}` },
            }),
            controller.signal,
            (late) => {
              void late.body?.cancel().catch(() => undefined);
            },
          );
          if (!response.ok) {
            void response.body?.cancel().catch(() => undefined);
            throw new Error(`Plugin module unavailable (${response.status})`);
          }
          const read = await readBoundedArtifactBytes(
            response,
            512 * 1024,
            controller.signal,
          );
          if (read.truncated)
            throw new Error("Plugin module byte budget exceeded");
          const code = new TextDecoder("utf-8", { fatal: true }).decode(
            read.bytes,
          );
          const moduleURL = URL.createObjectURL(
            new Blob([code], { type: "text/javascript" }),
          );
          try {
            loadedModule = (
              await awaitPluginOperation(
                importer(moduleURL, controller.signal),
                controller.signal,
              )
            ).default as FrontendExtension;
          } finally {
            URL.revokeObjectURL(moduleURL);
          }
        } else {
          throw new Error("Unsupported browser asset transport");
        }
        controller.signal.throwIfAborted();
        if (
          loadedModule?.apiVersion !== 1 ||
          loadedModule.module !== entry.module ||
          (loadedModule.conversationActions !== undefined &&
            typeof loadedModule.conversationActions !== "function")
        )
          throw new Error("Incompatible browser extension");
        if (loadedModule.surfaces !== undefined) {
          const seen = new Set<string>();
          if (
            !Array.isArray(loadedModule.surfaces) ||
            loadedModule.surfaces.length > 16
          )
            throw new Error("Invalid plugin surfaces");
          for (const surface of loadedModule.surfaces) {
            if (
              !surface ||
              !/^[a-z][a-z0-9-]{0,63}$/.test(surface.id) ||
              seen.has(surface.id) ||
              surface.slot !== "page" ||
              typeof surface.title !== "string" ||
              !surface.title.trim() ||
              typeof surface.mount !== "function"
            )
              throw new Error("Invalid plugin surface");
            if (surface.navigation !== undefined) {
              const nav = surface.navigation;
              if (
                surface.slot !== "page" ||
                !nav ||
                typeof nav.label !== "string" ||
                !nav.label.trim() ||
                nav.label.length > 120 ||
                (nav.labelZh !== undefined &&
                  (typeof nav.labelZh !== "string" ||
                    !nav.labelZh.trim() ||
                    nav.labelZh.length > 120)) ||
                (nav.icon !== undefined && typeof nav.icon !== "string")
              )
                throw new Error("Invalid plugin navigation");
            }
            seen.add(surface.id);
          }
        }
        const { fileCollection, fileFilingApiVersion } = loadedModule;
        if (
          fileCollection !== undefined ||
          fileFilingApiVersion !== undefined
        ) {
          if (
            fileFilingApiVersion !== 1 ||
            typeof fileCollection !== "function"
          )
            throw new Error("Unsupported file filing capability");
          loadedModule = Object.create(Object.getPrototypeOf(loadedModule), {
            ...Object.getOwnPropertyDescriptors(loadedModule),
            fileCollection: {
              value: fileCollection,
              enumerable: true,
            },
            fileFilingApiVersion: { value: 1, enumerable: true },
          }) as FrontendExtension;
        }
        const artifactSurfaces = loadedModule.artifacts;
        if (artifactSurfaces !== undefined) {
          if (
            loadedModule.artifactApiVersion !== 1 ||
            !Array.isArray(artifactSurfaces) ||
            artifactSurfaces.length > 16
          )
            throw new Error("Unsupported artifact presentation capability");
          const seen = new Set<string>();
          const installed = new Set(
            entry.artifact_presentations?.map((item) => item.id),
          );
          const snapshots: PluginArtifactSurface[] = [];
          for (const surface of artifactSurfaces) {
            if (!surface)
              throw new Error("Invalid artifact presentation contribution");
            const { id, title, kind } = surface;
            const handler =
              kind === "native"
                ? surface.mount
                : kind === "passive"
                  ? surface.present
                  : undefined;
            if (
              typeof id !== "string" ||
              !/^[a-z][a-z0-9-]{0,63}$/.test(id) ||
              seen.has(id) ||
              !installed.has(id) ||
              typeof title !== "string" ||
              !title.trim() ||
              title.length > 256 ||
              typeof handler !== "function"
            )
              throw new Error("Invalid artifact presentation contribution");
            seen.add(id);
            snapshots.push(
              Object.freeze(
                kind === "native"
                  ? {
                      id,
                      title,
                      kind,
                      mount: handler as Extract<
                        PluginArtifactSurface,
                        { kind: "native" }
                      >["mount"],
                    }
                  : {
                      id,
                      title,
                      kind: "passive",
                      present: handler as Extract<
                        PluginArtifactSurface,
                        { kind: "passive" }
                      >["present"],
                    },
              ),
            );
          }
          // Preserve page-only module semantics while replacing the new
          // capability with validated values and immutable callable snapshots.
          loadedModule = Object.create(Object.getPrototypeOf(loadedModule), {
            ...Object.getOwnPropertyDescriptors(loadedModule),
            artifacts: { value: Object.freeze(snapshots), enumerable: true },
            artifactApiVersion: { value: 1, enumerable: true },
          }) as FrontendExtension;
        }
        return { ...entry, extension: loadedModule };
      } catch (error) {
        options.signal?.throwIfAborted();
        console.warn(`Browser extension ${entry.module} unavailable`, error);
        return {
          ...entry,
          extension: undefined,
          error: "Module failed to load. Reload the page to retry.",
        };
      } finally {
        clearTimeout(timeout);
        options.signal?.removeEventListener("abort", abort);
      }
    }),
  );
}
export function activeFrontendExtensions(entries: LoadedContribution[]) {
  return entries.flatMap((contribution) =>
    contribution.settings.enabled === true && contribution.extension
      ? [{ contribution, extension: contribution.extension }]
      : [],
  );
}
