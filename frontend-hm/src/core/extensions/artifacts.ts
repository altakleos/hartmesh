import { isArtifactViewPath } from "@/core/artifact-views/contract";

import type {
  ArtifactPresentationDescriptor,
  PluginArtifactSurface,
} from "./contracts";
import { activeFrontendExtensions, type LoadedContribution } from "./registry";

export type InstalledArtifactPresentation = {
  contribution: LoadedContribution;
  descriptor: ArtifactPresentationDescriptor;
  surface: PluginArtifactSurface;
};

/** Selection uses only the installed snapshot. Producer files cannot choose code. */
export function artifactPresentationFor(
  entries: LoadedContribution[],
  filepath: string,
): InstalledArtifactPresentation | undefined {
  if (isArtifactViewPath(filepath)) return;
  const matches: InstalledArtifactPresentation[] = [];
  for (const { contribution, extension } of activeFrontendExtensions(entries)) {
    if (extension.artifactApiVersion !== 1) continue;
    for (const descriptor of contribution.artifact_presentations ?? []) {
      if (
        !descriptor.suffixes.some((suffix) =>
          filepath.toLowerCase().endsWith(suffix),
        )
      )
        continue;
      const surface = extension.artifacts?.find(
        (item) => item.id === descriptor.id,
      );
      if (surface) matches.push({ contribution, descriptor, surface });
    }
  }
  // Refuse ambiguity, including two declarations within the same package.
  return matches.length === 1 ? matches[0] : undefined;
}
