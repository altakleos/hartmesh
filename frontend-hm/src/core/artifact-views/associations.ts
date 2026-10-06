import {
  isArtifactViewPath,
  isArtifactViewRevision,
  isSafeViewReference,
  resolveViewReference,
  type ArtifactViewDocument,
} from "./contract";

export const SOURCE_VIEW_MAX_CANDIDATES = 8;
export const SOURCE_VIEW_MAX_READERS = 2;
export type SourceView = {
  filepath: string;
  revision: string;
  view: ArtifactViewDocument;
};

function localFile(path: string) {
  return (
    path.startsWith("/mnt/user-data/") && isSafeViewReference(path.slice(1))
  );
}
function parent(path: string) {
  return path.slice(0, path.lastIndexOf("/") + 1);
}

/** Discover named candidates from presentations only; never expand a directory. */
export function associationCandidates(
  source: string,
  presented: readonly string[],
): {
  paths: string[];
  overBudget: boolean;
} {
  if (
    !localFile(source) ||
    isArtifactViewPath(source) ||
    !presented.some((path) => {
      const candidate = path.replace(/\/$/, "");
      return source === candidate || source.startsWith(`${candidate}/`);
    })
  )
    return { paths: [], overBudget: false };
  const paths = [
    ...new Set(
      presented.filter(
        (path) =>
          localFile(path) &&
          isArtifactViewPath(path) &&
          parent(path) === parent(source),
      ),
    ),
  ].sort();
  return paths.length > SOURCE_VIEW_MAX_CANDIDATES
    ? { paths: [], overBudget: true }
    : { paths, overBudget: false };
}

/** A display relation is one hop to one distinct ordinary sibling, never authority. */
export function chooseSourceView(
  source: string,
  candidates: readonly SourceView[],
): SourceView | null {
  if (!localFile(source) || isArtifactViewPath(source)) return null;
  const matches = candidates.filter(
    (candidate) =>
      candidate.filepath !== source &&
      localFile(candidate.filepath) &&
      isArtifactViewPath(candidate.filepath) &&
      parent(candidate.filepath) === parent(source) &&
      candidate.view.primary_source !== undefined &&
      resolveViewReference(
        candidate.filepath,
        candidate.view.primary_source.path,
      ) === source,
  );
  return matches.length === 1 && isArtifactViewRevision(matches[0]!.revision)
    ? matches[0]!
    : null;
}
