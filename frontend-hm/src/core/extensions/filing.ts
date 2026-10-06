import { viewCollection } from "@/core/artifact-views/contract";

import type { FileCollectionContext } from "./contracts";
import { activeFrontendExtensions, type LoadedContribution } from "./registry";

/** A failed module may own a filing policy; uncertainty must not silently file at root. */
export function isFileFilingReady(query: {
  isPending?: boolean;
  isError?: boolean;
  data?: LoadedContribution[];
}) {
  return (
    !query.isPending &&
    !query.isError &&
    !query.data?.some((item) => item.error)
  );
}

/** A collection hint is never a path, an eligibility grant, or a directory copy. */
export function installedFileCollection(
  entries: LoadedContribution[],
  input: FileCollectionContext,
): string | undefined {
  const context = Object.freeze({
    ...input,
    presented: Object.freeze([...input.presented]),
  });
  const decisions: string[] = [];
  for (const { extension } of activeFrontendExtensions(entries)) {
    if (extension.fileFilingApiVersion !== 1 || !extension.fileCollection)
      continue;
    try {
      const answer = extension.fileCollection(context);
      if (answer && typeof (answer as { then?: unknown }).then === "function") {
        // Installed callbacks must be synchronous. Observe malformed late rejections.
        void Promise.resolve(answer).catch(() => undefined);
        return;
      }
      if (answer === null) continue;
      if (
        !answer ||
        typeof answer.collection !== "string" ||
        answer.collection.length > 64
      )
        return;
      const hint = viewCollection({ destination: answer });
      if (hint.ignored || !hint.collection) return;
      decisions.push(hint.collection);
    } catch {
      return;
    }
  }
  return decisions.length === 1 ? decisions[0] : undefined;
}
