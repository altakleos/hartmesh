import type { Model } from "./types";

/** One selection policy for the picker, persisted settings and a new run. */
export function selectAvailableModel(
  models: Model[],
  currentName: string | undefined,
  defaultName?: string | null,
): Model | undefined {
  return (
    models.find((model) => model.name === currentName) ??
    models.find((model) => model.name === defaultName) ??
    models[0]
  );
}
