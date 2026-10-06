import { describe, expect, it } from "@rstest/core";

import { artifactPresentationFor } from "@/core/extensions/artifacts";
import type { LoadedContribution } from "@/core/extensions/registry";

const descriptor = {
  id: "summary",
  suffixes: [".summary.json"],
  source_max_bytes: 1024 * 1024,
  preview_max_bytes: 1024 * 1024,
  projection_marker: null,
};
const native = {
  id: "summary",
  title: "Summary",
  kind: "native" as const,
  mount: () => ({ dispose: () => undefined }),
};
const entry: LoadedContribution = {
  namespace: "example.summary",
  viewer_id: "viewer-1",
  module: "summary.v1",
  entry: `/api/plugins/modules/summary.v1/${"a".repeat(64)}.mjs`,
  settings: { enabled: true },
  title: "Summary",
  description: "",
  artifact_presentations: [descriptor],
  extension: {
    apiVersion: 1,
    module: "summary.v1",
    artifactApiVersion: 1,
    artifacts: [native],
  },
};

describe("installed artifact contribution selection", () => {
  it("selects only an enabled installed matching handler", () => {
    const selected = artifactPresentationFor(
      [entry],
      "/mnt/user-data/outputs/a.summary.json",
    );
    expect(selected?.contribution.namespace).toBe("example.summary");
    expect(selected?.surface).toBe(native);
    expect(selected?.descriptor).toBe(descriptor);
    expect(
      artifactPresentationFor(
        [{ ...entry, settings: { enabled: false } }],
        "a.summary.json",
      ),
    ).toBeUndefined();
    expect(
      artifactPresentationFor(
        [{ ...entry, extension: undefined }],
        "a.summary.json",
      ),
    ).toBeUndefined();
    expect(artifactPresentationFor([entry], "a.json")).toBeUndefined();
  });
  it("keeps passive files and ambiguous handler ownership on their ordinary host path", () => {
    expect(artifactPresentationFor([entry], "a.view.json")).toBeUndefined();
    expect(
      artifactPresentationFor(
        [entry, { ...entry, namespace: "other.summary" }],
        "a.summary.json",
      ),
    ).toBeUndefined();
  });
  it("does not let a browser module declare an unregistered source handler", () => {
    expect(
      artifactPresentationFor(
        [{ ...entry, artifact_presentations: [] }],
        "a.summary.json",
      ),
    ).toBeUndefined();
    expect(
      artifactPresentationFor(
        [
          {
            ...entry,
            extension: {
              ...entry.extension!,
              artifacts: [{ ...native, id: "unregistered" }],
            },
          },
        ],
        "a.summary.json",
      ),
    ).toBeUndefined();
  });
});
