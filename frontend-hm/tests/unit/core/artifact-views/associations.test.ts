import { expect, test } from "@rstest/core";

import {
  associationCandidates,
  chooseSourceView,
} from "@/core/artifact-views/associations";
import { parseArtifactView } from "@/core/artifact-views/contract";

const SOURCE = "/mnt/user-data/outputs/results/source.json";
const VIEW = "/mnt/user-data/outputs/results/result.view.json";
const document = (source = "source.json") =>
  parseArtifactView(
    JSON.stringify({
      format: "hartmesh.artifact-view",
      version: 1,
      title: "Result",
      primary_source: { path: source },
      blocks: [],
      exports: [],
    }),
  );

test("discovery uses only explicit already-presented views in the ordinary source directory", () => {
  expect(
    associationCandidates(SOURCE, [
      SOURCE,
      VIEW,
      VIEW,
      "/mnt/user-data/outputs/elsewhere/other.view.json",
      "/mnt/user-data/outputs/results/nested/child.view.json",
      "/mnt/user-data/outputs/results/",
    ]),
  ).toEqual({ paths: [VIEW], overBudget: false });
  expect(associationCandidates(SOURCE, [VIEW]).paths).toEqual([]);
  expect(associationCandidates(VIEW, [SOURCE, VIEW]).paths).toEqual([]);
});

test("candidate budget refuses a partial set instead of choosing a hidden winner", () => {
  const views = Array.from(
    { length: 9 },
    (_, index) => `/mnt/user-data/outputs/results/${index}.view.json`,
  );
  expect(associationCandidates(SOURCE, [SOURCE, ...views])).toEqual({
    paths: [],
    overBudget: true,
  });
});

test("a distinct unique sibling relation retains its own source-view identity", () => {
  const view = document()!;
  const match = { filepath: VIEW, revision: "a".repeat(64), view };
  expect(chooseSourceView(SOURCE, [match])).toEqual(match);
  expect(
    chooseSourceView(SOURCE, [{ ...match, view: document("other.json")! }]),
  ).toBeNull();
  expect(
    chooseSourceView(SOURCE, [
      {
        ...match,
        filepath: "/mnt/user-data/outputs/elsewhere/result.view.json",
      },
    ]),
  ).toBeNull();
  expect(chooseSourceView(SOURCE, [{ ...match, revision: "weak" }])).toBeNull();
  expect(
    chooseSourceView(SOURCE, [
      match,
      {
        ...match,
        filepath: "/mnt/user-data/outputs/results/another.view.json",
        revision: "weak",
      },
    ]),
  ).toBeNull();
});

test("duplicate sources and self/view/descendant relations never form a rich association", () => {
  const match = { filepath: VIEW, revision: "a".repeat(64), view: document()! };
  expect(
    chooseSourceView(SOURCE, [
      match,
      {
        ...match,
        filepath: "/mnt/user-data/outputs/results/another.view.json",
      },
    ]),
  ).toBeNull();
  expect(document("result.view.json")).toBeNull();
  expect(document("nested/source.json")).toBeNull();
  expect(chooseSourceView(VIEW, [match])).toBeNull();
});
