import { readFileSync } from "node:fs";

import { describe, expect, it } from "@rstest/core";

import {
  eligibleViewExports,
  isArtifactViewPath,
  isArtifactViewRevision,
  parseArtifactView,
  recordedPresentedPaths,
  resolveViewReference,
  viewCollection,
} from "@/core/artifact-views/contract";

const PATH = "/mnt/user-data/outputs/results/summary.view.json";
const base = {
  format: "hartmesh.artifact-view",
  version: 1,
  title: "Summary",
  blocks: [],
  exports: [],
};
const parse = (value: unknown) => parseArtifactView(JSON.stringify(value));

describe("passive artifact view v1", () => {
  it("requires a strong observed revision rather than a collision-prone fallback", () => {
    expect(isArtifactViewRevision("a".repeat(64))).toBe(true);
    expect(isArtifactViewRevision("f93912e6")).toBe(false);
    expect(isArtifactViewRevision(undefined)).toBe(false);
  });
  it.each(["comparison", "procedure", "summary"])(
    "accepts the shared %s example without a domain parser",
    (name) => {
      const bytes = readFileSync(
        `../contracts/artifact_view/examples/${name}.view.json`,
        "utf-8",
      );
      expect(parseArtifactView(bytes)).not.toBeNull();
    },
  );
  it("uses recorded presentation rather than incidental discovery or an unanswered request", () => {
    expect(
      recordedPresentedPaths([
        { type: "human", additional_kwargs: { presented_files: ["spoofed"] } },
        { type: "ai", additional_kwargs: { other: true } },
        { type: "tool", additional_kwargs: { presented_files: ["a", 3, "b"] } },
        { type: "ai", additional_kwargs: { presented_files: ["b", "c"] } },
      ]),
    ).toEqual(["a", "b", "c"]);
  });
  it("accepts the six primitives and preformatted strings", () => {
    const view = parse({
      ...base,
      subtitle: "Prepared from supplied documents",
      accent: "#aBc123",
      primary_source: { path: "summary.json" },
      destination: { collection: "Summaries" },
      blocks: [
        {
          type: "facts",
          items: [{ label: "Total", value: "$52,310.40", detail: "+6.4%" }],
        },
        {
          type: "text",
          heading: "Scope",
          paragraphs: ["<script>literal text</script>"],
        },
        { type: "list", ordered: true, items: ["First", "Second"] },
        {
          type: "table",
          columns: [{ label: "Name" }, { label: "Value", align: "end" }],
          rows: [["A", "$10"]],
          footer: ["Total", "$10"],
        },
        {
          type: "image",
          path: "images/chart.png",
          alt: "Comparison",
          caption: "From supplied data",
        },
        {
          type: "notice",
          tone: "warning",
          text: "Review the source",
          attribution: "Document skill",
        },
      ],
      exports: [{ path: "summary.pdf", label: "Printable summary" }],
    });
    expect(view?.blocks).toHaveLength(6);
    expect(view?.exports[0]?.label).toBe("Printable summary");
  });

  it.each([
    { ...base, version: 2 },
    { ...base, version: true },
    { ...base, format: "another-format" },
    { ...base, onClick: "run" },
    { ...base, module: "https://example.test/code.js" },
    { ...base, title: "" },
    { ...base, title: "x".repeat(257) },
    { ...base, accent: "#123456\n" },
    { ...base, accent: "red;display:none" },
    { ...base, destination: { collection: "x", extra: true } },
    { ...base, destination: { collection: "x".repeat(65) } },
    { ...base, primary_source: { path: "nested/source.json" } },
    { ...base, primary_source: { path: "another.VIEW.JSON" } },
    { ...base, blocks: [{ type: "script", code: "alert(1)" }] },
    {
      ...base,
      blocks: [{ type: "text", paragraphs: ["x"], css: "color:red" }],
    },
    {
      ...base,
      blocks: [{ type: "facts", items: [{ label: "Amount", value: 12 }] }],
    },
    { ...base, blocks: [{ type: "notice", tone: "verified", text: "x" }] },
    {
      ...base,
      blocks: [{ type: "image", path: "https://example.test/x.png", alt: "x" }],
    },
    {
      ...base,
      blocks: [
        { type: "table", columns: [{ label: "A" }], rows: [["x", "y"]] },
      ],
    },
    {
      ...base,
      blocks: [
        {
          type: "table",
          columns: [{ label: "A" }],
          rows: [],
          footer: ["x", "y"],
        },
      ],
    },
    {
      ...base,
      exports: [
        { path: "a.pdf", label: "A" },
        { path: "a.pdf", label: "Again" },
      ],
    },
    {
      ...base,
      exports: Array.from({ length: 17 }, (_, index) => ({
        path: `${index}.pdf`,
        label: "PDF",
      })),
    },
    {
      ...base,
      blocks: Array.from({ length: 33 }, () => ({
        type: "image",
        path: "x.png",
        alt: "x",
      })),
    },
    {
      ...base,
      blocks: Array.from({ length: 65 }, () => ({
        type: "text",
        paragraphs: ["x"],
      })),
    },
  ])("rejects unsupported or structurally invalid documents %#", (value) => {
    expect(parse(value)).toBeNull();
  });

  it("bounds aggregate table cells, including headers and footers", () => {
    const table = {
      type: "table",
      columns: Array.from({ length: 12 }, () => ({ label: "Column" })),
      rows: Array.from({ length: 200 }, () =>
        Array.from({ length: 12 }, () => "value"),
      ),
    };
    expect(parse({ ...base, blocks: [table, table] })).not.toBeNull();
    expect(
      parse({
        ...base,
        blocks: [table, table, { ...table, rows: table.rows.slice(0, 15) }],
      }),
    ).toBeNull();
  });

  it("counts Unicode characters and serialized UTF-8 bytes separately", () => {
    expect(parse({ ...base, title: "🙂".repeat(256) })).not.toBeNull();
    expect(
      parseArtifactView(" ".repeat(1024 * 1024) + JSON.stringify(base)),
    ).toBeNull();
    expect(parseArtifactView("{truncated")).toBeNull();
  });

  it.each([
    "/absolute.png",
    "../x.png",
    "a/../x.png",
    "./x.png",
    "a//b",
    ".hidden.png",
    "a/.hidden/b",
    "a\\b",
    "a%20b",
    "a:b",
    "a?x",
    "a#x",
    "a\u0000b",
    "a\u007fb",
    "a\ud800b",
    "a\udc00b",
    "",
  ])("refuses unsafe resource references %j", (path) => {
    expect(resolveViewReference(PATH, path)).toBeNull();
    expect(parse({ ...base, exports: [{ path, label: "Export" }] })).toBeNull();
  });

  it("confines resources to the view directory and preserves exact filenames", () => {
    expect(resolveViewReference(PATH, "🙂.pdf")).not.toBeNull();
    expect(resolveViewReference(PATH, "charts/a b.png")).toBe(
      "/mnt/user-data/outputs/results/charts/a b.png",
    );
    expect(
      resolveViewReference("https://example.test/a.view.json", "x.png"),
    ).toBeNull();
    expect(isArtifactViewPath(PATH)).toBe(true);
    expect(isArtifactViewPath("summary.json")).toBe(false);
  });

  it("offers only explicit presented exports, with bounded directory coverage", () => {
    const view = parse({
      ...base,
      primary_source: { path: "source.xlsx" },
      blocks: [{ type: "image", path: "chart.png", alt: "Chart" }],
      exports: [
        { path: "source.xlsx", label: "Source workbook" },
        { path: "nested/a.pdf", label: "PDF" },
        { path: "unpresented.pdf", label: "Unavailable" },
      ],
    })!;
    expect(
      eligibleViewExports(view, PATH, [
        "/mnt/user-data/outputs/results/source.xlsx",
        "/mnt/user-data/outputs/results/nested/",
      ]),
    ).toEqual([
      {
        path: "/mnt/user-data/outputs/results/source.xlsx",
        label: "Source workbook",
      },
      { path: "/mnt/user-data/outputs/results/nested/a.pdf", label: "PDF" },
    ]);
    expect(
      eligibleViewExports(view, PATH, [
        PATH,
        "/mnt/user-data/outputs/results/chart.png",
      ]),
    ).toEqual([]);
    expect(
      eligibleViewExports(view, PATH, [
        "/mnt/user-data/outputs/results/nested",
      ]),
    ).toEqual([
      { path: "/mnt/user-data/outputs/results/nested/a.pdf", label: "PDF" },
    ]);
    expect(
      eligibleViewExports(view, PATH, ["/mnt/user-data/outputs/results/neste"]),
    ).toEqual([]);
  });

  it("visibly distinguishes ignored unsafe suggestions from the ordinary destination", () => {
    expect(
      viewCollection(
        parse({ ...base, destination: { collection: "a\ud800" } })!,
      ),
    ).toEqual({ collection: undefined, ignored: true });
    expect(
      viewCollection(
        parse({ ...base, destination: { collection: "Summaries" } })!,
      ),
    ).toEqual({ collection: "Summaries", ignored: false });
    expect(
      viewCollection(
        parse({ ...base, destination: { collection: "../Private" } })!,
      ),
    ).toEqual({ collection: undefined, ignored: true });
    expect(viewCollection(parse(base)!)).toEqual({
      collection: undefined,
      ignored: false,
    });
  });
});
