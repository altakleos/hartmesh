import { describe, expect, it } from "@rstest/core";

import {
  boundedBrowserRows,
  browserFolder,
  browserHref,
} from "@/core/file-areas/browser";

const file = (path: string, modified = 1) => ({
  path,
  name: path.split("/").at(-1)!,
  size: 3,
  modified,
  kind: "file" as const,
});
const records = [
  file("Work/報告/a.txt"),
  file("Work/b.txt", 10),
  file("home.txt"),
];
const options = {
  folder: "",
  query: "",
  order: "newest" as const,
  locale: "en-US",
  virtual: true,
};

describe("bounded browser locations", () => {
  it("groups direct children, retains source paths and searches the location from a subfolder", () => {
    expect(
      boundedBrowserRows(records, options).map((r) => [r.kind, r.path]),
    ).toEqual([
      ["directory", "Work"],
      ["file", "home.txt"],
    ]);
    expect(
      boundedBrowserRows(records, { ...options, folder: "Work" }).map(
        (r) => r.path,
      ),
    ).toEqual(["Work/報告", "Work/b.txt"]);
    expect(
      boundedBrowserRows(records, {
        ...options,
        folder: "Work/報告",
        query: "HOME",
      }),
    ).toEqual([records[2]]);
    expect(
      boundedBrowserRows(records, {
        ...options,
        virtual: false,
        folder: "Work",
        query: "home",
      }),
    ).toEqual([]);
  });
  it("keeps URLs within a known source and preserves literal Unicode and escaped bytes", () => {
    for (const path of [
      "../../escape",
      "/abs",
      "a//b",
      "a/./b",
      "a\\b",
      "a\u0000b",
    ])
      expect(browserFolder(path)).toBeNull();
    const path = "報告/100% #1";
    expect(
      new URL(
        browserHref("shared", path),
        "https://example.test",
      ).searchParams.get("folder"),
    ).toBe(path);
    expect(browserHref("personal", "")).toBe("/workspace/files");
    expect(browserHref("agent", "outputs", { instance: "abc" })).toBe(
      "/workspace/instances?instance=abc&tab=files&folder=outputs",
    );
  });
});
