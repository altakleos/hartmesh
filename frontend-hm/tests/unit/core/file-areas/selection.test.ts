import { describe, expect, it } from "@rstest/core";

import { selectLoadedFiles } from "@/core/file-areas/selection";

describe("loaded file selection", () => {
  it("orders natural names and duplicate paths deterministically without mutating records", () => {
    const entries = Object.freeze([
      Object.freeze({ name: "part10.txt", path: "part10.txt", modified: 1 }),
      Object.freeze({ name: "part2.txt", path: "Z/part2.txt", modified: 1 }),
      Object.freeze({ name: "part2.txt", path: "A/part2.txt", modified: 1 }),
    ]);
    const options = { query: "", order: "name" as const, locale: "en-US" };
    const selected = selectLoadedFiles(entries, options);
    expect(selected.map((entry) => entry.path)).toEqual([
      "A/part2.txt",
      "Z/part2.txt",
      "part10.txt",
    ]);
    expect(selectLoadedFiles([...entries].reverse(), options)).toEqual(
      selected,
    );
    expect(selected[0]).toBe(entries[2]);
    expect(entries[0]!.path).toBe("part10.txt");
  });

  it("uses the Shared publication date, falling back to modified time for unmanaged or invalid dates", () => {
    const entries = [
      {
        name: "older",
        path: "older",
        modified: 999,
        published_at: "1970-01-01T00:00:01Z",
      },
      { name: "unmanaged", path: "unmanaged", modified: 3, published_at: null },
      {
        name: "invalid",
        path: "invalid",
        modified: 2,
        published_at: "invalid",
      },
      { name: "unknown", path: "unknown", modified: NaN, published_at: null },
    ];
    expect(
      selectLoadedFiles(entries, {
        query: "",
        order: "newest",
        locale: "en-US",
      }).map((entry) => entry.path),
    ).toEqual(["unmanaged", "invalid", "older", "unknown"]);
  });

  it("matches Unicode forms in loaded names and folders while retaining literal action paths", () => {
    const entry = {
      name: "Résumé.txt",
      path: "Ｆｏｌｄｅｒ/Résumé.txt",
      modified: 1,
    };
    for (const query of [" re\u0301sume\u0301 ", "FOLDER/"]) {
      expect(
        selectLoadedFiles([entry], { query, order: "name", locale: "en-US" }),
      ).toEqual([entry]);
    }
    expect(entry.path).toBe("Ｆｏｌｄｅｒ/Résumé.txt");
  });
});
