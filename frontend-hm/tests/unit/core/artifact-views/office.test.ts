import { readFileSync } from "node:fs";

import { expect, it } from "@rstest/core";

import { parseArtifactView } from "@/core/artifact-views/contract";

it.each(["workbook", "document", "deck"])(
  "accepts the %s presentation produced by the installed package",
  (kind) => {
    const content = readFileSync(`tests/fixtures/office/${kind}.json`, "utf8");
    const view = parseArtifactView(content);
    expect(view).not.toBeNull();
    expect(view?.blocks.at(-1)).toMatchObject({ type: "notice" });
    expect(view?.exports).toEqual([]);
  },
);

it("accepts a full workbook cell measured in Unicode code points", () => {
  const view = JSON.parse(
    readFileSync("tests/fixtures/office/workbook.json", "utf8"),
  ) as {
    blocks: { rows: string[][] }[];
  };
  view.blocks[0]!.rows[0]![0] = "=" + "🎈".repeat(4095);
  expect(parseArtifactView(JSON.stringify(view))).not.toBeNull();
  view.blocks[0]!.rows[0]![0] += "🎈";
  expect(parseArtifactView(JSON.stringify(view))).toBeNull();
});
