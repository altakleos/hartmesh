import { describe, expect, it } from "@rstest/core";

import { selectAvailableModel } from "@/core/models/selection";
import type { Model } from "@/core/models/types";

const first: Model = {
  id: "first",
  name: "first",
  model: "first",
  display_name: "First",
};
const preferred: Model = {
  id: "preferred",
  name: "preferred",
  model: "preferred",
  display_name: "Preferred",
};
describe("available model selection", () => {
  it("keeps an available selection ahead of an agent default", () => {
    expect(selectAvailableModel([first, preferred], "first", "preferred")).toBe(
      first,
    );
  });
  it("uses an available agent default when the selected provider disappears", () => {
    expect(
      selectAvailableModel([first, preferred], "removed", "preferred"),
    ).toBe(preferred);
  });
  it("uses the first model when both saved selections disappear", () => {
    expect(selectAvailableModel([first], "removed", "preferred")).toBe(first);
  });
  it("does not invent a model for an empty catalog", () => {
    expect(selectAvailableModel([], "removed", "preferred")).toBeUndefined();
  });
});
