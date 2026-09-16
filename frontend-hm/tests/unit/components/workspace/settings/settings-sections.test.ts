import { describe, expect, it } from "@rstest/core";

import {
  resolveActiveSection,
  visibleSettingsSections,
} from "@/components/workspace/settings/settings-sections";

const SECTIONS = [
  { id: "account" },
  { id: "appearance" },
  { id: "notification" },
  { id: "channels" },
  { id: "integrations" },
  { id: "memory" },
  { id: "tools" },
  { id: "subagents" },
  { id: "skills" },
  { id: "about" },
];

describe("visibleSettingsSections", () => {
  it("offers everything where the developer screens are visible", () => {
    expect(visibleSettingsSections(SECTIONS, true)).toEqual(SECTIONS);
  });

  it("keeps what is a person's own and drops what builds the deployment", () => {
    // Channels and memory stay: the phone someone messages it from, and what
    // the agent has remembered about them, are theirs, not the deployment's.
    expect(
      visibleSettingsSections(SECTIONS, false).map((section) => section.id),
    ).toEqual([
      "account",
      "appearance",
      "notification",
      "channels",
      "memory",
      "about",
    ]);
  });
});

describe("resolveActiveSection", () => {
  it("keeps a screen the person can see", () => {
    const visible = visibleSettingsSections(SECTIONS, false);

    expect(resolveActiveSection(visible, "appearance")).toBe("appearance");
  });

  it("sends a deep link to a hidden screen somewhere it can go", () => {
    // `?settings=skills` must not render the screen the profile hides.
    const visible = visibleSettingsSections(SECTIONS, false);

    expect(resolveActiveSection(visible, "skills")).toBe("account");
  });
});
