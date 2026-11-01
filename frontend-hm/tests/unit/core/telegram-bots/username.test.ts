import { describe, expect, it } from "@rstest/core";

import { displayNameOf, suggestUsername } from "@/core/telegram-bots/username";

const RANDOM = "[abcdefghjkmnpqrstuvwxyz23456789]";
const custom = (name: string) => ({ kind: "custom_agent", name }) as const;

describe("suggestUsername", () => {
  it("builds slug, four random characters and _bot", () => {
    expect(suggestUsername(custom("Research-Helper"))).toMatch(
      new RegExp(`^researchhelper_${RANDOM}{4}_bot$`),
    );
    expect(
      suggestUsername({ kind: "agent", id: "a".repeat(32), name: "Maya" }),
    ).toMatch(new RegExp(`^maya_${RANDOM}{4}_bot$`));
  });

  it("gives the ordinary chat's bot six random characters", () => {
    expect(suggestUsername({ kind: "chat" })).toMatch(
      new RegExp(`^chat_${RANDOM}{6}_bot$`),
    );
  });

  it("puts ai_ before a slug that begins with a digit", () => {
    expect(suggestUsername(custom("3D helper"))).toMatch(
      new RegExp(`^ai_3dhelper_${RANDOM}{4}_bot$`),
    );
  });

  it("gives ai_ and eight characters for a name without Latin letters", () => {
    expect(
      suggestUsername({ kind: "agent", id: "b".repeat(32), name: "财务助理" }),
    ).toMatch(new RegExp(`^ai_${RANDOM}{8}_bot$`));
  });

  it("stays within 10 to 32 characters", () => {
    for (let length = 1; length <= 128; length += 1) {
      for (const name of ["a".repeat(length), "7".repeat(length)]) {
        const username = suggestUsername(custom(name));
        expect(username.length).toBeGreaterThanOrEqual(10);
        expect(username.length).toBeLessThanOrEqual(32);
      }
    }
  });
});

describe("displayNameOf", () => {
  it("cuts a name over 64 characters", () => {
    expect(displayNameOf("a".repeat(64))).toBe("a".repeat(64));
    expect(displayNameOf("a".repeat(65))).toBe(`${"a".repeat(62)}…`);
    expect(
      displayNameOf(
        "Quarterly financial reporting assistant for the regional sales offices",
      ),
    ).toBe("Quarterly financial reporting assistant for the regional…");
    expect(displayNameOf("a".repeat(70))).toBe(`${"a".repeat(62)}…`);
    const astral = "😀".repeat(70);
    expect(displayNameOf(astral)).toBe(`${"😀".repeat(62)}…`);
  });
});
