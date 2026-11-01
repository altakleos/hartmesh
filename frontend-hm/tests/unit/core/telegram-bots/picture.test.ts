import { describe, expect, it } from "@rstest/core";

import {
  CHAT_COLOR,
  fingerprintOf,
  PICTURE_COLORS,
  pictureOf,
} from "@/core/telegram-bots/picture";

const ROWS: [string, string, string, string][] = [
  ["Maya", "0123456789abcdef0123456789abcdef", "#1D4ED8", "M"],
  ["Reporting analyst", "ffffffffffffffffffffffffffffffff", "#15803D", "RA"],
  [
    "Élodie Müller-Schmidt",
    "00000000000000000000000000000000",
    "#1D4ED8",
    "ÉM",
  ],
  ["财务助理", "9f86d081884c7d659a2feaa0c55ad015", "#A21CAF", "财"],
  ["3D helper", "a3f1c2d4e5b60718293a4b5c6d7e8f90", "#1D4ED8", "3H"],
  ["research-helper", "research-helper", "#0F766E", "RH"],
  ["QA2", "QA2", "#C2410C", "Q"],
];

describe("pictureOf", () => {
  it("the seven rows give their initials, hash, entry and colour", () => {
    for (const [name, key, color, initials] of ROWS) {
      expect(pictureOf(name, key)).toEqual({ initials, color });
    }
  });

  it("a name without a letter or digit has no initials", () => {
    expect(pictureOf("--- !!", "key").initials).toBe("");
  });

  it("keeps one character of a longer upper case, as the server does", () => {
    expect(pictureOf("ßeta", "key").initials).toBe("S");
  });

  it("tells two texts apart without holding either", () => {
    const secret = "a long secret text";
    expect(fingerprintOf(secret)).toBe(fingerprintOf(secret));
    expect(fingerprintOf(secret)).not.toBe(fingerprintOf(`${secret}!`));
    expect(fingerprintOf(secret).length).toBeLessThanOrEqual(8);
  });

  it("uses the eight colours and the chat colour of the design", () => {
    expect(PICTURE_COLORS).toHaveLength(8);
    expect(CHAT_COLOR).toBe("#334155");
  });
});
