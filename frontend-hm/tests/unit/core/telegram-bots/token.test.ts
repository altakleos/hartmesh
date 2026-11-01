import { describe, expect, it } from "@rstest/core";

import { findToken, TOKEN_BODY } from "@/core/telegram-bots/token";

// Built at run time: no tracked file may hold text of a token's shape.
const token = `${"1".repeat(9)}:${"A".repeat(35)}`;

describe("findToken", () => {
  it("finds a token alone", () => {
    expect(findToken(token)).toBe(token);
  });

  it("finds the token in BotFather's whole message", () => {
    expect(
      findToken(
        `Done! Use this token to access the HTTP API:\n${token}\nKeep it safe.`,
      ),
    ).toBe(token);
  });

  it("finds the token inside an API address", () => {
    expect(findToken(`https://api.telegram.org/bot${token}/getMe`)).toBe(token);
  });

  it("does not start a match inside a longer number", () => {
    const body = "A".repeat(35);
    expect(findToken(`${"1".repeat(13)}:${body}`)).toBeNull();
  });

  it("returns null without a token", () => {
    expect(findToken("123456789:short")).toBeNull();
    expect(findToken("")).toBeNull();
  });

  it("keeps the pattern the design gives", () => {
    expect(TOKEN_BODY).toBe("[0-9]{6,12}:[A-Za-z0-9_-]{30,}");
  });
});
