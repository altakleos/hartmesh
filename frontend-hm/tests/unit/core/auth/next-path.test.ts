import { describe, expect, test } from "@rstest/core";

import {
  resolveAuthNextPath,
  validateAuthNextPath,
} from "@/core/auth/next-path";

describe("auth next path validation", () => {
  test("accepts local absolute paths", () => {
    expect(validateAuthNextPath("/workspace")).toBe("/workspace");
    expect(validateAuthNextPath("/workspace/chats/new?tab=recent#top")).toBe(
      "/workspace/chats/new?tab=recent#top",
    );
  });

  test("an address with telegram=agent.<id> is kept", () => {
    const address = `/workspace?settings=channels&telegram=agent.${"a".repeat(32)}`;
    expect(validateAuthNextPath(address)).toBe(address);
  });

  test("an address with a colon in the target is refused", () => {
    expect(
      validateAuthNextPath("/workspace?settings=channels&telegram=agent:abc"),
    ).toBeNull();
  });

  test("rejects external and ambiguous redirects", () => {
    expect(validateAuthNextPath(null)).toBeNull();
    expect(validateAuthNextPath("workspace")).toBeNull();
    expect(validateAuthNextPath("//evil.example")).toBeNull();
    expect(validateAuthNextPath("https://evil.example")).toBeNull();
    expect(validateAuthNextPath("/:evil")).toBeNull();
    expect(validateAuthNextPath("/\\evil.example")).toBeNull();
    expect(validateAuthNextPath("/foo\\bar")).toBeNull();
  });

  test("falls back for unsafe paths", () => {
    expect(resolveAuthNextPath("/\\evil.example")).toBe("/workspace");
    expect(resolveAuthNextPath("/safe", "/fallback")).toBe("/safe");
  });
});
