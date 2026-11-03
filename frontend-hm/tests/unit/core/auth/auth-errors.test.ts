import { describe, expect, test } from "@rstest/core";

import { parseAuthError } from "@/core/auth/types";

describe("parseAuthError", () => {
  test("keeps the server's message for a disabled account", () => {
    // The Gateway refuses a disabled account with this code; an unknown code
    // fell back to the generic "Authentication failed".
    expect(
      parseAuthError({
        detail: {
          code: "account_disabled",
          message: "This account's access has been turned off.",
        },
      }),
    ).toEqual({
      code: "account_disabled",
      message: "This account's access has been turned off.",
    });
  });

  test("still falls back for an unrecognised code", () => {
    expect(
      parseAuthError({ detail: { code: "something_new", message: "x" } }),
    ).toEqual({
      code: "invalid_credentials",
      message: "Authentication failed",
    });
  });
});
