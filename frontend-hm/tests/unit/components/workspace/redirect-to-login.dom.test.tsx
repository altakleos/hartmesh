import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render } from "@testing-library/react";

rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: { common: { loading: "Loading" } } }),
}));

import { RedirectToLogin } from "@/components/workspace/redirect-to-login";
import { currentReturnPath } from "@/core/auth/next-path";

afterEach(() => {
  cleanup();
  rs.unstubAllGlobals();
});

describe("RedirectToLogin", () => {
  it("replaces the location with the sign-in address of the current page", () => {
    const replace = rs.fn();
    rs.stubGlobal("location", {
      pathname: "/workspace",
      search: "?settings=channels&telegram=chat",
      replace,
    });
    expect(currentReturnPath()).toBe(
      "/workspace?settings=channels&telegram=chat",
    );
    render(<RedirectToLogin />);
    expect(replace).toHaveBeenCalledWith(
      `/login?next=${encodeURIComponent("/workspace?settings=channels&telegram=chat")}`,
    );
  });
});
