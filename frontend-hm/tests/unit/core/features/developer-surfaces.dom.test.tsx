import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, renderHook } from "@testing-library/react";

const presentation: {
  profile?: "business" | "developer";
  isLoading: boolean;
} = { isLoading: false };
const auth: { user: { system_role: string } | null } = { user: null };

rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("@tanstack/react-query", () => ({
  useQuery: () => ({
    data:
      presentation.profile === undefined
        ? undefined
        : { profile: presentation.profile, starters: [] },
    isPending: presentation.isLoading,
  }),
}));

import { useDeveloperSurfacesVisible } from "@/core/features/hooks";

function visible() {
  return renderHook(() => useDeveloperSurfacesVisible()).result.current;
}

afterEach(() => {
  cleanup();
  presentation.profile = undefined;
  presentation.isLoading = false;
  auth.user = null;
});

describe("useDeveloperSurfacesVisible", () => {
  it("keeps the developer screens for administrators in a business workspace", () => {
    presentation.profile = "business";
    auth.user = { system_role: "user" };

    expect(visible()).toBe(false);
  });

  it("still offers them to an administrator", () => {
    presentation.profile = "business";
    auth.user = { system_role: "admin" };

    expect(visible()).toBe(true);
  });

  it("offers them to everyone where the deployment said nothing", () => {
    presentation.profile = "developer";
    auth.user = { system_role: "user" };

    expect(visible()).toBe(true);
  });

  it("offers them while the answer is still unknown", () => {
    // Taking screens away and putting them back is worse than a late hide,
    // and this is the state every deployment was in before the setting.
    presentation.isLoading = true;
    auth.user = { system_role: "user" };

    expect(visible()).toBe(true);
  });
});
