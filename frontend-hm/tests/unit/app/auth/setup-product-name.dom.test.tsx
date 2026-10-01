import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

type AuthState = {
  isAuthenticated: boolean;
  user: { email: string; needs_setup: boolean; system_role: string } | null;
};

let auth: AuthState = { isAuthenticated: false, user: null };
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: rs.fn(), replace: rs.fn(), refresh: rs.fn() }),
}));
rs.mock("next-themes", () => ({
  useTheme: () => ({ theme: "light", resolvedTheme: "light" }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ ...auth, isLoading: false }),
}));
rs.mock("@/core/auth/setup", () => ({
  fetchSetupStatus: rs.fn(async () => ({ needs_setup: true })),
  isSystemAlreadyInitializedError: () => false,
}));
// A canvas animation with no bearing on what the page offers.
rs.mock("@/components/ui/flickering-grid", () => ({
  FlickeringGrid: () => null,
}));

import SetupPage from "@/app/(auth)/setup/page";
import { I18nContext } from "@/core/i18n/context";
import { createEnUS } from "@/core/i18n/locales/en-US";

function renderSetup() {
  return render(
    <I18nContext.Provider
      value={{
        locale: "en-US",
        setLocale: () => undefined,
        t: createEnUS("Acme Assist"),
      }}
    >
      <SetupPage />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  cleanup();
  auth = { isAuthenticated: false, user: null };
});

describe("the setup page is headed by the deployment's product name", () => {
  it("when the first administrator is created", async () => {
    renderSetup();

    expect(
      await screen.findByRole("heading", { level: 1, name: "Acme Assist" }),
    ).toBeTruthy();
    expect(screen.getByText("Create admin account")).toBeTruthy();
    expect(screen.queryByText("HartMesh")).toBeNull();
  });

  it("when an added person finishes their account", async () => {
    auth = {
      isAuthenticated: true,
      user: {
        email: "person@example.com",
        needs_setup: true,
        system_role: "user",
      },
    };

    renderSetup();

    expect(
      await screen.findByRole("heading", { level: 1, name: "Acme Assist" }),
    ).toBeTruthy();
    expect(screen.getByText("Finish setting up your account")).toBeTruthy();
    expect(screen.queryByText("HartMesh")).toBeNull();
  });
});
