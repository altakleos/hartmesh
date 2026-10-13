import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const location = rs.hoisted(() => ({ search: "" }));
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: rs.fn() }),
  useSearchParams: () => new URLSearchParams(location.search),
}));
rs.mock("next-themes", () => ({
  useTheme: () => ({ theme: "light", resolvedTheme: "light" }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ isAuthenticated: false, user: null }),
}));
rs.mock("@/components/ui/flickering-grid", () => ({
  FlickeringGrid: () => null,
}));

import LoginPage from "@/app/(auth)/login/page";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";

function gateway(signOnOnly = false) {
  rs.spyOn(globalThis, "fetch").mockImplementation(
    (input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes("setup-status"))
        return Promise.resolve(
          Response.json({
            needs_setup: false,
            registration_enabled: true,
            sign_on_only: signOnOnly,
          }),
        );
      if (url.includes("providers"))
        return Promise.resolve(
          Response.json({
            providers: signOnOnly
              ? [{ id: "sso", display_name: "SSO", type: "oidc" }]
              : [],
          }),
        );
      if (url.includes("register"))
        return Promise.resolve(
          Response.json(
            {
              detail: {
                code: "no_seats_left",
                message: "raw server seat details",
              },
            },
            { status: 403 },
          ),
        );
      return Promise.resolve(new Response(null, { status: 404 }));
    },
  );
}
function page(chinese = false) {
  return render(
    <I18nContext.Provider
      value={{
        locale: chinese ? "zh-CN" : "en-US",
        setLocale: () => undefined,
        t: chinese ? zhCN : enUS,
      }}
    >
      <LoginPage />
    </I18nContext.Provider>,
  );
}
afterEach(() => {
  cleanup();
  rs.restoreAllMocks();
  location.search = "";
});

describe("sign-in seat refusals", () => {
  it.each([false, true])(
    "maps an address refusal in password/sign-on mode %s",
    async (signOnOnly) => {
      location.search = "error=no_seats_left";
      gateway(signOnOnly);
      page();
      expect(await screen.findByText(enUS.auth.noSeats)).toBeTruthy();
    },
  );

  it.each([false, true])(
    "maps a registration refusal in the active locale %s",
    async (chinese) => {
      gateway();
      page(chinese);
      const t = chinese ? zhCN : enUS;
      fireEvent.click(
        await screen.findByRole("button", { name: t.login.noAccountSignUp }),
      );
      fireEvent.change(screen.getByLabelText(t.login.email), {
        target: { value: "person@example.com" },
      });
      fireEvent.change(screen.getByLabelText(t.login.password), {
        target: { value: "fixture-password" },
      });
      fireEvent.click(
        screen.getByRole("button", { name: t.login.createAccount }),
      );
      expect(await screen.findByText(t.auth.noSeats)).toBeTruthy();
      expect(screen.queryByText("raw server seat details")).toBeNull();
    },
  );
});
