import { afterEach, describe, expect, it, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

type Role = "admin" | "user";
let role: Role = "admin";
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "u-1",
      email: "owner@example.com",
      system_role: role,
      needs_setup: false,
      oauth_provider: null,
    },
    logout: rs.fn(),
  }),
}));

import { AccountSettingsPage } from "@/components/workspace/settings/account-settings-page";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

const ONE_TIME = "Xy3-one-time-4Kq9sLmN0pRt";

function installGateway(signOnOnly: boolean) {
  const calls: { url: string; init?: RequestInit }[] = [];
  rs.spyOn(globalThis, "fetch").mockImplementation(
    (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      calls.push({ url, init });
      if (url.includes("/setup-status")) {
        return Promise.resolve(
          Response.json({
            needs_setup: false,
            registration_enabled: false,
            sign_on_only: signOnOnly,
          }),
        );
      }
      if (url.endsWith("/api/v1/auth/users")) {
        return Promise.resolve(
          Response.json(
            {
              id: "u-2",
              email: "pat@example.com",
              system_role: "user",
              needs_setup: true,
              one_time_password: ONE_TIME,
            },
            { status: 201 },
          ),
        );
      }
      return Promise.resolve(new Response(null, { status: 404 }));
    },
  );
  return calls;
}

function renderPage() {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <AccountSettingsPage />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  rs.restoreAllMocks();
  cleanup();
  role = "admin";
});

describe("adding a person from the account settings", () => {
  it("offers an administrator the control and shows the one-time password once", async () => {
    const calls = installGateway(false);
    renderPage();

    const input = await screen.findByLabelText(
      enUS.settings.account.addPersonEmail,
    );
    expect(screen.getByText(enUS.settings.account.addPersonTitle)).toBeTruthy();
    fireEvent.change(input, { target: { value: "pat@example.com" } });
    fireEvent.click(
      screen.getByRole("button", {
        name: enUS.settings.account.addPersonSubmit,
      }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("one-time-password").textContent).toBe(
        ONE_TIME,
      );
    });
    expect(screen.getByText("Added pat@example.com.")).toBeTruthy();
    const post = calls.find((call) => call.url.endsWith("/api/v1/auth/users"));
    expect(post?.init?.method).toBe("POST");
    expect(JSON.parse(post?.init?.body as string)).toEqual({
      email: "pat@example.com",
    });

    // Moving on is the last time it is seen.
    fireEvent.click(
      screen.getByRole("button", {
        name: enUS.settings.account.addPersonAnother,
      }),
    );
    expect(screen.queryByTestId("one-time-password")).toBeNull();
    expect(screen.queryByText(ONE_TIME)).toBeNull();
  });

  it("offers a user nothing", async () => {
    role = "user";
    const calls = installGateway(false);
    renderPage();

    expect(
      await screen.findByText(enUS.settings.account.changePasswordTitle),
    ).toBeTruthy();
    expect(screen.queryByText(enUS.settings.account.addPersonTitle)).toBeNull();
    expect(calls.some((call) => call.url.includes("/setup-status"))).toBe(
      false,
    );
  });

  it("offers an administrator nothing in sign-on-only mode", async () => {
    const calls = installGateway(true);
    renderPage();

    await waitFor(() => {
      expect(calls.some((call) => call.url.includes("/setup-status"))).toBe(
        true,
      );
    });
    // Let the answer land: the control must stay away after it, not only before.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(screen.queryByText(enUS.settings.account.addPersonTitle)).toBeNull();
    expect(
      screen.queryByLabelText(enUS.settings.account.addPersonEmail),
    ).toBeNull();
  });
});
