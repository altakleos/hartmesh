import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
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
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

const ONE_TIME = "Xy3-one-time-4Kq9sLmN0pRt";

function installGateway(
  signOnOnly: boolean,
  options: { managed?: boolean; inUse?: number; rejectSeats?: boolean } = {},
) {
  const calls: { url: string; init?: RequestInit }[] = [];
  let inUse = options.inUse ?? 2;
  rs.spyOn(globalThis, "fetch").mockImplementation(
    (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      calls.push({ url, init });
      if (url.endsWith("/api/features")) {
        return Promise.resolve(
          Response.json({
            agents_api: { enabled: true },
            storage_spaces: {
              enabled: options.managed === true,
              state: options.managed ? "ready" : "off",
              own: "ready",
            },
            accounts: {
              seats: {
                allowed: options.managed ? 3 : 0,
                in_use: options.managed ? inUse : 0,
              },
            },
            branding: {
              provider: {
                display_name: "Acme",
                support_url: "https://support.example.com/help",
              },
            },
          }),
        );
      }
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
        if (options.rejectSeats) {
          inUse = 3;
          return Promise.resolve(
            Response.json(
              {
                detail: {
                  code: "no_seats_left",
                  message: "raw server seat details",
                },
              },
              { status: 409 },
            ),
          );
        }
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

const clients: QueryClient[] = [];
function renderPage() {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <FileActionLifetimeProvider>
        <I18nContext.Provider
          value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
        >
          <AccountSettingsPage />
        </I18nContext.Provider>
      </FileActionLifetimeProvider>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  rs.restoreAllMocks();
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  role = "admin";
});

describe("adding a person from the account settings", () => {
  it("shows managed seats above Add a person and disables a full account form", async () => {
    installGateway(false, { managed: true, inUse: 3 });
    renderPage();
    expect(
      await screen.findByText(enUS.settings.account.seats(3, 3)),
    ).toBeTruthy();
    const button = await screen.findByRole<HTMLButtonElement>("button", {
      name: enUS.settings.account.addPersonSubmit,
    });
    expect(button.disabled).toBe(true);
    expect(button.getAttribute("aria-describedby")).toBeTruthy();
    expect(
      screen.getByText(enUS.settings.account.noSeats(3, "Acme")),
    ).toBeTruthy();
    expect(
      screen
        .getByRole("link", { name: enUS.workspace.providerSupport("Acme") })
        .getAttribute("href"),
    ).toBe("https://support.example.com/help");
  });

  it("shows the seat line and provider support in sign-on-only mode", async () => {
    installGateway(true, { managed: true, inUse: 3 });
    renderPage();
    expect(
      await screen.findByText(enUS.settings.account.seats(3, 3)),
    ).toBeTruthy();
    expect(
      await screen.findByText(enUS.settings.account.noSeatsSignOn(3, "Acme")),
    ).toBeTruthy();
    expect(
      screen.queryByRole("button", {
        name: enUS.settings.account.addPersonSubmit,
      }),
    ).toBeNull();
  });

  it("localizes a concurrent no_seats_left refusal and refreshes the count", async () => {
    const calls = installGateway(false, {
      managed: true,
      inUse: 2,
      rejectSeats: true,
    });
    renderPage();
    expect(
      await screen.findByText(enUS.settings.account.seats(2, 3)),
    ).toBeTruthy();
    fireEvent.change(
      screen.getByLabelText(enUS.settings.account.addPersonEmail),
      { target: { value: "new@example.com" } },
    );
    fireEvent.click(
      screen.getByRole("button", {
        name: enUS.settings.account.addPersonSubmit,
      }),
    );
    expect(
      await screen.findByText(enUS.settings.account.noSeats(3, "Acme")),
    ).toBeTruthy();
    await waitFor(() =>
      expect(screen.getByText(enUS.settings.account.seats(3, 3))).toBeTruthy(),
    );
    expect(screen.queryByText("raw server seat details")).toBeNull();
    expect(
      calls.filter((call) => call.url.endsWith("/api/features")).length,
    ).toBeGreaterThan(1);
  });

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
