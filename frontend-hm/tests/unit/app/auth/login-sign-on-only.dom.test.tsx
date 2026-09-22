import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

// The page pulls the router and the query string from next/navigation; hand
// it inert ones so nothing tries to drive a real Next.js router under
// happy-dom.
let search = "";
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: rs.fn(), replace: rs.fn(), refresh: rs.fn() }),
  useSearchParams: () => new URLSearchParams(search),
}));
rs.mock("next-themes", () => ({
  useTheme: () => ({ theme: "light", resolvedTheme: "light" }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ isAuthenticated: false, user: null, isLoading: false }),
}));
// A canvas animation with no bearing on what the page offers.
rs.mock("@/components/ui/flickering-grid", () => ({
  FlickeringGrid: () => null,
}));

import LoginPage from "@/app/(auth)/login/page";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

type SetupStatus = {
  needs_setup: boolean;
  registration_enabled: boolean;
  sign_on_only?: boolean;
};

function installGateway(
  setupStatus: SetupStatus,
  providers: { id: string; display_name: string; type: string }[],
) {
  rs.spyOn(globalThis, "fetch").mockImplementation(
    (input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      const body = url.includes("/setup-status")
        ? setupStatus
        : url.includes("/providers")
          ? { providers }
          : null;
      return Promise.resolve(
        new Response(JSON.stringify(body), {
          status: body === null ? 404 : 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    },
  );
}

function renderLogin() {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <LoginPage />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  rs.restoreAllMocks();
  cleanup();
  search = "";
});

describe("the login page in sign-on-only mode", () => {
  it("offers the provider's sign-in and nothing local", async () => {
    installGateway(
      { needs_setup: false, registration_enabled: false, sign_on_only: true },
      [{ id: "sso", display_name: "Single sign-on", type: "oidc" }],
    );

    renderLogin();

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Continue with Single sign-on" }),
      ).toBeTruthy();
    });
    expect(screen.getByText(enUS.login.signOnOnlyDescription)).toBeTruthy();
    expect(screen.queryByLabelText(enUS.login.email)).toBeNull();
    expect(screen.queryByLabelText(enUS.login.password)).toBeNull();
    expect(
      screen.queryByRole("button", { name: enUS.login.signIn }),
    ).toBeNull();
    expect(screen.queryByText(enUS.login.noAccountSignUp)).toBeNull();
    expect(screen.queryByText(enUS.login.createAdminAccount)).toBeNull();
    expect(screen.queryByText(enUS.login.orContinueWith)).toBeNull();
  });

  it("says so when the provider list is empty rather than showing a form", async () => {
    installGateway(
      { needs_setup: false, registration_enabled: false, sign_on_only: true },
      [],
    );

    renderLogin();

    await waitFor(() => {
      expect(screen.getByRole("status").textContent).toBe(
        enUS.login.signOnOnlyNoProvider,
      );
    });
    expect(screen.queryByLabelText(enUS.login.password)).toBeNull();
  });

  it("keeps the local form on a Gateway that does not close local passwords", async () => {
    installGateway({ needs_setup: false, registration_enabled: true }, [
      { id: "sso", display_name: "Single sign-on", type: "oidc" },
    ]);

    renderLogin();

    await waitFor(() => {
      expect(screen.getByText(enUS.login.noAccountSignUp)).toBeTruthy();
    });
    expect(screen.getByLabelText(enUS.login.password)).toBeTruthy();
    expect(screen.getByText(enUS.login.orContinueWith)).toBeTruthy();
  });

  it("does not offer the create-admin page while the mode is sign-on only", async () => {
    // A Gateway in sign-on-only mode answers needs_setup: false by contract;
    // this pins the page's own guard should the two ever disagree.
    installGateway(
      { needs_setup: true, registration_enabled: false, sign_on_only: true },
      [{ id: "sso", display_name: "Single sign-on", type: "oidc" }],
    );

    renderLogin();

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Continue with Single sign-on" }),
      ).toBeTruthy();
    });
    expect(screen.queryByText(enUS.login.createAdminAccount)).toBeNull();
  });

  it("shows a Gateway error redirect without suggesting a password", async () => {
    search = "error=sso_account_exists";
    installGateway(
      { needs_setup: false, registration_enabled: false, sign_on_only: true },
      [{ id: "sso", display_name: "Single sign-on", type: "oidc" }],
    );

    renderLogin();

    await waitFor(() => {
      expect(screen.getByRole("alert").textContent).toBe(
        enUS.login.signOnOnlyErrors.sso_account_exists,
      );
    });
    expect(screen.queryByLabelText(enUS.login.password)).toBeNull();
    expect(screen.queryByText(enUS.login.errors.sso_account_exists)).toBeNull();
  });

  it.each([
    ["sso_no_access", enUS.login.signOnOnlyErrors.sso_no_access],
    ["sso_access_off", enUS.login.signOnOnlyErrors.sso_access_off],
  ])(
    "tells a person refused by the membership rule what to do (%s)",
    async (code, message) => {
      search = `error=${code}`;
      installGateway(
        { needs_setup: false, registration_enabled: false, sign_on_only: true },
        [{ id: "sso", display_name: "Single sign-on", type: "oidc" }],
      );

      renderLogin();

      await waitFor(() => {
        expect(screen.getByRole("alert").textContent).toBe(message);
      });
      expect(message).toContain("Ask your administrator");
      expect(message.toLowerCase()).not.toContain("claim");
      expect(
        screen.getByRole("button", { name: "Continue with Single sign-on" }),
      ).toBeTruthy();
    },
  );

  it("names the address refusal instead of falling back to the generic failure", async () => {
    // An address no account can hold is its own refusal. Without words of its
    // own the page would show "Authentication failed." and send nobody
    // anywhere; it must never read as a conflict with an existing account.
    search = "error=sso_email_unusable";
    installGateway(
      { needs_setup: false, registration_enabled: false, sign_on_only: true },
      [{ id: "sso", display_name: "Single sign-on", type: "oidc" }],
    );

    renderLogin();

    const message = enUS.login.signOnOnlyErrors.sso_email_unusable;
    await waitFor(() => {
      expect(screen.getByRole("alert").textContent).toBe(message);
    });
    expect(message).not.toBe(enUS.login.authFailed);
    expect(message.toLowerCase()).not.toContain("already");
    expect(screen.queryByLabelText(enUS.login.password)).toBeNull();
    expect(
      screen.getByRole("button", { name: "Continue with Single sign-on" }),
    ).toBeTruthy();
  });

  it("says an address belongs to another account without suggesting a password", async () => {
    // A new subject carrying an address an old provider account still holds.
    // It must never read as "sign in with your password": there is no
    // password account of theirs to sign in to, and in this mode none can
    // exist at all.
    search = "error=sso_email_taken";
    installGateway(
      { needs_setup: false, registration_enabled: false, sign_on_only: true },
      [{ id: "sso", display_name: "Single sign-on", type: "oidc" }],
    );

    renderLogin();

    const message = enUS.login.signOnOnlyErrors.sso_email_taken;
    await waitFor(() => {
      expect(screen.getByRole("alert").textContent).toBe(message);
    });
    expect(message).not.toBe(enUS.login.authFailed);
    expect(message.toLowerCase()).not.toContain("password");
    expect(screen.queryByLabelText(enUS.login.password)).toBeNull();
    expect(
      screen.getByRole("button", { name: "Continue with Single sign-on" }),
    ).toBeTruthy();
    // The two held-address refusals reach the same page in this mode and
    // their remedies are opposite: the other one is a stale account of the
    // person's own to clear, this one is another person's live account, and
    // clearing that would delete someone. A support person hears these read
    // aloud, so they must not be near-identical sentences.
    const other = enUS.login.signOnOnlyErrors.sso_account_exists;
    expect(message).not.toBe(other);
    for (const [refusal, word] of [
      [message, "release"],
      [other, "cleared"],
    ] as const) {
      expect(refusal.toLowerCase()).toContain(word);
      const opposite = refusal === message ? other : message;
      expect(opposite.toLowerCase()).not.toContain(word);
    }
  });

  it("names the address refusal on a Gateway that still serves local passwords", async () => {
    // The other map: a deployment with local passwords AND a provider gets the
    // same redirect, and reads `login.errors`. Without its own entry there the
    // page would degrade to "Authentication failed." with nothing to act on.
    search = "error=sso_email_unusable";
    installGateway({ needs_setup: false, registration_enabled: true }, [
      { id: "sso", display_name: "Single sign-on", type: "oidc" },
    ]);

    renderLogin();

    // The mixed-mode branch renders the error as plain text, without the
    // role="alert" the sign-on-only branch carries, so assert on the words.
    const message = enUS.login.errors.sso_email_unusable;
    expect(await screen.findByText(message)).toBeTruthy();
    // Proves the mixed-mode branch rendered, not the sign-on-only one.
    expect(screen.getByLabelText(enUS.login.password)).toBeTruthy();
    expect(message).not.toBe(enUS.login.authFailed);
    for (const leak of [
      "special-use",
      "reserved",
      "@-sign",
      "validator",
      "pydantic",
      "already",
    ]) {
      expect(message.toLowerCase()).not.toContain(leak);
    }
  });

  it("names a held address on a Gateway that still serves local passwords", async () => {
    // `sso_email_taken` was added to both maps; the mixed-mode one reads
    // `login.errors`, and without a test there it could be deleted and the
    // page would degrade to "Authentication failed." with nothing failing.
    search = "error=sso_email_taken";
    installGateway({ needs_setup: false, registration_enabled: true }, [
      { id: "sso", display_name: "Single sign-on", type: "oidc" },
    ]);

    renderLogin();

    const message = enUS.login.errors.sso_email_taken;
    expect(await screen.findByText(message)).toBeTruthy();
    // Proves the mixed-mode branch rendered, not the sign-on-only one.
    expect(screen.getByLabelText(enUS.login.password)).toBeTruthy();
    expect(message).not.toBe(enUS.login.authFailed);
    // Even here, where a password door exists, it is not this person's: the
    // account in the way is somebody else's.
    expect(message).not.toBe(enUS.login.errors.sso_account_exists);
    expect(message.toLowerCase()).not.toContain("your password");
  });
});
