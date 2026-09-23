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

const KEY = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY";
const copy = enUS.settings.providerKeys;

type Provider = {
  provider: string;
  variable: string;
  kind: "models" | "tools";
  source: "product" | "environment" | "none";
  product_key: "absent" | "set" | "unreadable";
  wrapped_with: null;
  changed_at: string | null;
  changed_by: string | null;
};

function provider(
  id: string,
  variable: string,
  kind: Provider["kind"],
  source: Provider["source"],
  productKey: Provider["product_key"] = "absent",
): Provider {
  const stored = productKey !== "absent";
  return {
    provider: id,
    variable,
    kind,
    source,
    product_key: productKey,
    wrapped_with: null,
    changed_at: stored ? "2026-09-23T10:00:00+00:00" : null,
    changed_by: stored ? "owner@example.com" : null,
  };
}

type GatewayError = { status: number; code: string; message: string };

function installGateway(status: object, putError?: GatewayError) {
  const calls: { url: string; init?: RequestInit }[] = [];
  let current = status;
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
          Response.json({ needs_setup: false, sign_on_only: true }),
        );
      }
      if (url.includes("/api/provider-keys/events")) {
        return Promise.resolve(
          Response.json({
            events: [
              {
                event_id: "e-1",
                variable: "ANTHROPIC_API_KEY",
                action: "added",
                actor_id: "u-1",
                actor_email: "owner@example.com",
                occurred_at: "2026-09-23T10:00:00+00:00",
              },
            ],
          }),
        );
      }
      if (
        url.endsWith("/api/provider-keys/anthropic") &&
        init?.method === "PUT"
      ) {
        if (putError) {
          return Promise.resolve(
            Response.json(
              { detail: { code: putError.code, message: putError.message } },
              { status: putError.status },
            ),
          );
        }
        current = {
          ...(current as { providers: Provider[] }),
          providers: [
            provider("openai", "OPENAI_API_KEY", "models", "environment"),
            provider(
              "anthropic",
              "ANTHROPIC_API_KEY",
              "models",
              "product",
              "set",
            ),
            provider("tavily", "TAVILY_API_KEY", "tools", "none"),
          ],
        };
        return Promise.resolve(
          Response.json({ action: "added", provider: {} }),
        );
      }
      if (
        url.endsWith("/api/provider-keys/openai") &&
        init?.method === "DELETE"
      ) {
        return Promise.resolve(
          Response.json({
            action: "removed",
            provider: provider("openai", "OPENAI_API_KEY", "models", "none"),
          }),
        );
      }
      if (url.endsWith("/api/provider-keys")) {
        return Promise.resolve(Response.json(current));
      }
      return Promise.resolve(new Response(null, { status: 404 }));
    },
  );
  return calls;
}

const MANAGED = {
  available: true,
  refusal: null,
  wrapping_key: "set",
  providers: [
    provider("openai", "OPENAI_API_KEY", "models", "environment"),
    provider("anthropic", "ANTHROPIC_API_KEY", "models", "none"),
    provider("tavily", "TAVILY_API_KEY", "tools", "none"),
  ],
};

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

describe("provider keys in the account settings", () => {
  it("shows an administrator where each key comes from and sends a new one without keeping it", async () => {
    const calls = installGateway(MANAGED);
    const { container } = renderPage();

    expect(await screen.findByText(copy.title)).toBeTruthy();
    const openai = await screen.findByTestId("provider-key-openai");
    expect(openai.textContent).toContain(copy.sourceEnvironment);
    expect(screen.getByTestId("provider-key-anthropic").textContent).toContain(
      copy.sourceNone,
    );
    expect(
      await screen.findByText(/owner@example.com added the Anthropic key/),
    ).toBeTruthy();

    const anthropic = screen.getByTestId("provider-key-anthropic");
    fireEvent.click(
      Array.from(anthropic.querySelectorAll("button")).find(
        (button) => button.textContent === copy.set,
      )!,
    );
    const input = screen.getByLabelText<HTMLInputElement>(
      copy.keyLabel.replace("{provider}", "Anthropic"),
    );
    expect(input.type).toBe("password");
    fireEvent.change(input, { target: { value: KEY } });
    fireEvent.click(screen.getByRole("button", { name: copy.save }));

    await waitFor(() =>
      expect(
        screen.getByTestId("provider-key-anthropic").textContent,
      ).toContain(copy.sourceProduct),
    );
    const put = calls.find((call) => call.init?.method === "PUT");
    expect(put?.url).toContain("/api/provider-keys/anthropic");
    expect(JSON.parse(put?.init?.body as string)).toEqual({ key: KEY });
    expect(screen.getByRole("status").textContent).toContain("Anthropic");
    // The form closes once the key is sent.
    expect(
      screen.queryByLabelText(copy.keyLabel.replace("{provider}", "Anthropic")),
    ).toBeNull();
    // Once sent, the key is nowhere on the page, not even in an input's value.
    expect(container.innerHTML).not.toContain(KEY);
    expect(
      Array.from(container.querySelectorAll("input")).some(
        (element) => element.value === KEY,
      ),
    ).toBe(false);
  });

  async function submitAnthropicKey() {
    const anthropic = await screen.findByTestId("provider-key-anthropic");
    fireEvent.click(
      Array.from(anthropic.querySelectorAll("button")).find(
        (button) => button.textContent === copy.set,
      )!,
    );
    fireEvent.change(
      screen.getByLabelText(copy.keyLabel.replace("{provider}", "Anthropic")),
      { target: { value: KEY } },
    );
    fireEvent.click(screen.getByRole("button", { name: copy.save }));
  }

  it("shows the Gateway's refusal of a key without echoing the key", async () => {
    installGateway(MANAGED, {
      status: 422,
      code: "key_invalid",
      message: "not one token",
    });
    const { container } = renderPage();
    await submitAnthropicKey();
    expect(await screen.findByText("not one token")).toBeTruthy();
    expect(container.textContent).not.toContain(KEY);
  });

  it("says in the administrator's words when the workspace's setup would not take the key", async () => {
    installGateway(MANAGED, {
      status: 422,
      code: "render_refused",
      message:
        "the deployment's configuration would not render: OPENAI_API_KEY",
    });
    renderPage();
    await submitAnthropicKey();
    expect(await screen.findByText(copy.renderRefused)).toBeTruthy();
    expect(screen.queryByText(/OPENAI_API_KEY would not render/)).toBeNull();
  });

  it("says why keys cannot be set, that a saved key is intact, and still lets one be removed", async () => {
    installGateway({
      available: true,
      refusal: {
        code: "no_wrapping_key",
        message: "HARTMESH_PROVIDER_KEYS_SECRET is not set",
      },
      wrapping_key: "absent",
      providers: [
        provider("openai", "OPENAI_API_KEY", "models", "none", "unreadable"),
        provider("tavily", "TAVILY_API_KEY", "tools", "none"),
      ],
    });
    const { container } = renderPage();
    expect(await screen.findByText(copy.refusalNoWrappingKey)).toBeTruthy();
    // The admin reads what to do, not the variable the deployer sets.
    expect(container.textContent).not.toContain(
      "HARTMESH_PROVIDER_KEYS_SECRET",
    );
    const openai = await screen.findByTestId("provider-key-openai");
    const labels = Array.from(openai.querySelectorAll("button")).map(
      (button) => button.textContent,
    );
    expect(labels).toEqual([copy.remove]);
    expect(openai.textContent).toContain(copy.unreadable);
    // Setting it again is not on offer here; the key is still stored and
    // comes back once the host restores the setting.
    expect(openai.textContent).toContain(copy.unreadableHintHostSetting);
    expect(openai.textContent).not.toContain(copy.unreadableHint);
    expect(screen.queryByRole("button", { name: copy.set })).toBeNull();
  });

  it("offers nothing to change where the host chooses the models", async () => {
    installGateway({
      available: true,
      refusal: {
        code: "operator_model_file",
        message: "HARTMESH_MODELS_FILE is set",
      },
      wrapping_key: "set",
      providers: [
        provider("openai", "OPENAI_API_KEY", "models", "environment", "set"),
      ],
    });
    renderPage();
    expect(await screen.findByText(copy.refusalOperatorModelFile)).toBeTruthy();
    const openai = await screen.findByTestId("provider-key-openai");
    // The Gateway refuses a removal here too; the page does not offer one.
    expect(openai.querySelectorAll("button").length).toBe(0);
  });

  it("asks before removing a key, and removes nothing when the administrator declines", async () => {
    const calls = installGateway({
      ...MANAGED,
      providers: [
        provider("openai", "OPENAI_API_KEY", "models", "product", "set"),
      ],
    });
    const confirm = rs.spyOn(window, "confirm").mockReturnValue(false);
    renderPage();
    const openai = await screen.findByTestId("provider-key-openai");
    const removeButton = () =>
      Array.from(openai.querySelectorAll("button")).find(
        (button) => button.textContent === copy.remove,
      )!;
    fireEvent.click(removeButton());
    expect(confirm).toHaveBeenCalledWith(
      copy.removeConfirm.replace("{provider}", "OpenAI"),
    );
    expect(calls.some((call) => call.init?.method === "DELETE")).toBe(false);

    confirm.mockReturnValue(true);
    fireEvent.click(removeButton());
    await waitFor(() =>
      expect(calls.some((call) => call.init?.method === "DELETE")).toBe(true),
    );
    expect((await screen.findByRole("status")).textContent).toBe(
      copy.removedToNone.replace("{provider}", "OpenAI"),
    );
  });

  it("renders nothing where the deployment does not manage keys in the product", async () => {
    installGateway({
      available: false,
      refusal: { code: "not_available", message: "not here" },
      wrapping_key: null,
      providers: [],
    });
    renderPage();
    await waitFor(() =>
      expect(
        (
          globalThis.fetch as unknown as { mock: { calls: unknown[][] } }
        ).mock.calls.some((call) =>
          String(call[0]).endsWith("/api/provider-keys"),
        ),
      ).toBe(true),
    );
    expect(screen.queryByText(copy.title)).toBeNull();
  });

  it("is not offered to someone who is not an administrator", async () => {
    role = "user";
    const calls = installGateway(MANAGED);
    renderPage();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(screen.queryByText(copy.title)).toBeNull();
    expect(calls.some((call) => call.url.includes("/api/provider-keys"))).toBe(
      false,
    );
  });
});
