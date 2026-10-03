import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
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
import { MODELS_QUERY_KEY, useModels } from "@/core/models/hooks";

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

function installGateway(
  status: object,
  putError?: GatewayError,
  probe?: () => Promise<Response>,
  putResponse?: Promise<Response>,
) {
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
      if (url.endsWith("/test"))
        return (
          probe?.() ?? Promise.resolve(Response.json({ result: "accepted" }))
        );
      if (url.endsWith("/api/models")) {
        const providers = (current as { providers: Provider[] }).providers;
        return Promise.resolve(
          Response.json({
            models: providers
              .filter((p) => p.kind === "models" && p.source !== "none")
              .map((p) => ({
                name: p.provider,
                display_name: p.provider,
                model: p.provider,
                id: p.provider,
              })),
            token_usage: { enabled: false },
          }),
        );
      }
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
        return (
          putResponse ??
          Promise.resolve(Response.json({ action: "added", provider: {} }))
        );
      }
      if (
        url.endsWith("/api/provider-keys/openai") &&
        init?.method === "DELETE"
      ) {
        current = {
          ...(current as { providers: Provider[] }),
          providers: (current as { providers: Provider[] }).providers.map(
            (p) =>
              p.provider === "openai"
                ? { ...p, source: "none", product_key: "absent" }
                : p,
          ),
        };
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

const clients: QueryClient[] = [];
function ModelObserver() {
  const { models } = useModels();
  return (
    <output data-testid="models">
      {models.map((model) => model.name).join(",")}
    </output>
  );
}
function renderPage(observeModels = false) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(queryClient);
  const tree = (showSettings = true) => (
    <QueryClientProvider client={queryClient}>
      <FileActionLifetimeProvider>
        <I18nContext.Provider
          value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
        >
          {showSettings && <AccountSettingsPage />}
          {observeModels && <ModelObserver />}
        </I18nContext.Provider>
      </FileActionLifetimeProvider>
    </QueryClientProvider>
  );
  const rendered = render(tree());
  return {
    ...rendered,
    queryClient,
    closeSettings: () => rendered.rerender(tree(false)),
  };
}

afterEach(() => {
  rs.restoreAllMocks();
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
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

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((finish) => {
    resolve = finish;
  });
  return { promise, resolve };
}
async function editKey(id = "anthropic", name = "Anthropic") {
  const row = await screen.findByTestId(`provider-key-${id}`);
  fireEvent.click(
    Array.from(row.querySelectorAll("button")).find(
      (button) =>
        button.textContent === copy.set || button.textContent === copy.replace,
    )!,
  );
  const input = screen.getByLabelText<HTMLInputElement>(
    copy.keyLabel.replace("{provider}", name),
  );
  fireEvent.change(input, { target: { value: KEY } });
  return input;
}

describe("provider key feedback and catalog freshness", () => {
  it("refreshes a mounted, indefinitely cached model catalog after saving without a test", async () => {
    const calls = installGateway(MANAGED);
    renderPage(true);
    await waitFor(() =>
      expect(screen.getByTestId("models").textContent).toBe("openai"),
    );
    await editKey();
    fireEvent.click(screen.getByRole("button", { name: copy.save }));
    await waitFor(() =>
      expect(screen.getByTestId("models").textContent).toBe("openai,anthropic"),
    );
    expect(
      calls.filter((call) => call.url.endsWith("/api/models")),
    ).toHaveLength(2);
    expect(calls.some((call) => call.url.endsWith("/test"))).toBe(false);
  });
  it("refreshes the model catalog to empty after removing the final key", async () => {
    installGateway({
      ...MANAGED,
      providers: [
        provider("openai", "OPENAI_API_KEY", "models", "product", "set"),
      ],
    });
    rs.spyOn(window, "confirm").mockReturnValue(true);
    renderPage(true);
    await waitFor(() =>
      expect(screen.getByTestId("models").textContent).toBe("openai"),
    );
    fireEvent.click(screen.getByRole("button", { name: copy.remove }));
    await waitFor(() =>
      expect(screen.getByTestId("models").textContent).toBe(""),
    );
  });
  it("does not invalidate models when a save is refused", async () => {
    const calls = installGateway(MANAGED, {
      status: 422,
      code: "key_invalid",
      message: "invalid key",
    });
    renderPage(true);
    await waitFor(() =>
      expect(screen.getByTestId("models").textContent).toBe("openai"),
    );
    await editKey();
    fireEvent.click(screen.getByRole("button", { name: copy.save }));
    await screen.findByText("invalid key");
    expect(
      calls.filter((call) => call.url.endsWith("/api/models")),
    ).toHaveLength(1);
  });
  it.each([
    ["accepted", "The provider accepted this key. It has not been saved."],
    [
      "rejected",
      "The provider rejected this key. You can edit it or save it anyway.",
    ],
    [
      "inconclusive",
      "The provider could not confirm this key. You can retry or save it anyway.",
    ],
    [
      "not_testable",
      "This provider cannot test keys here. You can still save the key.",
    ],
  ])(
    "reports %s only after an explicit test and never saves",
    async (result, message) => {
      const calls = installGateway(MANAGED, undefined, async () =>
        Response.json({ result, reason: KEY, model: KEY }),
      );
      const { queryClient } = renderPage();
      const input = await editKey();
      expect(calls.some((call) => call.url.endsWith("/test"))).toBe(false);
      fireEvent.click(screen.getByRole("button", { name: "Test key" }));
      await screen.findByText(message);
      expect(input.value).toBe(KEY);
      expect(calls.filter((call) => call.url.endsWith("/test"))).toHaveLength(
        1,
      );
      expect(calls.some((call) => call.init?.method === "PUT")).toBe(false);
      expect(
        JSON.stringify(queryClient.getQueryCache().getAll()),
      ).not.toContain(KEY);
      expect(
        screen
          .getByRole("button", { name: copy.save })
          .hasAttribute("disabled"),
      ).toBe(false);
      expect(document.body.textContent).not.toContain(KEY);
    },
  );
  it.each(["edit", "provider", "cancel", "save"])(
    "retires a pending probe on %s",
    async (action) => {
      const response = deferred<Response>();
      const calls = installGateway(MANAGED, undefined, () => response.promise);
      renderPage();
      const input = await editKey();
      fireEvent.click(screen.getByRole("button", { name: "Test key" }));
      if (action === "edit")
        fireEvent.change(input, { target: { value: "another-key" } });
      if (action === "provider") await editKey("openai", "OpenAI");
      if (action === "cancel")
        fireEvent.click(screen.getByRole("button", { name: copy.cancel }));
      if (action === "save") {
        fireEvent.click(screen.getByRole("button", { name: copy.save }));
        await screen.findByText(copy.saved.replace("{provider}", "Anthropic"));
      }
      expect(
        calls.find((call) => call.url.endsWith("/test"))?.init?.signal?.aborted,
      ).toBe(true);
      await act(async () =>
        response.resolve(Response.json({ result: "accepted" })),
      );
      expect(
        screen.queryByText(
          "The provider accepted this key. It has not been saved.",
        ),
      ).toBeNull();
    },
  );
  it("does not follow a retired account's save with catalog or history requests", async () => {
    const response = deferred<Response>();
    const calls = installGateway(
      MANAGED,
      undefined,
      undefined,
      response.promise,
    );
    const { unmount, queryClient } = renderPage(true);
    await waitFor(() =>
      expect(screen.getByTestId("models").textContent).toBe("openai"),
    );
    await editKey();
    fireEvent.click(screen.getByRole("button", { name: copy.save }));
    unmount();
    const before = calls.length;
    await act(async () => response.resolve(Response.json({ provider: {} })));
    expect(calls).toHaveLength(before);
    expect(queryClient.getQueryState(MODELS_QUERY_KEY)?.isInvalidated).toBe(
      false,
    );
  });
});

it("refreshes models when a save succeeds after closing settings in the same account", async () => {
  const response = deferred<Response>();
  const calls = installGateway(MANAGED, undefined, undefined, response.promise);
  const { closeSettings } = renderPage(true);
  await waitFor(() =>
    expect(screen.getByTestId("models").textContent).toBe("openai"),
  );
  await editKey();
  fireEvent.click(screen.getByRole("button", { name: copy.save }));
  closeSettings();
  const statusReads = calls.filter((call) =>
    call.url.endsWith("/api/provider-keys"),
  ).length;
  await act(async () => response.resolve(Response.json({ provider: {} })));
  await waitFor(() =>
    expect(screen.getByTestId("models").textContent).toBe("openai,anthropic"),
  );
  expect(
    calls.filter((call) => call.url.endsWith("/api/provider-keys")),
  ).toHaveLength(statusReads);
});

it("retires a probe while its JSON body is still arriving", async () => {
  const body = deferred<{ result: string }>();
  const json = rs.fn(() => body.promise);
  installGateway(
    MANAGED,
    undefined,
    async () => ({ ok: true, json }) as unknown as Response,
  );
  renderPage();
  const input = await editKey();
  fireEvent.click(screen.getByRole("button", { name: copy.test }));
  await waitFor(() => expect(json).toHaveBeenCalledTimes(1));
  fireEvent.change(input, { target: { value: "replacement" } });
  await act(async () => body.resolve({ result: "accepted" }));
  expect(screen.queryByText(copy.testAccepted)).toBeNull();
});

it("aborts an in-flight models query when its owning cache is cleared", async () => {
  const response = deferred<Response>();
  const calls = installGateway(MANAGED);
  const { queryClient } = renderPage(true);
  await waitFor(() =>
    expect(screen.getByTestId("models").textContent).toBe("openai"),
  );
  rs.spyOn(globalThis, "fetch").mockImplementation((_input, init) => {
    calls.push({ url: "pending-models", init });
    return response.promise;
  });
  let refresh!: Promise<void>;
  act(() => {
    refresh = queryClient.invalidateQueries({ queryKey: MODELS_QUERY_KEY });
  });
  const request = calls.find((call) => call.url === "pending-models");
  expect(request?.init?.signal?.aborted).toBe(false);
  const previousLocation = window.location.href;
  act(() => queryClient.clear());
  expect(request?.init?.signal?.aborted).toBe(true);
  await act(async () => {
    response.resolve(new Response(null, { status: 401 }));
    await refresh;
  });
  expect(window.location.href).toBe(previousLocation);
  expect(calls.filter((call) => call.url === "pending-models")).toHaveLength(1);
});
