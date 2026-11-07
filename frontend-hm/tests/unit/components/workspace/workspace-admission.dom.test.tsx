import { afterEach, expect, rs, test } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { cloneElement, type ReactElement, type ReactNode } from "react";

const routing = rs.hoisted(() => ({ pathname: "/workspace/chats/new" }));
const server = rs.hoisted(() => ({
  result: { tag: "gateway_unavailable" } as unknown,
}));
rs.mock("next/headers", () => ({
  cookies: async () => ({ get: () => undefined }),
}));
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: () => undefined }),
  usePathname: () => routing.pathname,
}));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/product/context", () => ({
  useProductIdentity: () => DEFAULT_IDENTITY,
  RuntimeProductProvider: ({ children }: { children: ReactNode }) => children,
}));
rs.mock("@/core/auth/server", () => ({
  getServerSideUser: async () => server.result,
}));
rs.mock("@/core/i18n/server", () => ({
  detectLocaleServer: async () => "en-US",
}));
rs.mock("@/core/product/server", () => ({
  getServerSideProductIdentity: async () => DEFAULT_IDENTITY,
  productMetadata: async () => ({}),
}));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({
    t: {
      workspace: {
        logout: "Log out",
        gatewayUnavailable: "Gateway unavailable",
        gatewayUnavailableRetrying: "Retrying",
      },
    },
  }),
}));
rs.mock("@/components/query-client-provider", () => ({
  QueryClientProvider: ({ children }: { children: ReactNode }) => children,
}));
rs.mock("@/components/ui/sidebar", () => ({
  SidebarInset: ({ children }: { children: ReactNode }) => children,
  SidebarProvider: ({ children }: { children: ReactNode }) => children,
}));
rs.mock("@/components/workspace/command-palette", () => ({
  CommandPalette: () => null,
}));
rs.mock("@/components/workspace/model-load-error-banner", () => ({
  ModelLoadErrorBanner: () => null,
}));
rs.mock("@/components/workspace/settings", () => ({
  SettingsDialogHost: () => null,
}));
rs.mock("@/components/workspace/telegram/setup-host", () => ({
  TelegramSetupHost: () => null,
}));
rs.mock("@/components/workspace/workspace-settings-deep-link", () => ({
  WorkspaceSettingsDeepLink: () => null,
}));
rs.mock("@/components/workspace/workspace-sidebar", () => ({
  WorkspaceSidebar: () => <span>Private workspace chrome</span>,
}));
rs.mock("@/core/extensions/hooks", () => ({
  ExtensionPageBootstrap: () => null,
}));
rs.mock("sonner", () => ({ Toaster: () => null, toast: { dismiss: rs.fn() } }));

import WorkspaceLayout from "@/app/workspace/layout";
import { WorkspaceContent } from "@/app/workspace/workspace-content";
import { AuthProvider, useAuth } from "@/core/auth/AuthProvider";
import {
  presentationDigest,
  useTenantPresentation,
} from "@/core/features/tenant-presentation";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { DEFAULT_IDENTITY } from "@/core/product/identity";

afterEach(() => {
  cleanup();
  rs.restoreAllMocks();
  routing.pathname = "/workspace/chats/new";
});

function requestURL(input: RequestInfo | URL): string {
  return typeof input === "string"
    ? input
    : input instanceof URL
      ? input.href
      : input.url;
}

async function resolvedLayout(children: ReactNode) {
  // React DOM cannot execute async Server Components. Resolve only the actual
  // WorkspaceContent child as Next would, retaining the layout's client types.
  const outer = (await WorkspaceLayout({ children })) as ReactElement<{
    children: ReactElement<{
      children: ReactElement<Parameters<typeof WorkspaceContent>[0]>;
    }>;
  }>;
  const auth = outer.props.children;
  const workspace = await WorkspaceContent(auth.props.children.props);
  return cloneElement(outer, {}, cloneElement(auth, {}, workspace));
}

test("actual workspace composition recovers auth before tenant admission and retires account snapshots", async () => {
  const one = {
    id: "one",
    email: "one@example.com",
    system_role: "user" as const,
    needs_setup: false,
  };
  const two = { ...one, id: "two", email: "two@example.com" };
  const bodies = await Promise.all(
    ["One", "Two"].map(async (label) => {
      const body = {
        schema_version: 1,
        mode: "tenant",
        product_sha256: await presentationDigest(DEFAULT_IDENTITY),
        tenant: {
          name: `Example Tenant ${label}`,
          logo: "data:image/png;base64,iVBORw0KGgo=",
        },
      };
      return { ...body, snapshot_id: await presentationDigest(body) };
    }),
  );
  let selected = 0;
  let lifetime: ReturnType<typeof useFileActionLifetime> | undefined;
  let releaseLogout: (response: Response) => void = () => undefined;
  const fetch = rs
    .spyOn(globalThis, "fetch")
    .mockImplementation(async (input) => {
      if (requestURL(input).endsWith("/auth/me")) return Response.json(one);
      if (requestURL(input).endsWith("/auth/logout"))
        return new Promise<Response>((resolve) => {
          releaseLogout = resolve;
        });
      if (requestURL(input).endsWith("/branding/presentation"))
        return Response.json(bodies[selected]);
      throw new Error("Unexpected transport");
    });
  function Probe() {
    lifetime = useFileActionLifetime();
    const auth = useAuth();
    const snapshot = useTenantPresentation();
    return (
      <>
        <p>
          {snapshot?.mode === "tenant" ? snapshot.tenant?.name : "no tenant"}
        </p>
        <button
          onClick={() => {
            selected = 1;
            auth.applyUser(two);
          }}
        >
          Switch account
        </button>
        <button onClick={() => void auth.logout()}>Sign out</button>
      </>
    );
  }
  const tree = await WorkspaceContent({
    gatewayUnavailable: true,
    children: <Probe />,
  });
  const app = render(<AuthProvider initialUser={null}>{tree}</AuthProvider>);
  await screen.findByText("Example Tenant One");
  const firstLifetime = lifetime!;
  const firstSignal = firstLifetime.signal;
  firstLifetime.toasts.add("pending-file-notice");
  expect(
    fetch.mock.calls.filter(([url]) => requestURL(url).endsWith("/auth/me")),
  ).toHaveLength(1);
  selected = 1;
  routing.pathname = "/workspace";
  const index = await WorkspaceContent({
    gatewayUnavailable: true,
    children: <p>Index route control</p>,
  });
  app.rerender(<AuthProvider initialUser={null}>{index}</AuthProvider>);
  await screen.findByText("Index route control");
  expect(screen.getByText("Private workspace chrome")).toBeTruthy();
  expect(firstSignal.aborted).toBe(false);
  expect(firstLifetime.toasts.has("pending-file-notice")).toBe(true);
  routing.pathname = "/workspace/chats/new";
  app.rerender(<AuthProvider initialUser={null}>{tree}</AuthProvider>);
  await screen.findByText("Example Tenant One");
  expect(lifetime).toBe(firstLifetime);
  expect(
    fetch.mock.calls.filter(([url]) =>
      requestURL(url).endsWith("/branding/presentation"),
    ),
  ).toHaveLength(1);
  fireEvent.click(screen.getByRole("button", { name: "Switch account" }));
  await screen.findByText("Example Tenant Two");
  expect(firstSignal.aborted).toBe(true);
  expect(firstLifetime.toasts.size).toBe(0);
  expect(screen.queryByText("Example Tenant One")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
  await waitFor(() =>
    expect(screen.queryByText("Example Tenant Two")).toBeNull(),
  );
  releaseLogout(Response.json({}));
});

test("unverified workspace entry can run pure route control while private UI stays closed", async () => {
  routing.pathname = "/workspace";
  const fetch = rs
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(Response.json({}, { status: 503 }));
  const tree = await WorkspaceContent({
    gatewayUnavailable: true,
    children: <p>Index route control</p>,
  });
  render(<AuthProvider initialUser={null}>{tree}</AuthProvider>);
  await screen.findByText("Index route control");
  expect(screen.queryByText("Private workspace chrome")).toBeNull();
  expect(
    fetch.mock.calls.filter(([url]) =>
      requestURL(url).endsWith("/branding/presentation"),
    ),
  ).toHaveLength(0);
});

test.each(["account", "role", "permissions"])(
  "actual layout retains uncertain SSR context and retires confirmed %s changes",
  async (change) => {
    const one = {
      id: "one",
      email: "one@example.com",
      system_role: "user" as const,
      needs_setup: false,
    };
    const two = { ...one, id: "two", email: "two@example.com" };
    const bodies = await Promise.all(
      ["One", "Two"].map(async (label) => {
        const body = {
          schema_version: 1,
          mode: "tenant",
          product_sha256: await presentationDigest(DEFAULT_IDENTITY),
          tenant: {
            name: `Example Tenant ${label}`,
            logo: "data:image/png;base64,iVBORw0KGgo=",
          },
        };
        return { ...body, snapshot_id: await presentationDigest(body) };
      }),
    );
    let selected = 0;
    let lifetime: ReturnType<typeof useFileActionLifetime> | undefined;
    const fetch = rs
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input) =>
        requestURL(input).endsWith("/auth/me")
          ? Response.json(one)
          : Response.json(bodies[selected]),
      );
    const reload = rs
      .spyOn(window.location, "reload")
      .mockImplementation(() => undefined);
    function Probe() {
      lifetime = useFileActionLifetime();
      const snapshot = useTenantPresentation();
      return (
        <p>
          {snapshot?.mode === "tenant" ? snapshot.tenant?.name : "no tenant"}
        </p>
      );
    }
    server.result = { tag: "authenticated", user: one };
    const app = render(await resolvedLayout(<Probe />));
    await screen.findByText("Example Tenant One");
    const original = lifetime!;
    selected = 1;
    server.result = { tag: "gateway_unavailable" };
    app.rerender(await resolvedLayout(<Probe />));
    await screen.findByText("Example Tenant One");
    expect(original.signal.aborted).toBe(false);
    expect(lifetime).toBe(original);
    server.result = { tag: "authenticated", user: { ...one } };
    app.rerender(await resolvedLayout(<Probe />));
    await screen.findByText("Example Tenant One");
    expect(
      fetch.mock.calls.filter(([url]) =>
        requestURL(url).endsWith("/branding/presentation"),
      ),
    ).toHaveLength(1);
    const changed =
      change === "account"
        ? two
        : change === "role"
          ? { ...one, system_role: "admin" }
          : { ...one, permissions: ["threads:read"] };
    server.result = { tag: "authenticated", user: changed };
    app.rerender(await resolvedLayout(<Probe />));
    await waitFor(() =>
      expect(screen.queryByText("Example Tenant One")).toBeNull(),
    );
    expect(original.signal.aborted).toBe(true);
    expect(reload).toHaveBeenCalledTimes(1);
    for (const result of [
      { tag: "gateway_unavailable" },
      { tag: "authenticated", user: one },
    ]) {
      server.result = result;
      app.rerender(await resolvedLayout(<Probe />));
      expect(screen.queryByText("Example Tenant One")).toBeNull();
      expect(screen.queryByText("Private workspace chrome")).toBeNull();
      expect(
        fetch.mock.calls.filter(([url]) =>
          requestURL(url).endsWith("/branding/presentation"),
        ),
      ).toHaveLength(1);
      expect(reload).toHaveBeenCalledTimes(1);
    }
  },
);

test.each(["account", "role", "permissions"])(
  "actual layout permits a verified client %s refresh while SSR props stay unchanged",
  async (change) => {
    const one = {
      id: "one",
      email: "one@example.com",
      system_role: "admin" as const,
      needs_setup: false,
    };
    const next =
      change === "account"
        ? { ...one, id: "two", email: "two@example.com" }
        : change === "role"
          ? { ...one, system_role: "user" as const }
          : { ...one, permissions: ["threads:read"] };
    const bodies = await Promise.all(
      ["One", "Two"].map(async (label) => {
        const body = {
          schema_version: 1,
          mode: "tenant",
          product_sha256: await presentationDigest(DEFAULT_IDENTITY),
          tenant: {
            name: `Example Tenant ${label}`,
            logo: "data:image/png;base64,iVBORw0KGgo=",
          },
        };
        return { ...body, snapshot_id: await presentationDigest(body) };
      }),
    );
    let selected = 0;
    let lifetime: ReturnType<typeof useFileActionLifetime> | undefined;
    rs.spyOn(globalThis, "fetch").mockImplementation(async (input) =>
      requestURL(input).endsWith("/auth/me")
        ? Response.json(next)
        : Response.json(bodies[selected]),
    );
    const reload = rs
      .spyOn(window.location, "reload")
      .mockImplementation(() => undefined);
    function Probe() {
      const auth = useAuth();
      lifetime = useFileActionLifetime();
      const snapshot = useTenantPresentation();
      return (
        <>
          <p>{snapshot?.mode === "tenant" ? snapshot.tenant?.name : null}</p>
          <button onClick={() => void auth.refreshUser()}>
            Refresh account
          </button>
        </>
      );
    }
    server.result = { tag: "authenticated", user: one };
    const app = render(await resolvedLayout(<Probe />));
    await screen.findByText("Example Tenant One");
    const original = lifetime!;
    selected = 1;
    fireEvent.click(screen.getByRole("button", { name: "Refresh account" }));
    await screen.findByText("Example Tenant Two");
    expect(original.signal.aborted).toBe(true);
    expect(reload).not.toHaveBeenCalled();
    // A subsequent consistent SSR result preserves the new account. A newly
    // delivered conflicting result must still retire it before fresh checks.
    server.result = { tag: "authenticated", user: { ...next } };
    app.rerender(await resolvedLayout(<Probe />));
    expect(screen.queryByText("Example Tenant Two")).not.toBeNull();
    expect(reload).not.toHaveBeenCalled();
    server.result = { tag: "authenticated", user: { ...one } };
    app.rerender(await resolvedLayout(<Probe />));
    expect(screen.queryByText("Example Tenant Two")).toBeNull();
    expect(reload).toHaveBeenCalledTimes(1);
  },
);

test("a batched client refresh cannot swallow newly delivered conflicting SSR evidence", async () => {
  const one = {
    id: "one",
    email: "one@example.com",
    system_role: "user" as const,
    needs_setup: false,
  };
  let release: (response: Response) => void = () => undefined;
  let refresh: Promise<void> | undefined;
  rs.spyOn(globalThis, "fetch").mockImplementation(async (input) =>
    requestURL(input).endsWith("/auth/me")
      ? new Promise<Response>((resolve) => {
          release = resolve;
        })
      : Response.json({ schema_version: 1, mode: "legacy" }),
  );
  const reload = rs
    .spyOn(window.location, "reload")
    .mockImplementation(() => undefined);
  function Probe() {
    const auth = useAuth();
    return (
      <>
        <p>Account {auth.user?.id}</p>
        <button
          onClick={() => {
            refresh = auth.refreshUser();
          }}
        >
          Refresh account
        </button>
      </>
    );
  }
  server.result = { tag: "authenticated", user: one };
  const app = render(await resolvedLayout(<Probe />));
  await screen.findByText("Account one");
  fireEvent.click(screen.getByRole("button", { name: "Refresh account" }));
  server.result = { tag: "authenticated", user: { ...one } };
  const freshSSR = await resolvedLayout(<Probe />);
  await act(async () => {
    release(Response.json({ ...one, id: "two" }));
    await refresh;
    app.rerender(freshSSR);
  });
  expect(screen.queryByText("Account two")).toBeNull();
  expect(screen.queryByText("Private workspace chrome")).toBeNull();
  expect(reload).toHaveBeenCalledTimes(1);
});

test("logout retires the layout monitor before delayed verified SSR props arrive", async () => {
  const one = {
    id: "one",
    email: "one@example.com",
    system_role: "user" as const,
    needs_setup: false,
  };
  const reload = rs
    .spyOn(window.location, "reload")
    .mockImplementation(() => undefined);
  let releaseLogout: (response: Response) => void = () => undefined;
  rs.spyOn(globalThis, "fetch").mockImplementation(async (input) =>
    requestURL(input).endsWith("/auth/logout")
      ? new Promise<Response>((resolve) => {
          releaseLogout = resolve;
        })
      : Response.json({ schema_version: 1, mode: "legacy" }),
  );
  function Probe() {
    const auth = useAuth();
    return <button onClick={() => void auth.logout()}>Sign out</button>;
  }
  server.result = { tag: "authenticated", user: one };
  const app = render(await resolvedLayout(<Probe />));
  fireEvent.click(await screen.findByRole("button", { name: "Sign out" }));
  await waitFor(() =>
    expect(screen.queryByText("Private workspace chrome")).toBeNull(),
  );
  server.result = { tag: "authenticated", user: { ...one, id: "two" } };
  app.rerender(await resolvedLayout(<Probe />));
  expect(screen.queryByText("Private workspace chrome")).toBeNull();
  expect(reload).not.toHaveBeenCalled();
  releaseLogout(Response.json({}));
});
