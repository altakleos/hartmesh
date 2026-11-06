import { afterEach, expect, rs, test } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { ReactNode } from "react";

rs.mock("next/headers", () => ({
  cookies: async () => ({ get: () => undefined }),
}));
rs.mock("next/navigation", () => ({
  useRouter: () => ({ push: () => undefined }),
  usePathname: () => "/workspace",
}));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/product/context", () => ({
  useProductIdentity: () => DEFAULT_IDENTITY,
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
rs.mock("@/core/file-areas/file-action-lifetime", () => ({
  FileActionLifetimeProvider: ({ children }: { children: ReactNode }) =>
    children,
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
  WorkspaceSidebar: () => null,
}));
rs.mock("@/core/extensions/hooks", () => ({
  ExtensionPageBootstrap: () => null,
}));
rs.mock("sonner", () => ({ Toaster: () => null }));

import { WorkspaceContent } from "@/app/workspace/workspace-content";
import { AuthProvider, useAuth } from "@/core/auth/AuthProvider";
import {
  presentationDigest,
  useTenantPresentation,
} from "@/core/features/tenant-presentation";
import { DEFAULT_IDENTITY } from "@/core/product/identity";

afterEach(() => {
  cleanup();
  rs.restoreAllMocks();
});

function requestURL(input: RequestInfo | URL): string {
  return typeof input === "string"
    ? input
    : input instanceof URL
      ? input.href
      : input.url;
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
  render(<AuthProvider initialUser={null}>{tree}</AuthProvider>);
  await screen.findByText("Example Tenant One");
  expect(
    fetch.mock.calls.filter(([url]) => requestURL(url).endsWith("/auth/me")),
  ).toHaveLength(1);
  fireEvent.click(screen.getByRole("button", { name: "Switch account" }));
  await screen.findByText("Example Tenant Two");
  expect(screen.queryByText("Example Tenant One")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
  await waitFor(() =>
    expect(screen.queryByText("Example Tenant Two")).toBeNull(),
  );
  releaseLogout(Response.json({}));
});
