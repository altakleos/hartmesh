import { afterEach, expect, rs, test } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

rs.mock("next/navigation", () => ({ usePathname: () => "/workspace" }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => ({}) }));
const aboutState = rs.hoisted(() => ({
  answered: true,
  error: false,
  markdown: undefined as string | undefined,
}));
rs.mock("@tanstack/react-query", () => ({
  useQuery: ({
    select,
  }: {
    select: (features: FeaturesResponse) => unknown;
  }) => ({
    data: aboutState.answered
      ? select({
          agents_api: { enabled: false },
          ui: { about_markdown: aboutState.markdown },
          branding: {
            company_name: "Private Tenant",
            has_logo: true,
            provider: {
              display_name: "Tenant Provider",
              support_url: "https://tenant.example.test",
            },
          },
        })
      : undefined,
    isError: aboutState.error,
  }),
}));
rs.mock("@/components/ui/sidebar", () => ({
  useSidebar: () => ({ state: "expanded" }),
  SidebarTrigger: () => null,
  SidebarMenu: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  SidebarMenuItem: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  SidebarMenuButton: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
}));
rs.mock("@/env", () => ({ env: {} }));

import { AboutSettingsPage } from "@/components/workspace/settings/about-settings-page";
import { WorkspaceHeader } from "@/components/workspace/workspace-header";
import type { FeaturesResponse } from "@/core/features";
import { useBranding, useDocumentTitle } from "@/core/features/hooks";
import { useProductName } from "@/core/i18n/context";
import {
  RuntimeProductProvider,
  ProviderIdentity,
  ProductLogo,
} from "@/core/product/context";
import {
  parseProductIdentity,
  DEFAULT_IDENTITY,
  type ProductIdentity,
} from "@/core/product/identity";

const png = "data:image/png;base64,iVBORw0KGgo=";
function profile(number: string): ProductIdentity {
  return {
    schema_version: 1,
    mode: "configured",
    name: `Example Product ${number}`,
    product: {
      name: `Example Product ${number}`,
      logo: png,
      browser_icon: png,
    },
    provider: {
      name: `Example Provider ${number}`,
      logo: png,
      support_url: `https://support.example.test/${number}`,
    },
  };
}
function Surface() {
  const name = useProductName();
  const brand = useBranding();
  useDocumentTitle("Chats", name);
  return (
    <>
      <h1>{name}</h1>
      <ProductLogo />
      <ProviderIdentity />
      <span>{brand.providerName}</span>
    </>
  );
}
afterEach(() => {
  cleanup();
  document.title = "";
  aboutState.answered = true;
  aboutState.error = false;
  aboutState.markdown = undefined;
});

test.each(["One", "Two"])("complete public identity for %s", (number) => {
  const data = profile(number);
  expect(parseProductIdentity(data)).toEqual(data);
  render(
    <RuntimeProductProvider snapshot={data} initialLocale="en-US">
      <Surface />
    </RuntimeProductProvider>,
  );
  expect(screen.getByRole("heading").textContent).toBe(data.name);
  expect(document.title).toBe(`Chats - ${data.name}`);
  expect(
    document.querySelector<HTMLLinkElement>('link[rel="icon"]')?.href,
  ).toBe(png);
  expect(
    screen
      .getByRole("link", { name: `${data.provider?.name} support` })
      .getAttribute("href"),
  ).toBe(data.provider?.support_url);
  expect(screen.getByRole("img", { name: `${data.name} logo` })).toBeTruthy();
  expect(screen.queryByText("Tenant Provider")).toBeNull();
});

test("tenant cannot replace configured shell or product information", () => {
  render(
    <RuntimeProductProvider snapshot={profile("One")} initialLocale="en-US">
      <WorkspaceHeader />
      <AboutSettingsPage />
    </RuntimeProductProvider>,
  );
  expect(screen.queryByText("Private Tenant")).toBeNull();
  expect(
    screen.getByRole("heading", { name: "Example Product One" }),
  ).toBeTruthy();
  expect(screen.getByText(/HartMesh component version/)).toBeTruthy();
});

test.each([DEFAULT_IDENTITY, profile("One")])(
  "operator Markdown replaces the entire About panel with $mode identity",
  (identity) => {
    aboutState.markdown = [
      "# Example Suite",
      "",
      "Release 2026.11",
      "",
      "| Component | Version |",
      "| --- | --- |",
      "| Document reader | 4.2 |",
      "| Search service | 3.1 |",
      "",
      "[Support portal](https://help.example.test/)",
      "",
      '<iframe src="https://example.test/"></iframe>',
      "",
      "[Unsafe](javascript:alert%281%29)",
    ].join("\n");

    const { container } = render(
      <RuntimeProductProvider snapshot={identity} initialLocale="en-US">
        <AboutSettingsPage />
      </RuntimeProductProvider>,
    );

    expect(screen.getByRole("heading", { name: "Example Suite" })).toBeTruthy();
    expect(screen.getByText("Release 2026.11")).toBeTruthy();
    expect(screen.getByRole("cell", { name: "Document reader" })).toBeTruthy();
    expect(screen.getByRole("cell", { name: "4.2" })).toBeTruthy();
    expect(
      screen.getByRole("link", { name: "Support portal" }).getAttribute("href"),
    ).toBe("https://help.example.test/");
    expect(screen.queryByRole("img")).toBeNull();
    expect(
      screen.queryByRole("link", { name: /provider.*support/i }),
    ).toBeNull();
    expect(
      screen.queryByText(/HartMesh|Workspace version|Private Tenant/),
    ).toBeNull();
    expect(container.querySelector("iframe, [href^='javascript:']")).toBeNull();
  },
);

test("the built-in About waits for an answer before displaying default content", () => {
  aboutState.answered = false;
  const panel = (
    <RuntimeProductProvider snapshot={profile("One")} initialLocale="en-US">
      <AboutSettingsPage />
    </RuntimeProductProvider>
  );
  const { rerender } = render(panel);
  expect(screen.getByRole("status").textContent).toBe("Loading...");
  expect(screen.queryByText(/HartMesh component version/)).toBeNull();

  aboutState.answered = true;
  rerender(
    <RuntimeProductProvider snapshot={profile("One")} initialLocale="en-US">
      <AboutSettingsPage />
    </RuntimeProductProvider>,
  );
  expect(screen.getByText(/HartMesh component version/)).toBeTruthy();
  expect(
    screen.getByRole("link", { name: "Example Provider One support" }),
  ).toBeTruthy();
});

test("an unavailable features response does not reveal the default About panel", () => {
  aboutState.answered = false;
  aboutState.error = true;
  render(
    <RuntimeProductProvider snapshot={profile("One")} initialLocale="en-US">
      <AboutSettingsPage />
    </RuntimeProductProvider>,
  );
  expect(screen.getByRole("status").textContent).toBe(
    "Gateway is temporarily unavailable.",
  );
  expect(screen.queryByText(/HartMesh component version/)).toBeNull();
});

test("an explicitly empty About replaces the built-in panel", () => {
  aboutState.markdown = "";
  const { container } = render(
    <RuntimeProductProvider snapshot={profile("One")} initialLocale="en-US">
      <AboutSettingsPage />
    </RuntimeProductProvider>,
  );
  expect(container.textContent).toBe("");
  expect(screen.queryByRole("img")).toBeNull();
  expect(screen.queryByRole("link")).toBeNull();
});

test("fresh unavailable page has no invented identity or children", () => {
  render(
    <RuntimeProductProvider snapshot={null} initialLocale="en-US">
      <Surface />
    </RuntimeProductProvider>,
  );
  expect(screen.getByRole("heading").textContent).toBe(
    "Service identity unavailable",
  );
  expect(screen.queryByText("HartMesh")).toBeNull();
  expect(screen.queryByRole("link")).toBeNull();
});

test("established page retains the complete identity when reader becomes unavailable", () => {
  const { rerender } = render(
    <RuntimeProductProvider snapshot={profile("Two")} initialLocale="en-US">
      <Surface />
    </RuntimeProductProvider>,
  );
  rerender(
    <RuntimeProductProvider snapshot={null} initialLocale="en-US">
      <Surface />
    </RuntimeProductProvider>,
  );
  expect(screen.getByRole("heading").textContent).toBe("Example Product Two");
  expect(screen.getByRole("link").getAttribute("href")).toBe(
    "https://support.example.test/Two",
  );
  expect(document.title).toBe("Chats - Example Product Two");
  expect(screen.getByRole("status")).toBeTruthy();
});

test("unknown, incomplete and mixed identities reject; default remains supported", () => {
  expect(parseProductIdentity(DEFAULT_IDENTITY)).toEqual(DEFAULT_IDENTITY);
  for (const bad of [
    { ...profile("One"), secret: "x" },
    { ...profile("One"), name: "Other" },
    { ...profile("One"), provider: null },
  ])
    expect(parseProductIdentity(bad)).toBeNull();
});

test("name bounds count Unicode scalar values consistently with the Gateway", () => {
  const body = profile("One");
  body.name = body.product.name = "😀".repeat(40);
  expect(parseProductIdentity(body)).not.toBeNull();
  body.name = body.product.name = "😀".repeat(41);
  expect(parseProductIdentity(body)).toBeNull();
});

// UiConfig predates the strict mounted contract and permits joiners in names.
test("unconfigured mode preserves legacy Unicode names without relaxing configured profiles", () => {
  const legacy = "Example\u200dAssist";
  const body = {
    ...DEFAULT_IDENTITY,
    name: legacy,
    product: { ...DEFAULT_IDENTITY.product, name: legacy },
  };
  expect(parseProductIdentity(body)?.name).toBe(legacy);
  const strict = profile("One");
  strict.name = strict.product.name = legacy;
  expect(parseProductIdentity(strict)).toBeNull();
});
