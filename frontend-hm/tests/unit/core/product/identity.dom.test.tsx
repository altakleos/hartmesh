import { afterEach, expect, rs, test } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

rs.mock("next/navigation", () => ({ usePathname: () => "/workspace" }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => ({}) }));
rs.mock("@tanstack/react-query", () => ({
  useQuery: () => ({
    data: {
      companyName: "Private Tenant",
      hasLogo: true,
      providerName: "Tenant Provider",
      supportURL: "https://tenant.example.test",
    },
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
