import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";

const branding: {
  companyName: string | null;
  hasLogo: boolean;
  isLoading: boolean;
} = { companyName: null, hasLogo: false, isLoading: false };
const sidebar: { state: "expanded" | "collapsed" } = { state: "expanded" };

rs.mock("next/navigation", () => ({ usePathname: () => "/workspace" }));
rs.mock("@/env", () => ({ env: {} }));
rs.mock("@/core/features", () => ({
  useBranding: () => branding,
  logoURL: () => "/api/branding/logo",
}));
rs.mock("@/components/ui/sidebar", () => ({
  useSidebar: () => sidebar,
  SidebarTrigger: () => null,
  SidebarMenu: ({ children }: { children?: ReactNode }) => (
    <div>{children}</div>
  ),
  SidebarMenuItem: ({ children }: { children?: ReactNode }) => (
    <div>{children}</div>
  ),
  SidebarMenuButton: ({ children }: { children?: ReactNode }) => (
    <div>{children}</div>
  ),
}));

import {
  WorkspaceHeader,
  companyMark,
} from "@/components/workspace/workspace-header";
import { I18nContext } from "@/core/i18n/context";
import { createEnUS, enUS } from "@/core/i18n/locales/en-US";

function renderHeader(t = enUS) {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t }}
    >
      <WorkspaceHeader />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  cleanup();
  branding.companyName = null;
  branding.hasLogo = false;
  branding.isLoading = false;
  sidebar.state = "expanded";
});

/**
 * The header is the first thing a person sees after signing in, and it says
 * whose workspace this is. A named company is that name; no company is the
 * product's; while the deployment has not answered it is neither, because a
 * tenant watching the product's name flash before their own on every load is
 * the wrong first impression.
 */
describe("WorkspaceHeader", () => {
  it("shows the company the deployment named, with its logo", () => {
    branding.companyName = "Example Services Co.";
    branding.hasLogo = true;

    renderHeader();

    expect(screen.getByText("Example Services Co.")).toBeTruthy();
    expect(screen.queryByText("HartMesh")).toBeNull();
    expect(screen.getByTestId("tenant-logo").getAttribute("src")).toBe(
      "/api/branding/logo",
    );
  });

  it("shows the company without a picture when the bundle has none", () => {
    branding.companyName = "Example Services Co.";

    renderHeader();

    expect(screen.getByText("Example Services Co.")).toBeTruthy();
    expect(screen.queryByTestId("tenant-logo")).toBeNull();
  });

  it("is the product's own where no company is named", () => {
    renderHeader();

    expect(screen.getByText("HartMesh")).toBeTruthy();
    expect(screen.queryByTestId("tenant-logo")).toBeNull();
  });

  it("is the product the deployment named", () => {
    renderHeader(createEnUS("Acme Assist"));

    expect(screen.getByText("Acme Assist")).toBeTruthy();
  });

  it("shows neither name until the deployment has answered", () => {
    branding.isLoading = true;

    renderHeader();

    expect(screen.queryByText("HartMesh")).toBeNull();
    expect(screen.getByText(enUS.sidebar.newChat)).toBeTruthy();
  });

  it("collapses to the logo, else the company's mark, else the product's", () => {
    sidebar.state = "collapsed";
    branding.companyName = "Example Services Co.";
    branding.hasLogo = true;
    renderHeader();
    expect(screen.getByTestId("tenant-logo")).toBeTruthy();
    expect(screen.queryByText("ES")).toBeNull();
    cleanup();

    branding.hasLogo = false;
    renderHeader();
    expect(screen.getByText("ES")).toBeTruthy();
    cleanup();

    branding.companyName = null;
    renderHeader();
    expect(screen.getByText("H")).toBeTruthy();
    cleanup();

    renderHeader(createEnUS("Acme Assist"));
    expect(screen.getByText("AA")).toBeTruthy();
  });
});

describe("companyMark", () => {
  it("takes the first letters of the first two words", () => {
    expect(companyMark("Example Services Co.")).toBe("ES");
    expect(companyMark("Acme")).toBe("A");
    expect(companyMark("  two   words here ")).toBe("TW");
    expect(companyMark("🦊 Foxes")).toBe("🦊F");
  });
});
