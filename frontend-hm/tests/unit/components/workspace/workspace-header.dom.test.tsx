import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";

const branding: {
  companyName: string | null;
  hasLogo: boolean;
  isLoading: boolean;
} = { companyName: null, hasLogo: false, isLoading: false };
const sidebar: { state: "expanded" | "collapsed"; isMobile: boolean } = {
  state: "expanded",
  isMobile: false,
};

rs.mock("next/navigation", () => ({ usePathname: () => "/workspace" }));
rs.mock("@/env", () => ({ env: {} }));
rs.mock("@/core/features", () => ({
  useBranding: () => branding,
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
  productInitials,
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
  sidebar.isMobile = false;
});

describe("WorkspaceHeader", () => {
  it("shows only the product when customer branding is configured", () => {
    branding.companyName = "Customer Example";
    branding.hasLogo = true;
    const view = renderHeader(createEnUS("Example Product"));
    expect(screen.getByText("Example Product")).toBeTruthy();
    expect(view.container.innerHTML).not.toContain(branding.companyName);
    expect(view.container.querySelector("img")).toBeNull();
    expect(screen.getByText(enUS.sidebar.newChat)).toBeTruthy();
  });

  it("uses product initials in the collapsed rail", () => {
    sidebar.state = "collapsed";
    branding.companyName = "Customer Example";
    branding.hasLogo = true;
    renderHeader(createEnUS("Example Product"));
    expect(screen.getByText("EP")).toBeTruthy();
    expect(screen.queryByText("CE")).toBeNull();
    expect(document.querySelector("img")).toBeNull();
  });

  it("shows the full product name in the phone drawer after desktop collapse", () => {
    sidebar.state = "collapsed";
    sidebar.isMobile = true;
    branding.companyName = "Customer Example";
    const view = renderHeader(createEnUS("Example Product"));
    expect(screen.getByText("Example Product")).toBeTruthy();
    expect(view.container.innerHTML).not.toContain(branding.companyName);
  });

  it("renders the default product independently of pending tenant features", () => {
    branding.isLoading = true;
    renderHeader();
    expect(screen.getByText("HartMesh")).toBeTruthy();
  });
});

describe("productInitials", () => {
  it("takes the first letters of the first two words", () => {
    expect(productInitials("Example Services Co.")).toBe("ES");
    expect(productInitials("Acme")).toBe("A");
    expect(productInitials("  two   words here ")).toBe("TW");
    expect(productInitials("🦊 Foxes")).toBe("🦊F");
  });
});
