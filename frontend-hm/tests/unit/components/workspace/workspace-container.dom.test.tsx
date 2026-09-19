import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";

const branding: { companyName: string | null; isLoading: boolean } = {
  companyName: null,
  isLoading: false,
};

rs.mock("next/navigation", () => ({ usePathname: () => "/workspace/chats" }));
rs.mock("@/core/features", () => ({ useBranding: () => branding }));
rs.mock("@/components/ui/sidebar", () => ({ SidebarTrigger: () => null }));
rs.mock("@/components/workspace/tooltip", () => ({
  Tooltip: ({ children }: { children?: ReactNode }) => <>{children}</>,
}));

import { WorkspaceHeader } from "@/components/workspace/workspace-container";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

function renderHeader() {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <WorkspaceHeader />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  cleanup();
  branding.companyName = null;
});

/**
 * The product's repository link belongs in the product's own workspace. In
 * a company's workspace it is a link to someone else's project.
 */
describe("workspace page header", () => {
  it("links to the product's repository where no company is named", () => {
    renderHeader();

    const link = screen.getByRole("link", {
      name: (name) => name.length === 0 || name.includes("GitHub"),
    });
    expect(link.getAttribute("href")).toBe(
      "https://github.com/bytedance/deer-flow",
    );
  });

  it("drops it in a company's workspace", () => {
    branding.companyName = "Example Services Co.";

    renderHeader();

    expect(
      screen
        .queryAllByRole("link")
        .some((link) => link.getAttribute("href")?.includes("github.com")),
    ).toBe(false);
  });
});
