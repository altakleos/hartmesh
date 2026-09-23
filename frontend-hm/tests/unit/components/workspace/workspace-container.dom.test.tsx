import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

rs.mock("next/navigation", () => ({ usePathname: () => "/workspace/chats" }));
rs.mock("@/components/ui/sidebar", () => ({ SidebarTrigger: () => null }));

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

afterEach(cleanup);

/**
 * The page header leads only to places in the workspace. The framework's
 * repository is not a place someone using the product has any reason to go.
 */
describe("workspace page header", () => {
  it("links nowhere outside the workspace", () => {
    renderHeader();

    for (const link of screen.queryAllByRole("link")) {
      expect(link.getAttribute("href")).toMatch(/^\//);
    }
  });
});
