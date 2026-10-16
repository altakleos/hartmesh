import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({ path: "/workspace/chats" }));
rs.mock("next/navigation", () => ({ usePathname: () => state.path }));
rs.mock("@/components/ui/sidebar", () => ({ SidebarTrigger: () => null }));

import { WorkspaceHeader } from "@/components/workspace/workspace-container";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";

function renderHeader(
  props: { breadcrumbSection?: { label: string; href: string } } = {},
  locale: "en-US" | "zh-CN" = "en-US",
) {
  return render(
    <I18nContext.Provider
      value={{
        locale,
        setLocale: () => undefined,
        t: locale === "en-US" ? enUS : zhCN,
      }}
    >
      <WorkspaceHeader {...props} />
    </I18nContext.Provider>,
  );
}

beforeEach(() => {
  state.path = "/workspace/chats";
});
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

for (const [path, label] of [
  ["assistants", "My assistants"],
  ["employees", "AI employees"],
]) {
  it(`localizes the ${path} list breadcrumb`, () => {
    state.path = `/workspace/${path}`;
    renderHeader();
    expect(screen.getByRole("link", { name: label }).getAttribute("href")).toBe(
      `/workspace/${path}`,
    );
  });
}
it("an instance detail returns to its verified list instead of the internal route", () => {
  state.path = "/workspace/instances";
  renderHeader({
    breadcrumbSection: { label: "AI employees", href: "/workspace/employees" },
  });
  expect(
    screen.getByRole("link", { name: "AI employees" }).getAttribute("href"),
  ).toBe("/workspace/employees");
  expect(screen.queryByText("Agent instances")).toBeNull();
});
for (const [path, label] of [
  ["assistants", "我的助理"],
  ["employees", "AI 员工"],
]) {
  it(`localizes the ${path} breadcrumb in Chinese`, () => {
    state.path = `/workspace/${path}`;
    renderHeader({}, "zh-CN");
    expect(screen.getByRole("link", { name: label }).getAttribute("href")).toBe(
      `/workspace/${path}`,
    );
  });
}
