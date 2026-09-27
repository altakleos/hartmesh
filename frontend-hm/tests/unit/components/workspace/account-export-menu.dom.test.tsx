import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const staticMode = rs.hoisted(() => ({ value: false }));
rs.mock("@/core/static-mode", () => ({
  isStaticWebsiteOnly: () => staticMode.value,
}));
rs.mock("@/core/features", () => ({
  useBranding: () => ({ companyName: null, primary: null }),
}));
// The dialog has its own tests; here only whether the menu opens it.
rs.mock("@/components/workspace/account-export-dialog", () => ({
  AccountExportDialog: ({ open }: { open: boolean }) =>
    open ? <div role="dialog">account export dialog</div> : null,
}));

import { SidebarProvider } from "@/components/ui/sidebar";
import { WorkspaceNavMenu } from "@/components/workspace/workspace-nav-menu";
import { I18nProvider } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales";

const t = enUS.accountExport;

async function openMenu() {
  render(
    <I18nProvider initialLocale="en-US">
      <SidebarProvider>
        <WorkspaceNavMenu />
      </SidebarProvider>
    </I18nProvider>,
  );
  const trigger = await screen.findByRole("button", { name: /settings/i });
  fireEvent.pointerDown(trigger, { button: 0, ctrlKey: false });
  await screen.findByRole("menu");
}

afterEach(() => {
  cleanup();
  staticMode.value = false;
});

describe("the menu's Download all my data", () => {
  it("opens the dialog", async () => {
    await openMenu();
    fireEvent.click(screen.getByRole("menuitem", { name: t.menuItem }));
    expect(await screen.findByRole("dialog")).toBeTruthy();
  });

  it("is not offered by a static demo, which has no Gateway", async () => {
    staticMode.value = true;
    await openMenu();
    expect(screen.queryByRole("menuitem", { name: t.menuItem })).toBeNull();
  });
});
