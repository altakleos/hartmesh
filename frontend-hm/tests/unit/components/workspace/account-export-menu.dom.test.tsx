import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const staticMode = rs.hoisted(() => ({ value: false }));
rs.mock("@/core/static-mode", () => ({
  isStaticWebsiteOnly: () => staticMode.value,
}));
const branding = rs.hoisted(() => ({
  companyName: null as string | null,
  providerName: null as string | null,
  supportURL: null as string | null,
}));
rs.mock("@/core/features", () => ({ useBranding: () => branding }));
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
  branding.companyName = null;
  branding.providerName = null;
  branding.supportURL = null;
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

describe("the configured support entry", () => {
  it("names the provider without replacing the customer's About entry", async () => {
    branding.companyName = "Customer";
    branding.providerName = "Hosting";
    branding.supportURL = "https://help.example.test/";
    await openMenu();
    const link = screen.getByRole("menuitem", {
      name: "Contact Hosting support",
    });
    expect(link.getAttribute("href")).toBe("https://help.example.test/");
    expect(link.getAttribute("rel")).toBe("noopener noreferrer");
    expect(link.getAttribute("referrerpolicy")).toBe("no-referrer");
    expect(link.getAttribute("target")).toBe("_blank");
    expect(
      screen.getByRole("menuitem", { name: "About Customer" }),
    ).toBeTruthy();
  });
  it("uses a generic label if only a destination is set", async () => {
    branding.supportURL = "https://help.example.test/";
    await openMenu();
    expect(
      screen.getByRole("menuitem", { name: "Contact support" }),
    ).toBeTruthy();
  });
  it("offers no dead link while the destination is absent", async () => {
    branding.providerName = "Hosting";
    await openMenu();
    expect(screen.queryByRole("menuitem", { name: /Contact/ })).toBeNull();
  });
});
