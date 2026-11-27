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
rs.mock("@/core/features", () => ({
  useBranding: () => branding,
  useFeatures: () => ({ data: null }),
}));
// The dialog has its own tests; here only whether the menu opens it.
rs.mock("@/components/workspace/account-export-dialog", () => ({
  AccountExportDialog: ({ open }: { open: boolean }) =>
    open ? <div role="dialog">account export dialog</div> : null,
}));

import { SidebarProvider } from "@/components/ui/sidebar";
import { AboutSettingsPage } from "@/components/workspace/settings/about-settings-page";
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

it("keeps About in the menu and provider support off the sidebar", async () => {
  branding.companyName = "Customer";
  branding.providerName = "Hosting";
  branding.supportURL = "https://help.example.test/";
  await openMenu();
  expect(screen.getByRole("menuitem", { name: "About HartMesh" })).toBeTruthy();
  expect(
    screen.queryByRole("menuitem", { name: /customer|support/i }),
  ).toBeNull();
});

describe("provider support in About", () => {
  it.each([
    ["Hosting", "Contact Hosting support"],
    [null, "Contact support"],
  ] as const)(
    "opens the configured destination with provider name %s",
    (provider, name) => {
      branding.providerName = provider;
      branding.supportURL = "https://help.example.test/";
      render(
        <I18nProvider initialLocale="en-US">
          <AboutSettingsPage />
        </I18nProvider>,
      );
      const link = screen.getByRole("link", { name });
      expect(link.getAttribute("href")).toBe(branding.supportURL);
      expect(link.getAttribute("target")).toBe("_blank");
      expect(link.getAttribute("rel")).toBe("noopener noreferrer");
      expect(link.getAttribute("referrerpolicy")).toBe("no-referrer");
    },
  );
  it("omits the link when no destination is configured", () => {
    render(
      <I18nProvider initialLocale="en-US">
        <AboutSettingsPage />
      </I18nProvider>,
    );
    expect(screen.queryByRole("link")).toBeNull();
  });
});
