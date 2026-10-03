"use client";

import {
  ChevronsUpDown,
  DownloadIcon,
  InfoIcon,
  LifeBuoyIcon,
  Settings2Icon,
  SettingsIcon,
} from "lucide-react";
import { useEffect, useState } from "react";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { useBranding } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { isStaticWebsiteOnly } from "@/core/static-mode";

import { AccountExportDialog } from "./account-export-dialog";
import { useSettingsDialog } from "./settings";

function NavMenuButtonContent({
  isSidebarOpen,
  t,
}: {
  isSidebarOpen: boolean;
  t: ReturnType<typeof useI18n>["t"];
}) {
  return isSidebarOpen ? (
    <div className="text-muted-foreground flex w-full items-center gap-2 text-left text-sm">
      <SettingsIcon className="size-4" />
      <span>{t.workspace.settingsAndMore}</span>
      <ChevronsUpDown className="text-muted-foreground ml-auto size-4" />
    </div>
  ) : (
    <div className="flex size-full items-center justify-center">
      <SettingsIcon className="text-muted-foreground size-4" />
    </div>
  );
}

export function WorkspaceNavMenu() {
  const { openSettings } = useSettingsDialog();
  const [mounted, setMounted] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const { open: isSidebarOpen } = useSidebar();
  const { t } = useI18n();
  // About is the company's when the tenant bundle names one, else the product's.
  const { companyName, providerName, supportURL } = useBranding();

  useEffect(() => {
    setMounted(true);
  }, []);

  return (
    <>
      <SidebarMenu className="w-full">
        <SidebarMenuItem>
          {mounted ? (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <SidebarMenuButton
                  size="lg"
                  className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
                >
                  <NavMenuButtonContent isSidebarOpen={isSidebarOpen} t={t} />
                </SidebarMenuButton>
              </DropdownMenuTrigger>
              <DropdownMenuContent
                className="w-(--radix-dropdown-menu-trigger-width) min-w-56 rounded-lg"
                align="end"
                sideOffset={4}
              >
                <DropdownMenuGroup>
                  <DropdownMenuItem
                    onClick={() => {
                      openSettings("appearance");
                    }}
                  >
                    <Settings2Icon />
                    {t.common.settings}
                  </DropdownMenuItem>
                  {/* A static demo has no Gateway to prepare it. */}
                  {!isStaticWebsiteOnly() && (
                    <DropdownMenuItem onClick={() => setExportOpen(true)}>
                      <DownloadIcon />
                      {t.accountExport.menuItem}
                    </DropdownMenuItem>
                  )}
                </DropdownMenuGroup>
                <DropdownMenuSeparator />
                {supportURL && (
                  <DropdownMenuItem asChild>
                    <a
                      href={supportURL}
                      target="_blank"
                      rel="noopener noreferrer"
                      referrerPolicy="no-referrer"
                    >
                      <LifeBuoyIcon />
                      {providerName
                        ? t.workspace.providerSupport(providerName)
                        : t.workspace.contactSupport}
                    </a>
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem
                  onClick={() => {
                    openSettings("about");
                  }}
                >
                  <InfoIcon />
                  {companyName === null
                    ? t.workspace.about
                    : t.workspace.aboutCompany(companyName)}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          ) : (
            <SidebarMenuButton size="lg" className="pointer-events-none">
              <NavMenuButtonContent isSidebarOpen={isSidebarOpen} t={t} />
            </SidebarMenuButton>
          )}
        </SidebarMenuItem>
      </SidebarMenu>
      {/* Mounted only while open, so it never shows what an earlier opening saw. */}
      {exportOpen && <AccountExportDialog open onOpenChange={setExportOpen} />}
    </>
  );
}
