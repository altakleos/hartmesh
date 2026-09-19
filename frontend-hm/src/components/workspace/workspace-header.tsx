"use client";

import { MessageSquarePlus } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";

import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarTrigger,
  useSidebar,
} from "@/components/ui/sidebar";
import { logoURL, useBranding } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { env } from "@/env";
import { cn } from "@/lib/utils";

const PRODUCT_NAME = "DeerFlow";
const PRODUCT_MARK = "DF";

/** The first letters of the first two words: what a collapsed sidebar shows for a company without a logo. */
export function companyMark(companyName: string): string {
  return companyName
    .split(/\s+/)
    .filter((word) => word.length > 0)
    .slice(0, 2)
    .map((word) => Array.from(word)[0]!.toUpperCase())
    .join("");
}

export function WorkspaceHeader({ className }: { className?: string }) {
  const { t } = useI18n();
  const { state } = useSidebar();
  const pathname = usePathname();
  const branding = useBranding();
  // Nothing until the deployment has answered: the wrong name is worse than
  // a beat with none.
  const name = branding.isLoading ? "" : (branding.companyName ?? PRODUCT_NAME);
  const mark = branding.isLoading
    ? ""
    : branding.companyName
      ? companyMark(branding.companyName)
      : PRODUCT_MARK;
  const logo = branding.hasLogo ? (
    <img
      src={logoURL()}
      alt=""
      className="size-6 shrink-0 object-contain"
      data-testid="tenant-logo"
    />
  ) : null;
  return (
    <>
      <div
        className={cn(
          "group/workspace-header flex h-12 flex-col justify-center",
          className,
        )}
      >
        {state === "collapsed" ? (
          <div className="group-has-data-[collapsible=icon]/sidebar-wrapper:-translate-y flex w-full cursor-pointer items-center justify-center">
            <div className="text-primary block pt-1 font-serif group-hover/workspace-header:hidden">
              {logo ?? mark}
            </div>
            <SidebarTrigger className="hidden pl-2 group-hover/workspace-header:block" />
          </div>
        ) : (
          <div className="flex items-center justify-between gap-2">
            {env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true" ? (
              <Link
                href="/"
                className="text-primary ml-2 flex items-center gap-2 font-serif"
              >
                {logo}
                {name}
              </Link>
            ) : (
              <div className="text-primary ml-2 flex cursor-default items-center gap-2 font-serif">
                {logo}
                {name}
              </div>
            )}
            <SidebarTrigger />
          </div>
        )}
      </div>
      <SidebarMenu>
        <SidebarMenuItem>
          <SidebarMenuButton
            isActive={pathname === "/workspace/chats/new"}
            asChild
          >
            <Link className="text-muted-foreground" href="/workspace/chats/new">
              <MessageSquarePlus size={16} />
              <span>{t.sidebar.newChat}</span>
            </Link>
          </SidebarMenuButton>
        </SidebarMenuItem>
      </SidebarMenu>
    </>
  );
}
