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
import { useBranding } from "@/core/features";
import { useProductName } from "@/core/i18n/context";
import { useI18n } from "@/core/i18n/hooks";
import { useProductIdentity } from "@/core/product/context";
import { env } from "@/env";
import { cn } from "@/lib/utils";

/** The first letters of the first two words: what a collapsed sidebar shows without a logo. */
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
  const { state, isMobile } = useSidebar();
  const pathname = usePathname();
  const branding = useBranding();
  const identity = useProductIdentity();
  const configured = identity?.mode === "configured";
  const productName = useProductName();
  // Nothing until the deployment has answered: the wrong name is worse than
  // a beat with none.
  const name =
    configured || branding.strict
      ? (identity?.name ?? productName)
      : branding.isLoading
        ? ""
        : (branding.companyName ?? productName);
  const mark = name ? companyMark(name) : "";
  const logo = configured ? (
    <img
      src={identity.product.logo}
      alt=""
      className="size-6 shrink-0 rounded bg-white object-contain"
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
        {state === "collapsed" && !isMobile ? (
          <div className="group-has-data-[collapsible=icon]/sidebar-wrapper:-translate-y flex w-full cursor-pointer items-center justify-center">
            <div
              title={name}
              className="text-primary block pt-1 font-serif group-hover/workspace-header:hidden"
            >
              {logo && <span className="sr-only">{name}</span>}
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
      {branding.strict && branding.companyName && (
        <div
          role="group"
          aria-label={`${t.breadcrumb.workspace}: ${branding.companyName}`}
          title={branding.companyName}
          data-testid="tenant-context"
          data-snapshot-id={branding.snapshotId}
          className={cn(
            "text-sidebar-foreground min-w-0 truncate px-2 pb-1 text-xs group-data-[collapsible=icon]:sr-only",
            configured && "ps-10",
          )}
        >
          <bdi>{branding.companyName}</bdi>
        </div>
      )}
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
