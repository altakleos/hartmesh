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
import { useProductName } from "@/core/i18n/context";
import { useI18n } from "@/core/i18n/hooks";
import { ProviderIdentity, useProductIdentity } from "@/core/product/context";
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
  const { state } = useSidebar();
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
  ) : !branding.strict && branding.hasLogo ? (
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
      <ProviderIdentity />
      {branding.strict && branding.companyName && (
        <div
          role="group"
          aria-label={`Workspace: ${branding.companyName}`}
          title={`Workspace: ${branding.companyName}`}
          data-testid="tenant-context"
          data-snapshot-id={branding.snapshotId}
          className="mx-1 flex min-w-0 items-center gap-2 rounded bg-white p-2 text-sm text-black group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:p-1"
        >
          <img
            src={branding.logo ?? undefined}
            alt=""
            width={32}
            height={32}
            className="size-8 shrink-0 object-contain group-data-[collapsible=icon]:size-5"
            data-testid="tenant-logo"
          />
          <span className="min-w-0 wrap-anywhere group-data-[collapsible=icon]:hidden">
            Workspace: <bdi>{branding.companyName}</bdi>
          </span>
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
