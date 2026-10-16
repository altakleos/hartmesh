"use client";
import { BellIcon } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";

import { SidebarMenuButton, SidebarMenuItem } from "@/components/ui/sidebar";
import { useAttention } from "@/core/attention/hooks";
import { useI18n } from "@/core/i18n/hooks";

export function AttentionNav() {
  const query = useAttention();
  const { t } = useI18n();
  const path = usePathname();
  const counts = !query.isError && query.data?.counts;
  return (
    <SidebarMenuItem>
      <SidebarMenuButton
        isActive={path.startsWith("/workspace/attention")}
        asChild
      >
        <Link href="/workspace/attention">
          <BellIcon />
          <span>{t.attention.title}</span>
          {counts && (
            <span
              aria-label={t.attention.navCount(counts.pending, counts.routing)}
              title={t.attention.navCount(counts.pending, counts.routing)}
            >
              {counts.pending + counts.routing}
            </span>
          )}
        </Link>
      </SidebarMenuButton>
    </SidebarMenuItem>
  );
}
