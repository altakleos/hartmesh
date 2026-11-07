"use client";

import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import { TenantPresentationProvider } from "@/core/features/tenant-presentation";

/** Keep the existing index redirect outside private presentation admission. */
export function WorkspacePresentationBoundary({
  route,
  children,
}: {
  route: ReactNode;
  children: ReactNode;
}) {
  const pathname = usePathname();
  return (
    <TenantPresentationProvider
      routeControl={pathname === "/workspace" ? route : null}
    >
      {children}
    </TenantPresentationProvider>
  );
}
