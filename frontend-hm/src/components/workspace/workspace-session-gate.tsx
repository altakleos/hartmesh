"use client";

import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import { useAuth } from "@/core/auth/AuthProvider";
import { isStaticWebsiteOnly } from "@/core/static-mode";

/** Allow the index redirect and auth recovery before mounting private workspace UI. */
export function WorkspaceSessionGate({
  route,
  children,
}: {
  route: ReactNode;
  children: ReactNode;
}) {
  const { user } = useAuth();
  const pathname = usePathname();
  if (user?.id || isStaticWebsiteOnly()) return children;
  if (pathname === "/workspace") return route;
  return (
    <main className="flex min-h-screen flex-col items-center justify-center gap-4 bg-white p-4 text-black">
      <h1 role="status">Loading workspace</h1>
    </main>
  );
}
