"use client";

import { useEffect, useState, type ReactNode } from "react";

import { AuthProvider, useAuth, type User } from "@/core/auth/AuthProvider";

function scope(user: User | null): string {
  return JSON.stringify(
    user ? [user.id, user.system_role, user.permissions?.slice().sort()] : null,
  );
}

function VerifiedWorkspaceSession({
  verifiedUser,
  children,
}: {
  verifiedUser: User | undefined;
  children: ReactNode;
}) {
  const { user } = useAuth();
  const [retired, setRetired] = useState(false);
  const changed =
    verifiedUser !== undefined && scope(verifiedUser) !== scope(user);
  useEffect(() => {
    if (changed && !retired) {
      setRetired(true);
      window.location.reload();
    }
  }, [changed, retired]);
  // An uncertain SSR result retains the admitted account. A confirmed different
  // scope clears private state and gets fresh document/session admission; late
  // server props never become a client account. Logout retires this subtree.
  return changed || retired ? (
    <p role="status" className="bg-white p-4 text-black">
      Refreshing session…
    </p>
  ) : (
    children
  );
}

/** Stable across healthy/unavailable SSR refreshes, with fresh-account fencing. */
export function WorkspaceAuthProvider({
  verifiedUser,
  children,
}: {
  verifiedUser?: User;
  children: ReactNode;
}) {
  return (
    <AuthProvider initialUser={verifiedUser ?? null}>
      <VerifiedWorkspaceSession verifiedUser={verifiedUser}>
        {children}
      </VerifiedWorkspaceSession>
    </AuthProvider>
  );
}
