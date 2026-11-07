"use client";

import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { AuthProvider, useAuth, type User } from "@/core/auth/AuthProvider";

function scope(user: User | null): string {
  return JSON.stringify(
    user ? [user.id, user.system_role, user.permissions?.slice().sort()] : null,
  );
}

type ServerEvidence = { user: User | undefined; retired: boolean };

function VerifiedWorkspaceSession({
  verifiedUser,
  evidence,
  observe,
  children,
}: {
  verifiedUser: User | undefined;
  evidence: ServerEvidence;
  observe: (user: User | undefined, conflict: boolean) => void;
  children: ReactNode;
}) {
  const { user } = useAuth();
  const changed =
    verifiedUser !== evidence.user &&
    verifiedUser !== undefined &&
    scope(verifiedUser) !== scope(user);
  useLayoutEffect(() => {
    observe(verifiedUser, changed);
  }, [verifiedUser, changed, observe]);
  // Unchanged initial props cannot contradict a newer verified client refresh.
  // New conflicting SSR evidence hides private state before its retirement is
  // committed. Uncertain or older results cannot reopen a retired subtree.
  return changed || evidence.retired ? (
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
  // This evidence must survive AuthProvider's account-keyed child remounts,
  // including a client scope change batched with a new server result.
  const [evidence, setEvidence] = useState<ServerEvidence>({
    user: verifiedUser,
    retired: false,
  });
  const observe = useCallback((user: User | undefined, conflict: boolean) => {
    setEvidence((current) =>
      current.user === user && (!conflict || current.retired)
        ? current
        : { user, retired: current.retired || conflict },
    );
  }, []);
  const reloadStarted = useRef(false);
  useEffect(() => {
    if (evidence.retired && !reloadStarted.current) {
      reloadStarted.current = true;
      window.location.reload();
    }
  }, [evidence.retired]);
  return (
    <AuthProvider initialUser={verifiedUser ?? null}>
      <VerifiedWorkspaceSession
        verifiedUser={verifiedUser}
        evidence={evidence}
        observe={observe}
      >
        {children}
      </VerifiedWorkspaceSession>
    </AuthProvider>
  );
}
