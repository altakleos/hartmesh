"use client";

import { useRouter, usePathname } from "next/navigation";
import React, {
  createContext,
  Fragment,
  useContext,
  useState,
  useCallback,
  useEffect,
  type ReactNode,
} from "react";

import { isStaticWebsiteOnly } from "../static-mode";

import { type User, buildLoginUrl, userSchema } from "./types";

const REFRESH_RETRY_DELAYS_MS = [2_000, 5_000, 15_000] as const;
const REFRESH_TIMEOUT_MS = 10_000;

// Re-export for consumers
export type { User };

/**
 * Authentication context provided to consuming components
 */
interface AuthContextType {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  logout: () => Promise<void>;
  refreshUser: () => Promise<void>;
  applyUser: (user: User | null) => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
  initialUser: User | null;
}

/**
 * AuthProvider - Unified authentication context for the application
 *
 * Per RFC-001:
 * - Only holds display information (user), never JWT or tokens
 * - initialUser comes from server-side guard, avoiding client flicker
 * - Provides logout and refresh capabilities
 */
export function AuthProvider({ children, initialUser }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(initialUser);
  const [isLoading, setIsLoading] = useState(false);
  const [isLoggingOut, setIsLoggingOut] = useState(false);
  const refreshGeneration = React.useRef(0);
  const retryTimer = React.useRef<ReturnType<typeof setTimeout> | null>(null);
  const activeRefresh = React.useRef<{
    controller: AbortController;
    timeout: ReturnType<typeof setTimeout>;
  } | null>(null);
  const router = useRouter();
  const pathname = usePathname();
  const currentPathname = React.useRef(pathname);
  const staticMode = isStaticWebsiteOnly();

  const isAuthenticated = user !== null;

  const cancelRefresh = useCallback(() => {
    if (retryTimer.current !== null) {
      clearTimeout(retryTimer.current);
      retryTimer.current = null;
    }
    if (activeRefresh.current !== null) {
      clearTimeout(activeRefresh.current.timeout);
      activeRefresh.current.controller.abort();
      activeRefresh.current = null;
    }
  }, []);

  useEffect(() => {
    currentPathname.current = pathname;
  }, [pathname]);

  useEffect(
    () => () => {
      refreshGeneration.current += 1;
      cancelRefresh();
    },
    [cancelRefresh],
  );

  /**
   * Apply a user value supplied by a caller (e.g. banner probe) that has
   * already fetched it. Invalidate earlier refreshes before publishing the
   * new identity so a late response cannot restore another account.
   */
  const applyUser = useCallback(
    (next: User | null) => {
      refreshGeneration.current += 1;
      cancelRefresh();
      setUser(next);
      setIsLoading(false);
    },
    [cancelRefresh],
  );

  /**
   * Fetch current user from FastAPI
   * Used when initialUser might be stale (e.g., after tab was inactive)
   */
  const requestRefresh = useCallback(
    async function refresh(attempt = 0) {
      if (staticMode) return;
      const generation = ++refreshGeneration.current;
      cancelRefresh();
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), REFRESH_TIMEOUT_MS);
      activeRefresh.current = { controller, timeout };

      try {
        setIsLoading(true);
        const res = await fetch("/api/v1/auth/me", {
          credentials: "include",
          signal: controller.signal,
        });

        if (res.ok) {
          const data = userSchema.parse(await res.json());
          if (generation !== refreshGeneration.current) return;
          applyUser(data);
        } else if (res.status === 401) {
          if (generation !== refreshGeneration.current) return;
          // Session expired or invalid
          applyUser(null);
          // Redirect to login if on a protected route
          if (currentPathname.current?.startsWith("/workspace")) {
            router.push(buildLoginUrl(currentPathname.current));
          }
        } else {
          throw new Error("Session check unavailable");
        }
      } catch {
        if (generation !== refreshGeneration.current) return;
        // An uncertain check cannot revoke the last verified identity. Keep
        // its query client and drafts while the server remains authoritative.
        // Do not log a malformed body or provider-supplied error details.
        console.warn("Session check temporarily unavailable");
        if (user !== null && attempt < REFRESH_RETRY_DELAYS_MS.length) {
          retryTimer.current = setTimeout(() => {
            retryTimer.current = null;
            if (generation === refreshGeneration.current)
              void refresh(attempt + 1);
          }, REFRESH_RETRY_DELAYS_MS[attempt]);
        }
      } finally {
        clearTimeout(timeout);
        if (activeRefresh.current?.controller === controller)
          activeRefresh.current = null;
        if (generation === refreshGeneration.current) setIsLoading(false);
      }
    },
    [staticMode, router, applyUser, cancelRefresh, user],
  );

  const refreshUser = useCallback(() => requestRefresh(), [requestRefresh]);

  /**
   * Logout - call FastAPI logout endpoint and clear local state
   * Per RFC-001: Immediately clear local state, don't wait for server confirmation
   *
   * When the gateway is unreachable the fetch silently fails — the SPA
   * router.push("/") would leave the user on "/" still holding stale
   * React state and any in-flight SSE / fetch / query subscriptions.
   * We therefore fall back to a hard navigation (window.location.href),
   * which discards all client state the same way the legacy form-POST
   * logout used to.
   */
  const logout = useCallback(async () => {
    // Static demos have no session to clear and their root returns to this
    // same workspace layout. Keep it mounted and usable after navigation.
    if (staticMode) {
      router.push("/");
      return;
    }

    // Retire private subscriptions before the request clears the cookie.
    // Rendering a new anonymous workspace while it is pending could fetch
    // this account's data again with the still-valid cookie.
    setIsLoggingOut(true);
    applyUser(null);

    let logoutFailed = false;
    try {
      const res = await fetch("/api/v1/auth/logout", {
        method: "POST",
        credentials: "include",
      });
      if (!res.ok) logoutFailed = true;
    } catch (err) {
      console.error("Logout request failed:", err);
      logoutFailed = true;
    }

    if (logoutFailed && typeof window !== "undefined") {
      // Hard navigation ensures every in-flight subscription is torn down,
      // matching the legacy form-POST logout behaviour during a gateway outage.
      window.location.href = "/";
      return;
    }

    // Leave the workspace layout: its cached root redirect could otherwise
    // return to this deliberately unmounted subtree instead of the login page.
    router.push("/login");
  }, [staticMode, router, applyUser]);

  /**
   * Handle visibility change - refresh user when tab becomes visible again.
   * Throttled to at most once per 60 s to avoid spamming the backend on rapid tab switches.
   */
  const lastCheckRef = React.useRef(0);

  useEffect(() => {
    if (staticMode) return;

    const handleVisibilityChange = () => {
      if (document.visibilityState !== "visible" || user === null) return;
      const now = Date.now();
      if (now - lastCheckRef.current < 60_000) return;
      lastCheckRef.current = now;
      void refreshUser();
    };

    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [staticMode, user, refreshUser]);

  const value: AuthContextType = {
    user,
    isAuthenticated,
    isLoading,
    logout,
    refreshUser,
    applyUser,
  };

  // A separate subtree owns all queries, mutations and local display state.
  // Late callbacks retain only the retired client, never the next account's.
  const identity = JSON.stringify(
    user ? [user.id, user.system_role, user.permissions?.slice().sort()] : null,
  );
  return (
    <AuthContext.Provider value={value}>
      {!isLoggingOut && <Fragment key={identity}>{children}</Fragment>}
    </AuthContext.Provider>
  );
}

/**
 * Hook to access authentication context
 * Throws if used outside AuthProvider - this is intentional for proper usage
 */
export function useAuth(): AuthContextType {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}

/**
 * Hook to require authentication - redirects to login if not authenticated
 * Useful for client-side checks in addition to server-side guards
 */
export function useRequireAuth(): AuthContextType {
  const auth = useAuth();
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    if (isStaticWebsiteOnly()) return;

    // Only redirect if we're sure user is not authenticated (not just loading)
    if (!auth.isLoading && !auth.isAuthenticated) {
      router.push(buildLoginUrl(pathname || "/workspace"));
    }
  }, [auth.isAuthenticated, auth.isLoading, router, pathname]);

  return auth;
}
