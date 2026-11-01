import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { useEffect } from "react";

const router = rs.hoisted(() => ({ push: rs.fn() }));
rs.mock("next/navigation", () => ({
  useRouter: () => router,
  usePathname: () => "/workspace",
}));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));

import {
  AuthProvider,
  useAuth,
  useRequireAuth,
  type User,
} from "@/core/auth/AuthProvider";

const alice: User = {
  id: "alice",
  email: "alice@example.com",
  system_role: "user",
  needs_setup: false,
};
const QUERY = "?settings=channels&telegram=chat";
const signIn = `/login?next=${encodeURIComponent(`/workspace${QUERY}`)}`;

let refresh: () => Promise<void>;

function Probe() {
  const auth = useAuth();
  useEffect(() => {
    refresh = auth.refreshUser;
  });
  return null;
}

function Guarded() {
  useRequireAuth();
  return null;
}

beforeEach(() => {
  router.push.mockReset();
  window.history.replaceState({}, "", `/workspace${QUERY}`);
});

afterEach(() => {
  cleanup();
  rs.unstubAllGlobals();
  window.history.replaceState({}, "", "/");
});

describe("the exits of AuthProvider", () => {
  it("keeps the query when a session check answers 401", async () => {
    rs.stubGlobal(
      "fetch",
      rs.fn(async () => new Response("{}", { status: 401 })),
    );
    render(
      <AuthProvider initialUser={alice}>
        <Probe />
      </AuthProvider>,
    );
    await act(() => refresh());
    expect(router.push).toHaveBeenCalledWith(signIn);
  });

  it("keeps the query when the person is not signed in", async () => {
    render(
      <AuthProvider initialUser={null}>
        <Guarded />
      </AuthProvider>,
    );
    await waitFor(() => expect(router.push).toHaveBeenCalledWith(signIn));
  });
});
