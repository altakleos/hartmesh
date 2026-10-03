import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { act, cleanup, render, screen } from "@testing-library/react";

const router = rs.hoisted(() => ({ push: rs.fn() }));
const sidebar = rs.hoisted(() => ({ setOpen: rs.fn() }));
rs.mock("next/navigation", () => ({
  useRouter: () => router,
  usePathname: () => "/workspace/chats/old-thread",
}));
rs.mock("@/components/ui/sidebar", () => ({ useSidebar: () => sidebar }));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));
rs.mock("@/env", () => ({ env: { NEXT_PUBLIC_STATIC_WEBSITE_ONLY: "false" } }));

import {
  ArtifactsProvider,
  useArtifacts,
} from "@/components/workspace/artifacts/context";
import { AuthProvider, useAuth, type User } from "@/core/auth/AuthProvider";

const alice: User = {
  id: "alice",
  email: "alice@example.com",
  system_role: "user",
  needs_setup: false,
};
const bob: User = { ...alice, id: "bob", email: "bob@example.com" };
let auth: ReturnType<typeof useAuth>;
let artifacts: ReturnType<typeof useArtifacts>;

function Display() {
  auth = useAuth();
  artifacts = useArtifacts();
  return <div data-testid="artifacts">{artifacts.artifacts.join(",")}</div>;
}

function App({ user }: { user: User }) {
  return (
    <AuthProvider initialUser={user}>
      <ArtifactsProvider>
        <Display />
      </ArtifactsProvider>
    </AuthProvider>
  );
}

afterEach(() => {
  cleanup();
  window.sessionStorage.clear();
});

describe("artifact bootstrap persistence", () => {
  it("restores only the current account's filenames at the same conversation URL", () => {
    const mounted = render(<App user={alice} />);
    act(() =>
      artifacts.setArtifacts(["/mnt/user-data/outputs/alice-private.pdf"]),
    );
    mounted.unmount();

    render(<App user={bob} />);
    expect(screen.getByTestId("artifacts").textContent).toBe("");
    act(() => artifacts.setArtifacts(["/mnt/user-data/outputs/bob.pdf"]));
    act(() => auth.applyUser(alice));
    expect(screen.getByTestId("artifacts").textContent).toBe(
      "/mnt/user-data/outputs/alice-private.pdf",
    );
  });

  it("ignores old cache entries whose owner was never recorded", () => {
    window.sessionStorage.setItem(
      `deerflow:artifacts:v1:${encodeURIComponent("/workspace/chats/old-thread")}`,
      JSON.stringify({
        artifacts: ["/mnt/user-data/outputs/unknown-owner.pdf"],
        selectedArtifact: null,
        open: true,
      }),
    );
    render(<App user={bob} />);
    expect(screen.getByTestId("artifacts").textContent).toBe("");
  });
});
