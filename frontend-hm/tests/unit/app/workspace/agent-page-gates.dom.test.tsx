import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({
  refetch: rs.fn(),
  permissions: ["agents:read"],
  agents: { known: true, enabled: true, isLoading: false, execution: "ready" },
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => ({
    ...state.agents,
    reason: "operator-hold",
    refetch: state.refetch,
  }),
  useDocumentTitle: () => undefined,
  useBranding: () => ({ providerName: "Provider" }),
  useStorage: () => ({ reason: null }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", system_role: "user", permissions: state.permissions },
  }),
}));
rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(),
}));
rs.mock("@/components/workspace/attention/inbox", () => ({
  AttentionInbox: () => <p>Readable requests</p>,
}));
rs.mock("@/components/workspace/workspace-container", () => ({
  WorkspaceContainer: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceBody: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceHeader: () => null,
}));
import AgentsLayout from "@/app/workspace/agents/layout";
import AttentionPage from "@/app/workspace/attention/page";
import { I18nProvider } from "@/core/i18n/context";

beforeEach(() => {
  state.refetch.mockReset();
  state.permissions = ["agents:read"];
  state.agents = {
    known: true,
    enabled: true,
    isLoading: false,
    execution: "ready",
  };
});
afterEach(cleanup);
function show() {
  render(
    <I18nProvider initialLocale="en-US">
      <AgentsLayout>
        <p>Custom agents</p>
      </AgentsLayout>
      <AttentionPage />
    </I18nProvider>,
  );
}

test("keeps the custom agent pages and Attention available while execution is held", () => {
  state.agents.execution = "unavailable";
  show();
  expect(screen.getByText("Custom agents")).toBeDefined();
  expect(screen.getByText("Readable requests")).toBeDefined();
});
test.each(["unknown", "disabled", "loading"])(
  "closes both agent surfaces while %s",
  (kind) => {
    if (kind === "unknown") state.agents.known = false;
    if (kind === "disabled") state.agents.enabled = false;
    if (kind === "loading") state.agents.isLoading = true;
    show();
    expect(screen.queryByText("Custom agents")).toBeNull();
    expect(screen.queryByText("Readable requests")).toBeNull();
  },
);
test("Attention shows loading without an unavailable sentence and offers Refresh after failed discovery", () => {
  state.agents.isLoading = true;
  const view = render(
    <I18nProvider initialLocale="en-US">
      <AttentionPage />
    </I18nProvider>,
  );
  expect(screen.getByRole("status").textContent).toBe("Loading…");
  expect(
    screen.queryByText(
      "AI assistants and AI employees are not available right now.",
    ),
  ).toBeNull();
  state.agents.isLoading = false;
  state.agents.known = false;
  view.rerender(
    <I18nProvider initialLocale="en-US">
      <AttentionPage />
    </I18nProvider>,
  );
  fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  expect(state.refetch).toHaveBeenCalledTimes(1);
});
test("Attention distinguishes a missing read ceiling from deployment availability", () => {
  state.permissions = [];
  show();
  expect(
    screen.getByText("You do not have permission to see requests."),
  ).toBeTruthy();
  expect(screen.queryByText("Readable requests")).toBeNull();
});
