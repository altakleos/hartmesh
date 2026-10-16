import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({
  agents: {} as AgentInstancesFeature & { isLoading: boolean },
  spaces: false,
  path: "/workspace/chats",
  counts: { pending: 3, routing: 2 },
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => state.agents,
  useSpacesPageVisible: () => state.spaces,
}));
rs.mock("@/core/agents", () => ({
  useAgentsApiEnabled: () => ({ enabled: true }),
}));
rs.mock("@/core/attention/hooks", () => ({
  useAttention: () => ({ data: { counts: state.counts } }),
}));
rs.mock("next/navigation", () => ({ usePathname: () => state.path }));
rs.mock("@/hooks/use-mobile", () => ({ useIsMobile: () => false }));

import { SidebarProvider } from "@/components/ui/sidebar";
import { WorkspaceNavChatList } from "@/components/workspace/workspace-nav-chat-list";
import type { AgentInstancesFeature } from "@/core/features/api";
import { I18nProvider } from "@/core/i18n/context";

function show() {
  return render(
    <I18nProvider initialLocale="en-US">
      <SidebarProvider>
        <WorkspaceNavChatList />
      </SidebarProvider>
    </I18nProvider>,
  );
}
beforeEach(() => {
  state.agents = {
    known: true,
    enabled: true,
    execution: "ready",
    reason: null,
    assistants: { perUser: 1, allowed: 10, inUse: 2, mine: 1 },
    employees: { allowed: 3, inUse: 1 },
    isLoading: false,
  };
  state.spaces = false;
  state.path = "/workspace/chats";
});
afterEach(cleanup);

test("puts Attention, Chats, My assistants, AI employees, My files, Agents and Scheduled tasks in order", () => {
  show();
  expect(
    screen.getAllByRole("link").map((link) => link.getAttribute("href")),
  ).toEqual([
    "/workspace/attention",
    "/workspace/chats",
    "/workspace/assistants",
    "/workspace/employees",
    "/workspace/files",
    "/workspace/agents",
    "/workspace/scheduled-tasks",
  ]);
});
test("keeps both agent lists and Attention readable during a deployment hold", () => {
  state.agents.execution = "unavailable";
  state.agents.reason = "operator-hold";
  show();
  expect(
    screen.getByRole("link", { name: /My assistants/ }).getAttribute("href"),
  ).toBe("/workspace/assistants");
  expect(
    screen.getByRole("link", { name: /AI employees/ }).getAttribute("href"),
  ).toBe("/workspace/employees");
  expect(
    screen.getAllByText(
      "AI assistants and AI employees are not available right now.",
    ).length,
  ).toBeGreaterThanOrEqual(2);
  expect(screen.getByRole("link", { name: /Attention/ })).toBeDefined();
});
test.each(["loading", "unknown", "disabled"])(
  "keeps entries inert and hides Attention when %s",
  (mode) => {
    state.agents.isLoading = mode === "loading";
    state.agents.known = mode !== "unknown";
    if (mode === "disabled") state.agents.enabled = false;
    show();
    expect(screen.queryByRole("link", { name: /My assistants/ })).toBeNull();
    expect(screen.queryByRole("link", { name: /AI employees/ })).toBeNull();
    expect(screen.queryByRole("link", { name: /Attention/ })).toBeNull();
    expect(
      screen
        .getByRole("button", { name: /My assistants/ })
        .getAttribute("aria-disabled"),
    ).toBe("true");
  },
);
test("disables only the kind whose allowance is zero and disables both for agents-none", () => {
  state.agents.assistants.perUser = 0;
  const view = show();
  expect(
    screen
      .getByRole("button", { name: "My assistants" })
      .getAttribute("aria-disabled"),
  ).toBe("true");
  expect(screen.getByRole("link", { name: "AI employees" })).toBeDefined();
  view.unmount();
  state.agents.execution = "unavailable";
  state.agents.reason = "agents-none";
  show();
  expect(screen.queryByRole("link", { name: "AI employees" })).toBeNull();
  expect(screen.getByText("AI employees are not offered here.")).toBeDefined();
});
test("gates Spaces separately and always renders the custom Agents entry", () => {
  state.spaces = true;
  const view = show();
  expect(screen.getByRole("link", { name: "Spaces" })).toBeDefined();
  expect(screen.getByRole("link", { name: "Agents" })).toBeDefined();
  view.unmount();
  state.spaces = false;
  state.agents.known = false;
  show();
  expect(screen.queryByRole("link", { name: "Spaces" })).toBeNull();
  expect(screen.queryByRole("link", { name: "Agents" })).toBeNull();
  expect(
    screen
      .getByRole("button", { name: "Agents" })
      .getAttribute("aria-disabled"),
  ).toBe("true");
});
test("shows the combined attention count with both counts in its accessible explanation", () => {
  show();
  const count = screen.getByLabelText(
    "3 waiting for your reply, 2 waiting for a recipient",
  );
  expect(count.textContent).toBe("5");
  expect(count.getAttribute("title")).toBe(
    "3 waiting for your reply, 2 waiting for a recipient",
  );
});
