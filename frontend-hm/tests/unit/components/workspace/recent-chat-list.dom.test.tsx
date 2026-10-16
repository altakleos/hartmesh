import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({ threads: [] as AgentThread[] }));
rs.mock("@/core/threads/hooks", () => ({
  useInfiniteThreads: () => ({
    data: { pages: [state.threads] },
    hasNextPage: false,
  }),
  usePinThread: () => ({ mutate: rs.fn() }),
  useRenameThread: () => ({ mutate: rs.fn() }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", email: "reader@example.com", system_role: "user" },
  }),
}));
rs.mock("@/core/people/api", () => ({ getPeople: rs.fn() }));
rs.mock("next/navigation", () => ({
  useParams: () => ({}),
  usePathname: () => "/workspace/chats",
}));
rs.mock("@/hooks/use-mobile", () => ({ useIsMobile: () => false }));
rs.mock("@/env", () => ({ env: { NEXT_PUBLIC_STATIC_WEBSITE_ONLY: "false" } }));
rs.mock("@/components/workspace/conversation-extension-actions", () => ({
  ConversationExtensionActions: () => null,
}));
rs.mock("@/components/workspace/thread-delete-dialog", () => ({
  useThreadDeleteDialog: () => rs.fn(),
}));

import { SidebarProvider } from "@/components/ui/sidebar";
import { RecentChatList } from "@/components/workspace/recent-chat-list";
import { I18nProvider } from "@/core/i18n/context";
import { getPeople } from "@/core/people/api";
import type { AgentThread } from "@/core/threads/types";

const clients: QueryClient[] = [];
function thread(id: string, title: string): AgentThread {
  return {
    thread_id: id,
    created_at: "2026-10-16T00:00:00Z",
    updated_at: "2026-10-16T00:00:00Z",
    status: "idle",
    metadata: {},
    values: { title, messages: [] },
    interrupts: {},
  };
}
function show() {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <SidebarProvider>
          <RecentChatList />
        </SidebarProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  rs.mocked(getPeople)
    .mockReset()
    .mockImplementation(async (ids) => ({
      users: ids.map((id) => ({
        id,
        email: `${id}@example.com`,
        name: null,
        active: true,
      })),
    }));
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});

test("keeps ordinary chat titles and makes no directory request for them", () => {
  state.threads = [thread("ordinary", "A regular chat")];
  show();
  expect(
    screen.getByRole("link", { name: "A regular chat" }).getAttribute("href"),
  ).toBe("/workspace/chats/ordinary");
  expect(screen.queryByRole("img", { name: "AI assistant" })).toBeNull();
  expect(getPeople).not.toHaveBeenCalled();
});
test("shows the bound agent and its qualified badge before the title", () => {
  state.threads = [
    {
      ...thread("bound", "Quarterly review"),
      agent_instance: {
        id: "a".repeat(32),
        name: "Researcher",
        status: "active",
        custody: "company",
        requester_id: "reader",
      },
    },
  ];
  show();
  const link = screen.getByRole("link", {
    name: /Researcher.*Quarterly review/,
  });
  expect(link.getAttribute("href")).toBe("/workspace/chats/bound");
  expect(screen.getByRole("img", { name: "AI employee" })).toBeDefined();
  expect(getPeople).not.toHaveBeenCalled();
});
test("resolves colleagues across visible rows in one batch without granting agent navigation", async () => {
  state.threads = ["lee", "sam"].map((id) => ({
    ...thread(id, `${id} conversation`),
    agent_instance: {
      id: "a".repeat(32),
      name: "Researcher",
      status: "active",
      custody: "personal",
      requester_id: id,
    },
  }));
  show();
  await waitFor(() =>
    expect(screen.getByText("with lee@example.com")).toBeDefined(),
  );
  expect(screen.getByText("with sam@example.com")).toBeDefined();
  expect(getPeople).toHaveBeenCalledTimes(1);
  expect(rs.mocked(getPeople).mock.calls[0]![0].sort()).toEqual(["lee", "sam"]);
  expect(screen.queryByRole("link", { name: "Researcher" })).toBeNull();
});
