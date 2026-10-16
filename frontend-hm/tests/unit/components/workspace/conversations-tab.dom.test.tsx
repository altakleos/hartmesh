import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", system_role: "user", permissions: ["agents:read"] },
  }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  usePersonName: () => "Colleague",
}));
rs.mock("@/core/agent-instances/api", { spy: true });
import { ConversationsTab } from "@/components/workspace/instances/conversations-tab";
import * as api from "@/core/agent-instances/api";
import { I18nProvider } from "@/core/i18n/context";
const clients: QueryClient[] = [];
function show(permissions = 3) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <ConversationsTab
          instance={
            {
              id: "a".repeat(32),
              name: "Researcher",
              permissions,
              generation: 1,
            } as api.AgentInstance
          }
        />
      </I18nProvider>
    </QueryClientProvider>,
  );
}
const row = {
  thread_id: "thread",
  requester_id: "reader",
  title: null,
  updated_at: "2026-10-15T12:00:00Z",
  running: false,
};
beforeEach(() => {
  rs.mocked(api.listInstanceConversations)
    .mockReset()
    .mockResolvedValue({ conversations: [row] });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("shows the caller's conversation with an untitled fallback", async () => {
  show(1);
  await screen.findByRole("link", { name: "Untitled conversation" });
  expect(screen.getByText("Your conversation")).toBeDefined();
  expect(
    screen.getByText(
      "You see your own conversations. People who can see all work see every conversation.",
    ),
  ).toBeDefined();
});
test("resolves a colleague by name and shows running work", async () => {
  rs.mocked(api.listInstanceConversations).mockResolvedValue({
    conversations: [
      { ...row, title: "Research", requester_id: "colleague", running: true },
    ],
  });
  show();
  await screen.findByRole("link", { name: "Research" });
  expect(screen.getByText("with Colleague")).toBeDefined();
  expect(screen.getByText("Working")).toBeDefined();
});
test("failed requests show localized copy and refresh on request", async () => {
  rs.mocked(api.listInstanceConversations).mockRejectedValueOnce(
    new Error("private detail"),
  );
  show();
  await screen.findByText(
    "Conversations could not be loaded. Refresh to try again.",
  );
  expect(screen.queryByText("private detail")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  await screen.findByRole("link", { name: "Untitled conversation" });
});
test("pages exactly fifty conversations without replacing the first page", async () => {
  rs.mocked(api.listInstanceConversations)
    .mockResolvedValueOnce({
      conversations: Array.from({ length: 50 }, (_, i) => ({
        ...row,
        thread_id: `first-${i}`,
        title: `First ${i}`,
      })),
    })
    .mockResolvedValueOnce({
      conversations: [{ ...row, thread_id: "last", title: "Last" }],
    });
  show();
  fireEvent.click(await screen.findByRole("button", { name: "Load more" }));
  await screen.findByRole("link", { name: "Last" });
  expect(screen.getByRole("link", { name: "First 0" })).toBeDefined();
  expect(rs.mocked(api.listInstanceConversations).mock.calls[1]![1]).toBe(50);
  await waitFor(() =>
    expect(screen.queryByRole("button", { name: "Load more" })).toBeNull(),
  );
});
