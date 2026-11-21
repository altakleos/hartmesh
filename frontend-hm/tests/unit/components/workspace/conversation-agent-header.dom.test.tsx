import { afterEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

rs.mock("@/core/agent-instances/api", { spy: true });
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: "reader" } }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  usePersonNames: (ids: string[]) =>
    new Map(ids.map((id) => [id, id === "owner" ? "Alex" : "Pat"])),
}));

import { ConversationAgentHeader } from "@/components/workspace/chats/conversation-agent-header";
import { request, type AgentInstanceDetail } from "@/core/agent-instances/api";
import { I18nProvider } from "@/core/i18n/context";
const binding = {
  id: "a".repeat(32),
  name: "Researcher",
  custody: "personal" as const,
  status: "active" as const,
  requester_id: "reader",
};
function show(
  detail: Partial<AgentInstanceDetail> | null,
  kind: "personal" | "company" = "personal",
  showConnections = false,
) {
  const client = new QueryClient();
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <ConversationAgentHeader
          binding={{ ...binding, custody: kind }}
          detail={
            detail
              ? ({
                  ...binding,
                  owner_id: "reader",
                  supervisor: { kind: "human", subject_id: "supervisor" },
                  ...detail,
                } as AgentInstanceDetail)
              : null
          }
          title={<span>My conversation</span>}
          alsoWorking
          showConnections={showConnections}
        />
      </I18nProvider>
    </QueryClientProvider>,
  );
}
afterEach(cleanup);
test("owner sees an AI-qualified avatar, linked name, conversation title and personal identity", () => {
  show({});
  expect(screen.getByRole("img", { name: "AI assistant" })).toBeDefined();
  expect(
    screen.getByRole("link", { name: "Researcher" }).getAttribute("href"),
  ).toBe(`/workspace/instances?instance=${binding.id}`);
  expect(screen.getByText("My conversation")).toBeDefined();
  expect(screen.getByText("Your AI assistant")).toBeDefined();
  expect(
    screen.getByText("Also working in another conversation"),
  ).toBeDefined();
});
test("a colleague sees the human owner's resolved name", () => {
  show({ owner_id: "owner" });
  expect(screen.getByText("Alex's AI assistant")).toBeDefined();
});
test("an AI employee names its human supervisor", () => {
  show({}, "company");
  expect(screen.getByText("AI employee · supervised by Pat")).toBeDefined();
});
test("failed or loading detail falls back only to the kind carried by the thread row", () => {
  show(null, "company");
  expect(screen.getByText("AI employee")).toBeDefined();
  expect(screen.queryByText(/supervised by/)).toBeNull();
});

test("a chat-only member sees usable names and sharing details from the current view", async () => {
  rs.mocked(request)
    .mockReset()
    .mockResolvedValue({
      connections: [{ name: "Weekly notes", host: "notes.example.test" }],
    });
  show({ permissions: 1, generation: 1 }, "company", true);
  const control = await screen.findByRole("button", {
    name: /Researcher can use: Weekly notes/,
  });
  fireEvent.click(control);
  expect(screen.getByRole("dialog").textContent).toContain(
    "anything in Researcher",
  );
  expect(screen.queryByText("notes.example.test")).toBeNull();
  expect(screen.queryByRole("switch")).toBeNull();
});
test("an unavailable feature or dormant agent does not read connection details", () => {
  rs.mocked(request).mockReset();
  const off = show({ permissions: 1, generation: 1 }, "company", false);
  off.unmount();
  show({ permissions: 1, generation: 1, status: "archived" }, "company", true);
  expect(rs.mocked(request)).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: /can use:/ })).toBeNull();
});
