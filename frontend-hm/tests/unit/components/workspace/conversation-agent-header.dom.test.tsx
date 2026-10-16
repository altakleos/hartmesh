import { afterEach, expect, test, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: "reader" } }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  usePersonNames: (ids: string[]) =>
    new Map(ids.map((id) => [id, id === "owner" ? "Alex" : "Pat"])),
}));

import { ConversationAgentHeader } from "@/components/workspace/chats/conversation-agent-header";
import type { AgentInstanceDetail } from "@/core/agent-instances/api";
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
) {
  return render(
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
      />
    </I18nProvider>,
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
