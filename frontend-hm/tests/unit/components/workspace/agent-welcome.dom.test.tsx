import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  query: "welcome=1",
  input: "",
  setInput: rs.fn(),
}));
rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(state.query),
}));
rs.mock("@/components/ai-elements/prompt-input", () => ({
  usePromptInputController: () => ({
    textInput: { value: state.input, setInput: state.setInput },
  }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", system_role: "user", permissions: ["agents:read"] },
  }),
}));
rs.mock("@/core/agent-instances/api", { spy: true });

import { AgentWelcome } from "@/components/workspace/chats/agent-welcome";
import * as api from "@/core/agent-instances/api";
import { I18nProvider } from "@/core/i18n/context";
const binding = {
  id: "a".repeat(32),
  name: "Researcher",
  status: "active" as const,
  custody: "personal" as const,
  requester_id: "reader",
};
const detail: api.AgentInstanceDetail = {
  id: binding.id,
  name: "Researcher",
  principal: { kind: "nonhuman", subject_id: "ai" },
  custody: "personal",
  owner_id: "reader",
  creator_id: "reader",
  supervisor: { kind: "human", subject_id: "reader" },
  definition_revision: "revision",
  generation: 1,
  home_id: "b".repeat(32),
  status: "active",
  permissions: 7,
  activity: {
    running_runs: 0,
    running: [],
    last_active_at: null,
    open_conversations: 1,
  },
  home: { state: "available", workspace: "absent" },
  execution: { execution: "ready", reason: null },
};
const clients: QueryClient[] = [];
function show(permissions = 7) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <AgentWelcome
          binding={binding}
          detail={{ ...detail, permissions }}
          threadId="thread"
        />
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  state.query = "welcome=1";
  state.input = "";
  state.setInput.mockReset().mockImplementation((value: string) => {
    state.input = value;
  });
  history.replaceState(null, "", "/workspace/chats/thread?welcome=1");
  rs.mocked(api.getInstanceDefinition)
    .mockReset()
    .mockResolvedValue({
      revision: "revision",
      config: { name: "researcher", description: "Research carefully" },
      soul: "Brief",
    });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("welcome prefills one unsent first message and removes its one-time URL parameter", async () => {
  show();
  await screen.findByText("Researcher is ready");
  expect(state.setInput).toHaveBeenCalledWith(
    "Hello Researcher. Please give a short introduction: the role, the kind of work to ask for, and how to hand over files.",
  );
  expect(location.search).toBe("");
  expect(
    screen.getByText(
      "A first message is ready in the message box. Nothing is sent until you press Send.",
    ),
  ).toBeDefined();
  await screen.findByText("Research carefully");
  expect(
    screen
      .getByRole("link", { name: "Add colleagues on the Access tab." })
      .getAttribute("href"),
  ).toContain("tab=access");
});
test("an existing draft is preserved and welcome still consumes the parameter", async () => {
  state.input = "My own draft";
  show();
  await screen.findByText("Researcher is ready");
  expect(state.setInput).not.toHaveBeenCalled();
  expect(location.search).toBe("");
});
test("without Inspect welcome does not read or show the adopted description", async () => {
  show(1);
  await waitFor(() =>
    expect(screen.getByText("Researcher is ready")).toBeDefined(),
  );
  expect(api.getInstanceDefinition).not.toHaveBeenCalled();
  expect(screen.queryByText("Research carefully")).toBeNull();
  expect(
    screen.queryByRole("link", { name: "Add colleagues on the Access tab." }),
  ).toBeNull();
  expect(screen.getByText("Researcher does not see My files.")).toBeDefined();
});
