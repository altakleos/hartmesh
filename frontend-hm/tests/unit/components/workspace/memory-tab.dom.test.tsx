import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", permissions: ["memory:read", "memory:write"] },
  }),
}));
rs.mock("@/core/agent-instances/api", { spy: true });
import { MemoryTab } from "@/components/workspace/instances/memory-tab";
import * as api from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
import type { UserMemory } from "@/core/memory/types";
const part = { summary: "Remember this", updatedAt: "2026-10-01" };
const memory: UserMemory = {
  version: "1",
  lastUpdated: "2026-10-01",
  user: {
    workContext: part,
    personalContext: { ...part, summary: "" },
    topOfMind: { ...part, summary: "" },
  },
  history: {
    recentMonths: { ...part, summary: "" },
    earlierContext: { ...part, summary: "" },
    longTermBackground: { ...part, summary: "" },
  },
  facts: [
    {
      id: "fact-one",
      content: "Use concise updates",
      category: "preference",
      confidence: 1,
      createdAt: "2026-10-01",
      source: "user",
    },
  ],
};
const clients: QueryClient[] = [];
function show(permissions = 7) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <MemoryTab
            instance={
              {
                id: "a".repeat(32),
                name: "Researcher",
                status: "active",
                generation: 1,
                permissions,
              } as api.AgentInstance
            }
          />
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  rs.mocked(api.getInstanceMemory).mockReset().mockResolvedValue(memory);
  rs.mocked(api.clearInstanceMemory).mockReset().mockResolvedValue({});
  rs.mocked(api.importInstanceMemory).mockReset().mockResolvedValue({});
  rs.mocked(api.createInstanceFact).mockReset().mockResolvedValue({});
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("shows named shared-memory copy and existing summaries and facts", async () => {
  show();
  await screen.findByText("Use concise updates");
  expect(
    screen.getByText(
      "What Researcher remembers from one conversation to the next. Everyone who can see all work can read it. It never includes anyone's private memory.",
    ),
  ).toBeDefined();
  expect(screen.getByText("Remember this")).toBeDefined();
});
test("clear memory requires its own action-specific confirmation", async () => {
  show();
  fireEvent.click(await screen.findByRole("button", { name: "Clear memory" }));
  expect(api.clearInstanceMemory).not.toHaveBeenCalled();
  const dialog = screen.getByRole("dialog");
  expect(
    within(dialog).getByText(
      "This changes everything Researcher remembers and cannot be undone.",
    ),
  ).toBeDefined();
  fireEvent.click(within(dialog).getByRole("button", { name: "Clear memory" }));
  await waitFor(() => expect(api.clearInstanceMemory).toHaveBeenCalledTimes(1));
});
test("invalid import uses localized guidance without exposing parser diagnostics", async () => {
  show();
  fireEvent.change(await screen.findByLabelText("Import memory"), {
    target: { value: "not json" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Replace memory" }));
  await screen.findByText(
    "That is not a memory export. Paste the text of a file made with Export memory.",
  );
  expect(api.importInstanceMemory).not.toHaveBeenCalled();
  expect(screen.queryByRole("dialog")).toBeNull();
});
test("oversized import is refused before parsing or sending", async () => {
  show();
  fireEvent.change(await screen.findByLabelText("Import memory"), {
    target: { value: "x".repeat(4 * 1024 * 1024 + 1) },
  });
  fireEvent.click(screen.getByRole("button", { name: "Replace memory" }));
  await screen.findByText(
    "That is too large for a memory import. Paste the text of a file made with Export memory.",
  );
  expect(api.importInstanceMemory).not.toHaveBeenCalled();
});
test("replacement confirms the captured JSON document", async () => {
  show();
  fireEvent.change(await screen.findByLabelText("Import memory"), {
    target: { value: JSON.stringify(memory) },
  });
  fireEvent.click(screen.getByRole("button", { name: "Replace memory" }));
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Replace memory",
    }),
  );
  await waitFor(() =>
    expect(api.importInstanceMemory).toHaveBeenCalledWith(
      "a".repeat(32),
      memory,
      expect.any(AbortSignal),
    ),
  );
});
test("a failed read shows only named product copy", async () => {
  rs.mocked(api.getInstanceMemory).mockRejectedValue(
    new Error("private memory backend detail"),
  );
  show();
  await screen.findByText("Researcher's memory cannot be shown right now.");
  expect(screen.queryByText("private memory backend detail")).toBeNull();
});
test("inspect-only access permits reading without mutation controls", async () => {
  show(2);
  await screen.findByText("Use concise updates");
  expect(screen.getByRole("button", { name: "Export memory" })).toBeDefined();
  expect(screen.queryByRole("button", { name: "Clear memory" })).toBeNull();
});
test("without Inspect no memory request is sent", () => {
  show(1);
  expect(api.getInstanceMemory).not.toHaveBeenCalled();
});
