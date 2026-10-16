import type { Message } from "@langchain/langgraph-sdk";
import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({
  select: rs.fn(),
  setOpen: rs.fn(),
  role: "user",
}));
rs.mock("@/components/workspace/artifacts", () => ({
  useArtifacts: () => ({
    select: state.select,
    setOpen: state.setOpen,
    autoOpen: true,
    autoSelect: true,
    selectedArtifact: null,
  }),
}));
rs.mock("@/components/workspace/browser-view", () => ({
  useMaybeBrowserView: () => null,
}));
rs.mock("@/components/workspace/messages/markdown-content", () => ({
  MarkdownContent: () => null,
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: "reader", system_role: state.role } }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  useSupervisorName: () => "Jordan",
}));

import {
  ConversationFilesContext,
  type ConversationFilesState,
} from "@/components/workspace/chats/conversation-files";
import { MessageGroup } from "@/components/workspace/messages/message-group";
import { I18nProvider } from "@/core/i18n/context";
const binding = {
  id: "a".repeat(32),
  name: "Researcher",
  status: "active" as const,
  custody: "personal" as const,
  requester_id: "reader",
};
const write: Message = {
  id: "write",
  type: "ai",
  content: "",
  tool_calls: [
    {
      id: "call",
      name: "write_file",
      args: {
        path: "/mnt/user-data/outputs/report.md",
        content: "# Report",
        description: "Draft report",
      },
    },
  ],
};
function show(
  value: ConversationFilesState,
  messages: Message[] = [write],
  isLoading = true,
) {
  return render(
    <I18nProvider initialLocale="en-US">
      <ConversationFilesContext.Provider value={value}>
        <MessageGroup
          messages={messages}
          threadId="thread"
          isLoading={isLoading}
        />
      </ConversationFilesContext.Provider>
    </I18nProvider>,
  );
}
test("blocked sites remain visible below collapsed command steps and repeated hosts appear once", () => {
  const messages: Message[] = [];
  for (const [index, host] of [
    "example.test",
    "example.test",
    "other.test",
  ].entries()) {
    messages.push({
      type: "ai",
      id: `call-${index}`,
      content: "",
      tool_calls: [
        { id: `tool-${index}`, name: "bash", args: { command: "read site" } },
      ],
    });
    messages.push({
      type: "tool",
      id: `result-${index}`,
      tool_call_id: `tool-${index}`,
      name: "bash",
      content: "Denied",
      artifact: { sandbox_network_denied: { host, port: 443 } },
    });
  }
  show({ locked: true, sentence: null, agent: binding }, messages, false);
  expect(screen.getAllByText(/Researcher could not reach/)).toHaveLength(2);
  expect(
    screen.getByText(/Researcher could not reach example.test/).textContent,
  ).toContain("Ask Jordan");
  expect(screen.getByTestId("network-denials").closest("[hidden]")).toBeNull();
});
test.each([
  { role: "admin", url: "https://support.example.test/help", linked: true },
  { role: "user", url: "https://support.example.test/help", linked: false },
  { role: "admin", url: null, linked: false },
])(
  "blocked-site guidance offers configured support only to an administrator: %s",
  ({ role, url, linked }) => {
    state.role = role;
    const messages: Message[] = [
      {
        type: "ai",
        id: "command",
        content: "",
        tool_calls: [
          { id: "tool", name: "bash", args: { command: "read site" } },
        ],
      },
      {
        type: "tool",
        id: "result",
        tool_call_id: "tool",
        name: "bash",
        content: "Denied",
        artifact: {
          sandbox_network_denied: { host: "example.test", port: 443 },
        },
      },
    ];
    show(
      {
        locked: true,
        sentence: null,
        agent: binding,
        providerName: "Provider",
        supportURL: url,
      },
      messages,
      false,
    );
    const link = screen.queryByRole("link", {
      name: "Contact Provider support",
    });
    expect(link !== null).toBe(linked);
    if (linked) expect(link?.getAttribute("href")).toBe(url);
  },
);
beforeEach(() => {
  state.select.mockReset();
  state.setOpen.mockReset();
});
afterEach(cleanup);
test.each([
  { locked: true, sentence: null, agent: null },
  { locked: true, sentence: "Ask a manager.", agent: binding },
  { locked: false, sentence: null, agent: binding },
])(
  "withheld artifact views cannot auto-open or open on write steps: %j",
  async (access) => {
    show(access);
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(state.select).not.toHaveBeenCalled();
    fireEvent.click(screen.getByText("/mnt/user-data/outputs/report.md"));
    expect(state.select).not.toHaveBeenCalled();
  },
);
test("ordinary write steps retain automatic opening", async () => {
  show({ locked: false, sentence: null, agent: null });
  await new Promise((resolve) => setTimeout(resolve, 150));
  expect(state.select).toHaveBeenCalledTimes(1);
});
const browser: Message[] = [
  {
    type: "ai",
    id: "view",
    content: "",
    tool_calls: [{ id: "browser-call", name: "browser_snapshot", args: {} }],
  },
  {
    type: "tool",
    id: "result",
    tool_call_id: "browser-call",
    name: "browser_snapshot",
    content: "Captured",
    additional_kwargs: {
      browser_view: {
        screenshot: "/mnt/user-data/outputs/view.png",
        title: "Saved view",
      },
    },
  },
];
test("a historical browser screenshot makes no file request while inspection is locked", () => {
  show(
    { locked: true, sentence: "Ask a manager.", agent: binding },
    browser,
    false,
  );
  expect(document.querySelector("img")).toBeNull();
  expect(screen.getByText("Saved view").getAttribute("title")).toBe(
    "Ask a manager.",
  );
});
test("an inspector opens the historical screenshot as a file without opening a browser panel", () => {
  show({ locked: false, sentence: null, agent: binding }, browser, false);
  expect(
    screen.getByRole("link", { name: "Saved view" }).getAttribute("href"),
  ).toContain("/artifacts/mnt/user-data/outputs/view.png");
});
