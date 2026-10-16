import { afterEach, expect, test, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

rs.mock("@/components/workspace/messages/markdown-content", () => ({
  MarkdownContent: ({ content }: { content: string }) => <div>{content}</div>,
}));
import {
  ConversationFilesContext,
  type ConversationFilesState,
} from "@/components/workspace/chats/conversation-files";
import { HumanInputCard } from "@/components/workspace/messages/human-input-card";
import { I18nProvider } from "@/core/i18n/context";
import type {
  HumanInputRequest,
  HumanInputResponse,
} from "@/core/messages/human-input";

afterEach(cleanup);
const binding = {
  id: "a".repeat(32),
  name: "Researcher",
  status: "active" as const,
  custody: "personal" as const,
  requester_id: "reader",
};
const request: HumanInputRequest = {
  version: 1,
  kind: "human_input_request",
  source: "sandbox_network",
  request_id: "request",
  title: "Server network title",
  question: "Server question",
  context: "Server context",
  input_mode: "single_choice",
  details: { host: "example.test", port: 443, ttl_seconds: 300 },
  options: [
    { id: "deny", value: "deny", label: "Server deny" },
    {
      id: "allow_temporary",
      value: "allow_temporary",
      label: "Server temporary",
    },
  ],
};
function show({
  agent = binding as ConversationFilesState["agent"],
  body = request,
  answered = null as HumanInputResponse | null,
  refused = "",
} = {}) {
  const submit = rs.fn(() => true);
  render(
    <I18nProvider initialLocale="en-US">
      <ConversationFilesContext.Provider
        value={{
          locked: false,
          sentence: null,
          agent,
          providerName: "Example provider",
          refusedNetworkRequests: new Set(refused ? [refused] : []),
        }}
      >
        <HumanInputCard
          request={body}
          answeredResponse={answered}
          onSubmit={submit}
        />
      </ConversationFilesContext.Provider>
    </I18nProvider>,
  );
  return submit;
}
test("agent network cards name the agent and preserve exact option values", () => {
  const submit = show();
  expect(screen.getByText("A site is blocked")).toBeDefined();
  expect(
    screen.getByText(/Researcher tried to reach example.test/).textContent,
  ).toContain("Example provider");
  expect(screen.queryByText("Server context")).toBeNull();
  expect(screen.getByRole("button", { name: "Keep blocked" })).toBeDefined();
  fireEvent.click(screen.getByRole("button", { name: "Allow for 5 minutes" }));
  expect(submit).toHaveBeenCalledWith(
    expect.objectContaining({
      option_id: "allow_temporary",
      value: "allow_temporary",
      request_id: "request",
    }),
  );
});
test("an ordinary chat keeps the server's three-option card", () => {
  show({
    agent: null,
    body: {
      ...request,
      options: [
        ...request.options!,
        {
          id: "allow_sandbox",
          value: "allow_sandbox",
          label: "Allow for this sandbox",
        },
      ],
    },
  });
  expect(screen.getByText("Server network title")).toBeDefined();
  expect(screen.getByText("Server context")).toBeDefined();
  expect(screen.getByText("Server question")).toBeDefined();
  expect(screen.getAllByRole("button")).toHaveLength(3);
  expect(
    screen.getByRole("button", { name: "Allow for this sandbox" }),
  ).toBeDefined();
});
test("a request without typed details keeps its server wording in an agent conversation", () => {
  show({ body: { ...request, details: undefined } });
  expect(screen.getByText("Server question")).toBeDefined();
  expect(
    screen.getByRole("button", { name: "Server temporary" }),
  ).toBeDefined();
});
test.each(["allow_temporary", "deny"])(
  "answered agent network card describes %s without printing its wire value",
  (value) => {
    show({
      answered: {
        version: 1,
        kind: "human_input_response",
        source: "sandbox_network",
        request_id: "request",
        response_kind: "option",
        option_id: value,
        value,
      },
    });
    expect(
      screen.getByText(
        value === "deny"
          ? "example.test stays blocked."
          : /example.test is allowed for 5 minutes/,
      ),
    ).toBeDefined();
    expect(screen.queryByText(`You answered: ${value}`)).toBeNull();
  },
);
test("a managers-only refusal is associated with its network card even after its answer was recorded", () => {
  show({
    refused: "request",
    answered: {
      version: 1,
      kind: "human_input_response",
      source: "sandbox_network",
      request_id: "request",
      response_kind: "option",
      option_id: "allow_temporary",
      value: "allow_temporary",
    },
  });
  expect(
    screen.getByText("Only people who can manage Researcher can allow a site."),
  ).toBeDefined();
  expect(screen.queryByText(/example.test is allowed/)).toBeNull();
});
test("a matching network refusal is visible before the answered message returns", () => {
  show({ refused: "request" });
  expect(screen.getByRole("alert").textContent).toBe(
    "Only people who can manage Researcher can allow a site.",
  );
});
