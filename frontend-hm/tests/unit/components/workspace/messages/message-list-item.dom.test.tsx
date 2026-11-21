import type { Message } from "@langchain/langgraph-sdk";
import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render } from "@testing-library/react";

import { MessageListItem } from "@/components/workspace/messages/message-list-item";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import * as messageUtils from "@/core/messages/utils";

// The unit under test is the row's copy-data memo, not message rendering.
// Stubbing the markdown pipeline also removes its first-render suspension:
// when a settled body suspends, React restarts the render attempt and
// legitimately re-runs useMemo factories, which would make call counts
// depend on microtask timing instead of the memo's own deps.
rs.mock("@/components/workspace/messages/markdown-content", () => ({
  MarkdownContent: () => null,
}));

// Count calls to the copy-data derivation itself. The render path reads the
// message content for the body (markdown, reasoning, tasks), so content
// getters cannot distinguish rendering reads from copy derivation — the spy
// on the derived entry point can.
rs.mock("@/components/workspace/person-name", () => ({
  usePersonName: (id: string) => {
    authority.name(id);
    return null;
  },
}));

const authority = rs.hoisted(() => ({
  run: undefined as unknown,
  read: rs.fn(),
  name: rs.fn(),
}));
rs.mock("@/core/threads/hooks", () => ({
  useRunDetail: (thread: string, run: string) => {
    authority.read(thread, run);
    return { data: authority.run, isError: false };
  },
}));
const copyDataCalls = rs.spyOn(messageUtils, "getMessageCopyData");

function makeMessage(type: "human" | "ai"): Message {
  return {
    id: `${type}-1`,
    type,
    content: "copy me",
  } as unknown as Message;
}

function withI18n(ui: React.ReactElement) {
  return (
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      {ui}
    </I18nContext.Provider>
  );
}

function renderRow(
  message: Message,
  {
    showCopyButton,
    isLoading = false,
  }: { showCopyButton: boolean; isLoading?: boolean },
) {
  return render(
    withI18n(
      <MessageListItem
        message={message}
        threadId="thread-1"
        showCopyButton={showCopyButton}
        isLoading={isLoading}
      />,
    ),
  );
}

afterEach(cleanup);
afterEach(() => {
  copyDataCalls.mockClear();
});

describe("MessageListItem copy-data derivation guard", () => {
  it("derives no copy data for an assistant row that never renders the toolbar", () => {
    const message = makeMessage("ai");

    const view = renderRow(message, { showCopyButton: false });
    expect(copyDataCalls).toHaveBeenCalledTimes(0);

    // The sole call site passes showCopyButton only for non-assistant rows;
    // the memo must stay gated so settled assistant rows skip the derivation
    // entirely (the pre-memo call sat behind the same toolbar guard).
    view.rerender(
      withI18n(
        <MessageListItem
          message={message}
          threadId="thread-1"
          showCopyButton={false}
          isLoading={false}
        />,
      ),
    );
    expect(copyDataCalls).toHaveBeenCalledTimes(0);
  });

  it("derives a human row's copy data once and reuses it across re-renders", () => {
    const message = makeMessage("human");

    const view = renderRow(message, { showCopyButton: true });
    // One memo serves both editing and the toolbar — not one derivation per
    // consumer.
    expect(copyDataCalls).toHaveBeenCalledTimes(1);

    view.rerender(
      withI18n(
        <MessageListItem
          message={message}
          threadId="thread-1"
          showCopyButton={true}
          isLoading={false}
        />,
      ),
    );
    expect(copyDataCalls).toHaveBeenCalledTimes(1);
  });

  it("still derives for a human row while loading (editing needs it)", () => {
    const message = makeMessage("human");

    renderRow(message, { showCopyButton: true, isLoading: true });

    expect(copyDataCalls.mock.calls.length).toBeGreaterThan(0);
  });
});

it("attributes only an employee human row bound to the same current run receipt", () => {
  const run = "f11d919a-1436-4662-bdf7-a880142c49a1";
  const marker = { package: "daily-notes", user_id: "member-1", run_id: run };
  const message = {
    ...makeMessage("human"),
    additional_kwargs: { deerflow_package_origin: marker },
  } as Message;
  const row = (employee: boolean, msg: Message) =>
    withI18n(
      <MessageListItem
        employeeConversation={employee}
        threadId="thread-1"
        message={msg}
      />,
    );
  const meta = {
    deerflow_origin: { kind: "extension", namespace: "daily-notes" },
    requester_id: "member-1",
    deerflow_package_turn: { run_id: run, message_ids: [message.id] },
  };
  const record = { thread_id: "thread-1", run_id: run, metadata: meta };
  authority.run = undefined;
  authority.name.mockClear();
  authority.read.mockClear();
  const view = render(row(true, message));
  expect(view.queryByText(/Started by/)).toBeNull();
  expect(authority.name).not.toHaveBeenCalled();
  for (const rejected of [
    { ...record, thread_id: "another-thread" },
    { ...record, run_id: "another-run" },
    { ...record, metadata: { ...meta, requester_id: "other" } },
    {
      ...record,
      metadata: {
        ...meta,
        deerflow_origin: { kind: "human", namespace: "daily-notes" },
      },
    },
    {
      ...record,
      metadata: {
        ...meta,
        deerflow_origin: { kind: "extension", namespace: "other" },
      },
    },
    {
      ...record,
      metadata: {
        ...meta,
        deerflow_package_turn: { run_id: run, message_ids: ["other"] },
      },
    },
    {
      ...record,
      metadata: {
        ...meta,
        deerflow_package_turn: { run_id: "other", message_ids: [message.id] },
      },
    },
    { ...record, metadata: { ...meta, deerflow_package_turn: null } },
  ]) {
    authority.run = rejected;
    view.rerender(row(true, message));
    expect(view.queryByText(/Started by/)).toBeNull();
  }
  authority.run = record;
  view.rerender(row(true, message));
  expect(view.getByText("Started by daily-notes for member-1")).toBeDefined();
  expect(authority.read).toHaveBeenCalledWith("thread-1", run);
  const otherRunMessage = { ...message, run_id: "another-run" };
  view.rerender(row(true, otherRunMessage));
  expect(view.queryByText(/Started by/)).toBeNull();
  view.rerender(row(false, message));
  expect(view.queryByText(/Started by/)).toBeNull();
  view.rerender(row(true, { ...message, type: "ai" } as Message));
  expect(view.queryByText(/Started by/)).toBeNull();
  authority.read.mockClear();
  view.rerender(
    row(true, {
      ...message,
      additional_kwargs: {
        deerflow_package_origin: { ...marker, run_id: "old" },
      },
    } as Message),
  );
  expect(authority.read).not.toHaveBeenCalled();
});
