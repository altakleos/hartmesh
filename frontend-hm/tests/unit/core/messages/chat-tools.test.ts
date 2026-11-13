import type { Message } from "@langchain/langgraph-sdk";
import { describe, expect, it } from "@rstest/core";

import {
  extractWebPageKey,
  getWebPageLinksByGroupIndex,
} from "@/core/messages/chat-tools";
import type { MessageGroup } from "@/core/messages/utils";

const offer = (key: string, overrides: object = {}) =>
  ({
    type: "tool",
    content: "",
    tool_call_id: "call",
    artifact: { web_page: { version: 1, kind: "web_page", key, ...overrides } },
  }) as unknown as Message;
const text = { type: "ai", content: "An answer" } as unknown as Message;
const human: MessageGroup = {
  id: "h",
  type: "human",
  messages: [{ type: "human", content: "Hi" } as unknown as Message],
};
const processing = (...messages: Message[]): MessageGroup => ({
  id: "p",
  type: "assistant:processing",
  messages,
});
const answer: MessageGroup = { id: "a", type: "assistant", messages: [text] };

describe("extractWebPageKey", () => {
  it("reads each of the twelve page keys and refuses any other", () => {
    for (const key of [
      "access",
      "files",
      "memory",
      "conversations",
      "work",
      "agentSettings",
      "addAssistant",
      "addEmployee",
      "telegram",
      "scheduled",
      "account",
      "timezone",
    ]) {
      expect(extractWebPageKey(offer(key))).toBe(key);
    }
    expect(extractWebPageKey(offer("elsewhere"))).toBeNull();
    expect(extractWebPageKey(offer("account", { version: 2 }))).toBeNull();
    expect(extractWebPageKey(offer("account", { kind: "page" }))).toBeNull();
    expect(extractWebPageKey(text)).toBeNull();
    expect(
      extractWebPageKey({ type: "tool", content: "" } as unknown as Message),
    ).toBeNull();
  });
});

describe("getWebPageLinksByGroupIndex", () => {
  it("gives a turn's page keys to its last group, in order, without repeats, at most three", () => {
    const groups = [
      human,
      processing(
        offer("account"),
        offer("memory"),
        offer("account"),
        offer("scheduled"),
        offer("telegram"),
      ),
      answer,
    ];
    expect(Array.from(getWebPageLinksByGroupIndex(groups))).toEqual([
      [2, ["account", "memory", "scheduled"]],
    ]);
  });

  it("keeps each turn's keys to that turn", () => {
    const groups = [
      human,
      processing(offer("account")),
      answer,
      human,
      answer,
      human,
      processing(offer("memory")),
      answer,
    ];
    expect(Array.from(getWebPageLinksByGroupIndex(groups))).toEqual([
      [2, ["account"]],
      [7, ["memory"]],
    ]);
  });

  it("gives no entry to a turn that offered no page", () => {
    expect(getWebPageLinksByGroupIndex([human, answer]).size).toBe(0);
  });
});

const proposal = (draft: object = {}, card: object = {}): Message =>
  ({
    type: "tool",
    name: "propose_assignment",
    tool_call_id: "proposal-call",
    content: "For the model",
    artifact: {
      assignment_card: {
        version: 1,
        kind: "assignment_card",
        tool_call_id: "proposal-call",
        draft: {
          objective: "Review <the> guide",
          success_criteria: "Report verified gaps",
          due_text: null,
          due_at: null,
          ...draft,
        },
        ...card,
      },
    },
  }) as unknown as Message;

describe("extractAssignmentCard", () => {
  it("reads a complete version-one proposal without treating it as saved Work", async () => {
    const { extractAssignmentCard } =
      await import("@/core/messages/chat-tools");
    expect(extractAssignmentCard(proposal())).toEqual({
      objective: "Review <the> guide",
      success_criteria: "Report verified gaps",
      due_text: null,
      due_at: null,
    });
    expect(
      extractAssignmentCard(
        proposal({ due_text: "Friday", due_at: "2026-12-04T18:00:00-05:00" }),
      )?.due_text,
    ).toBe("Friday");
    expect(
      extractAssignmentCard(proposal({ objective: "😀".repeat(4096) }))
        ?.objective,
    ).toHaveLength(8192);
  });

  it("refuses invalid envelopes, text, dates and mismatched tool identities", async () => {
    const { extractAssignmentCard } =
      await import("@/core/messages/chat-tools");
    for (const draft of [
      { objective: "" },
      { objective: " " },
      { success_criteria: "x".repeat(4097) },
      { due_text: "x".repeat(121) },
      { due_at: "2026-12-01T18:00:00" },
      { due_at: "not a date" },
      { unknown: true },
    ]) {
      expect(extractAssignmentCard(proposal(draft))).toBeNull();
    }
    for (const card of [
      { version: 2 },
      { kind: "work" },
      { tool_call_id: "another-call" },
      { draft: { objective: "Only an objective" } },
    ]) {
      expect(extractAssignmentCard(proposal({}, card))).toBeNull();
    }
    expect(extractAssignmentCard(text)).toBeNull();
    expect(
      extractAssignmentCard({ ...proposal(), name: "bash" } as Message),
    ).toBeNull();
  });
});
