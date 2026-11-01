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
