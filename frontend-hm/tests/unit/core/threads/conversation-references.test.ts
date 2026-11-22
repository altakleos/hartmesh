import { describe, expect, it } from "@rstest/core";

import { selectConversationReferenceLimit } from "@/core/features/api";
import {
  pastedConversationId,
  readableReference,
} from "@/core/threads/conversation-references";

const origin = "https://example.test:8443";
describe("pasted conversation references", () => {
  it("accepts only complete same-origin chat links, matching the server's canonical thread id", () => {
    for (const path of [
      "/workspace/chats/abc_1",
      "/workspace/agents/writer/chats/abc_1",
    ]) {
      expect(pastedConversationId(origin + path, origin)).toBe("abc_1");
    }
    for (const input of [
      "abc",
      "/workspace/chats/abc",
      origin + "/workspace/chats/new",
      origin + "/workspace/chats/abc?q=1",
      origin + "/workspace/chats/abc#step",
      "http://example.test:8443/workspace/chats/abc",
      "https://foreign.test/workspace/chats/abc",
      "https://me@example.test:8443/workspace/chats/abc",
      origin + "/workspace/chats/%2e%2e",
      origin + "/workspace/chats/a/../abc",
      origin + "/workspace/chats/abc extra",
    ]) {
      expect(pastedConversationId(input, origin)).toBeNull();
    }
  });
  it("requires a positively enabled feature and valid bound", () => {
    expect(selectConversationReferenceLimit({})).toBe(0);
    expect(
      selectConversationReferenceLimit({
        conversation_references: { enabled: true, max_references: 3 },
      }),
    ).toBe(3);
    for (const cap of [null, 0, -1, true, 1.5])
      expect(
        selectConversationReferenceLimit({
          conversation_references: { enabled: true, max_references: cap },
        }),
      ).toBe(0);
    expect(
      selectConversationReferenceLimit({
        conversation_references: { enabled: false, max_references: 3 },
      }),
    ).toBe(0);
  });
  it("checks requested identity and current reader scope before revealing a title", () => {
    const row = {
      thread_id: "abc",
      owner_user_id: "me",
      values: { title: "Budget review" },
      agent_instance: null,
    };
    expect(readableReference(row, "abc", "me", undefined)).toEqual({
      id: "abc",
      title: "Budget review",
    });
    expect(readableReference(row, "other", "me", undefined)).toBeNull();
    expect(readableReference(row, "abc", "another", undefined)).toBeNull();
    expect(readableReference(row, "abc", "me", "agent")).toBeNull();
    const bound = {
      ...row,
      owner_user_id: "other",
      agent_instance: { id: "agent" },
    };
    expect(readableReference(bound, "abc", "me", "another")).toBeNull();
    expect(readableReference(bound, "abc", "me", "agent")).toEqual({
      id: "abc",
      title: "Budget review",
    });
  });
});
