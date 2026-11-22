import { describe, expect, it } from "@rstest/core";

import {
  readSharedHistory,
  ordinarySharedRows,
  sharedConversationOperations,
} from "@/core/shared-conversations/protocol";

describe("shared conversation views", () => {
  it("keeps unmarked siblings and ordinary files on earlier Gateways", () => {
    const member = {
      path: "Conversations/Review/files/one",
      snapshot_publication_id: "snap",
    };
    const sibling = { path: "Conversations/Review/files/two" };
    expect(ordinarySharedRows([member, sibling], true)).toEqual([sibling]);
    expect(ordinarySharedRows([member, sibling], false)).toEqual([
      member,
      sibling,
    ]);
  });
  it("requires an explicitly supported version and operations", () => {
    expect(sharedConversationOperations({})).toEqual([]);
    expect(
      sharedConversationOperations({
        shared_conversations: { version: 2, operations: ["publish"] },
      }),
    ).toEqual([]);
    expect(
      sharedConversationOperations({
        shared_conversations: {
          version: 1,
          operations: ["read", "publish", "other"],
        },
      }),
    ).toEqual(["read", "publish"]);
  });
  it("accepts only individually verified seed IDs and one correct boundary", () => {
    const message = (id: string, publication_id = "snap") => ({
      id,
      additional_kwargs: { shared_history: { publication_id } },
    });
    const proof = { publication_id: "snap", message_ids: ["first", "last"] };
    expect(
      readSharedHistory(
        [message("first"), message("last"), message("forged")],
        proof,
      ),
    ).toEqual({ copied: new Set(["first", "last"]), last: "last" });
    expect(readSharedHistory([message("forged")], proof).copied.size).toBe(0);
    expect(
      readSharedHistory([message("last", "other")], proof).last,
    ).toBeNull();
    expect(readSharedHistory([message("first")], proof).last).toBeNull();
    expect(readSharedHistory([message("last")], null).copied.size).toBe(0);
  });
});

describe("agent snapshots", () => {
  it("negotiates agent publishing independently from ordinary publishing", () => {
    expect(
      sharedConversationOperations({
        shared_conversations: {
          version: 1,
          operations: ["publish", "agent_publish"],
        },
      }),
    ).toEqual(["publish", "agent_publish"]);
  });
});
