import type { Message } from "@langchain/langgraph-sdk";
import { describe, expect, it } from "@rstest/core";

import { getMessageGroups } from "@/core/messages/utils";

const tool: Message = {
  type: "tool",
  id: "proposal",
  name: "propose_assignment",
  tool_call_id: "call",
  content: "For the model",
  artifact: {
    assignment_card: {
      version: 1,
      kind: "assignment_card",
      tool_call_id: "call",
      draft: {
        objective: "Review the guide",
        success_criteria: "Report gaps",
        due_text: null,
        due_at: null,
      },
    },
  },
} as unknown as Message;
const call: Message = {
  type: "ai",
  id: "step",
  content: "",
  tool_calls: [{ id: "call", name: "propose_assignment", args: {} }],
} as Message;

describe("assignment proposal grouping", () => {
  it("keeps a readable proposal among the steps and opens a terminal card", () => {
    const groups = getMessageGroups([
      call,
      tool,
      {
        type: "tool",
        id: "later",
        content: "Later tool",
        tool_call_id: "other",
      } as Message,
    ]);
    expect(groups.map((group) => group.type)).toEqual([
      "assistant:processing",
      "assistant:assignment-card",
      "assistant:processing",
    ]);
    expect(groups[0]?.messages).toContain(tool);
    expect(groups[1]?.messages).toEqual([tool]);
    expect(groups[2]?.messages[0]?.id).toBe("later");
  });
  it("keeps model-only refusals as steps without a card", () => {
    expect(
      getMessageGroups([call, { ...tool, artifact: undefined } as Message]).map(
        (group) => group.type,
      ),
    ).toEqual(["assistant:processing"]);
  });
});
