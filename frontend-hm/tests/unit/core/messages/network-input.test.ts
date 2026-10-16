import type { Message } from "@langchain/langgraph-sdk";
import { expect, test } from "@rstest/core";

import { parseHumanInputRequest } from "@/core/messages/human-input";
import { getMessageGroups } from "@/core/messages/utils";

const request = {
  version: 1,
  kind: "human_input_request",
  source: "sandbox_network",
  request_id: "network-request",
  question: "Server question",
  input_mode: "single_choice",
  options: [{ id: "deny", label: "Server deny", value: "deny" }],
  details: { host: "example.test", port: 443, ttl_seconds: 300 },
};
test("preserves typed network details for presentation without requiring them for ordinary cards", () => {
  expect(parseHumanInputRequest(request)?.details).toEqual(request.details);
  expect(
    parseHumanInputRequest({
      ...request,
      details: { host: "example.test", port: "443", ttl_seconds: 300 },
    })?.details,
  ).toBeUndefined();
  expect(
    parseHumanInputRequest({ ...request, details: undefined })?.question,
  ).toBe(request.question);
});
test("a human request on any tool result opens a card while retaining the result among its steps", () => {
  const call: Message = {
    type: "ai",
    id: "command",
    content: "",
    tool_calls: [
      { id: "tool", name: "bash", args: { command: "read remote site" } },
    ],
  };
  const result: Message = {
    type: "tool",
    id: "result",
    tool_call_id: "tool",
    name: "bash",
    content: "Needs permission",
    artifact: { human_input: request },
  };
  const groups = getMessageGroups([call, result]);
  expect(
    groups.find((group) => group.type === "assistant:processing")?.messages,
  ).toContain(result);
  expect(
    groups.find((group) => group.type === "assistant:clarification")?.messages,
  ).toEqual([result]);
});
