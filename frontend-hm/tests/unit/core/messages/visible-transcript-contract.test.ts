import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import type { Message } from "@langchain/langgraph-sdk";
import { describe, expect, it } from "@rstest/core";

import {
  extractContentFromMessage,
  INTERNAL_MARKER_TAGS,
  isHiddenFromUIMessage,
} from "@/core/messages/utils";

// The Gateway writes every transcript a person downloads; this page draws
// the same conversation. Both are held to one fixture, so a download says
// what the page showed (backend/tests/test_transcript.py reads it too, and
// also checks the context markers the transcript strips on top).

interface ContractCase {
  name: string;
  message: Message;
  visible: boolean;
  content: string;
}

const CONTRACT_PATH = resolve(
  __dirname,
  "../../../../../contracts/visible_transcript_contract.json",
);
const CONTRACT = JSON.parse(readFileSync(CONTRACT_PATH, "utf-8")) as {
  internal_marker_tags: string[];
  cases: ContractCase[];
};

describe("visible transcript contract", () => {
  it("has cases", () => {
    expect(CONTRACT.cases.length).toBeGreaterThan(20);
  });

  it("names the same injected-context tags", () => {
    expect([...INTERNAL_MARKER_TAGS]).toEqual(CONTRACT.internal_marker_tags);
  });

  for (const testCase of CONTRACT.cases) {
    it(testCase.name, () => {
      const { message } = testCase;
      const visible =
        !isHiddenFromUIMessage(message) && message.type !== "tool";
      expect(visible).toBe(testCase.visible);
      expect(extractContentFromMessage(message)).toBe(testCase.content);
    });
  }
});
