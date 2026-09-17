import type { Message } from "@langchain/langgraph-sdk";
import { describe, expect, test } from "@rstest/core";

import { enUS } from "@/core/i18n/locales/en-US";
import { runActivityLabel } from "@/core/turn-progress";
import type { TurnProgress } from "@/core/turn-progress";

const human = {
  id: "h-1",
  type: "human",
  content: "Make the report",
} as Message;
const hiddenHuman = {
  id: "h-2",
  type: "human",
  content: "<slash_skill_activation>report</slash_skill_activation>",
  additional_kwargs: { hide_from_ui: true },
} as Message;
const stage = (s: TurnProgress["stage"]): TurnProgress => ({
  runId: "run-1",
  stage: s,
  atMs: 5,
});
const call = (id: string, description?: string) =>
  ({
    id,
    name: "bash",
    args: { command: "python report.py build", description },
  }) as never;

describe("runActivityLabel", () => {
  test("names the stage while the run has produced nothing the person can see", () => {
    expect(
      runActivityLabel({
        isLoading: true,
        messages: [human],
        progress: stage("preparing"),
        t: enUS,
      }),
    ).toBe("Preparing your workspace…");
    expect(
      runActivityLabel({
        isLoading: true,
        messages: [human],
        progress: stage("workspace_starting"),
        t: enUS,
      }),
    ).toBe("Starting a fresh workspace…");
    expect(
      runActivityLabel({
        isLoading: true,
        messages: [human],
        progress: stage("thinking"),
        t: enUS,
      }),
    ).toBe("Thinking…");
  });

  test("is Working… (null) before any frame and once the turn is over", () => {
    expect(
      runActivityLabel({
        isLoading: true,
        messages: [human],
        progress: null,
        t: enUS,
      }),
    ).toBeNull();
    expect(
      runActivityLabel({
        isLoading: false,
        messages: [human],
        progress: stage("thinking"),
        t: enUS,
      }),
    ).toBeNull();
  });

  test("steps aside once a tool card is on screen, whether or not it has a description", () => {
    for (const description of ["Build the August report", undefined]) {
      const messages = [
        human,
        {
          id: "ai-1",
          type: "ai",
          content: "",
          tool_calls: [call("c1", description)],
        },
      ] as Message[];
      expect(
        runActivityLabel({
          isLoading: true,
          messages,
          progress: stage("thinking"),
          t: enUS,
        }),
      ).toBeNull();
    }
  });

  test("steps aside once answer text is streaming", () => {
    const messages = [
      human,
      { id: "ai-1", type: "ai", content: "August was" },
    ] as Message[];
    expect(
      runActivityLabel({
        isLoading: true,
        messages,
        progress: stage("thinking"),
        t: enUS,
      }),
    ).toBeNull();
    const blocks = [
      human,
      { id: "ai-1", type: "ai", content: [{ type: "text", text: "August" }] },
    ] as Message[];
    expect(
      runActivityLabel({
        isLoading: true,
        messages: blocks,
        progress: stage("thinking"),
        t: enUS,
      }),
    ).toBeNull();
  });

  test("an empty AI message is not yet output", () => {
    const messages = [
      human,
      { id: "ai-1", type: "ai", content: "" },
    ] as Message[];
    expect(
      runActivityLabel({
        isLoading: true,
        messages,
        progress: stage("thinking"),
        t: enUS,
      }),
    ).toBe("Thinking…");
  });

  test("a slash-command turn's hidden human message still starts the turn", () => {
    // The previous turn ended with an unanswered tool call; this turn's own
    // message is hidden from the list. The old turn's card must not count.
    const messages = [
      human,
      { id: "ai-0", type: "ai", content: "", tool_calls: [call("c0")] },
      hiddenHuman,
    ] as Message[];
    expect(
      runActivityLabel({
        isLoading: true,
        messages,
        progress: stage("preparing"),
        t: enUS,
      }),
    ).toBe("Preparing your workspace…");
  });
});

test("is not masked by the client's own upload placeholder", () => {
  // An upload turn shows an optimistic AI message ("Uploading files…") beside
  // the human message until the server's first update replaces the list; a
  // cold sandbox makes that 10 s or more. It is not model output.
  const messages = [
    { ...human, additional_kwargs: { files: [{ filename: "x.xlsx", size: 1 }] } },
    {
      id: "opt-ai-1",
      type: "ai",
      content: "Uploading files…",
      additional_kwargs: { element: "task" },
    },
  ] as Message[];
  expect(
    runActivityLabel({
      isLoading: true,
      messages,
      progress: stage("workspace_starting"),
      t: enUS,
    }),
  ).toBe("Starting a fresh workspace…");
});
