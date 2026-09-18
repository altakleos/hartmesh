import type { Message } from "@langchain/langgraph-sdk";
import { describe, expect, test } from "@rstest/core";

import {
  areStreamMetadataSnapshotsEqual,
  extractContentFromMessage,
  extractPresentFilesFromMessage,
  extractTextFromMessage,
  extractReasoningContentFromMessage,
  getBranchableAssistantGroupIds,
  getLatestEditableTurn,
  getMessageCopyData,
  getAssistantTurnCopyData,
  getAssistantTurnUsageMessages,
  getMessageGroups,
  getStreamMetadataSnapshot,
  getStreamingMessageLookup,
  hasContent,
  hasPresentFiles,
  hasReasoning,
  isAssistantMessageGroupStreaming,
  isHiddenFromUIMessage,
  parseUploadedFiles,
  stripInternalMarkers,
  stripUploadedFilesTag,
} from "@/core/messages/utils";

function aiMessage(content: string): Message {
  return {
    id: "ai-1",
    type: "ai",
    content,
  } as Message;
}

test("aggregates token usage messages once per assistant turn", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Plan a trip",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "",
      tool_calls: [{ id: "tool-1", name: "web_search", args: {} }],
      usage_metadata: { input_tokens: 10, output_tokens: 5, total_tokens: 15 },
    },
    {
      id: "tool-1-result",
      type: "tool",
      name: "web_search",
      tool_call_id: "tool-1",
      content: "[]",
    },
    {
      id: "ai-2",
      type: "ai",
      content: "Here is the itinerary",
      usage_metadata: { input_tokens: 2, output_tokens: 8, total_tokens: 10 },
    },
    {
      id: "human-2",
      type: "human",
      content: "Make it shorter",
    },
    {
      id: "ai-3",
      type: "ai",
      content: "Short version",
      usage_metadata: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
    },
  ] as Message[];

  const groups = getMessageGroups(messages);
  const usageMessagesByGroupIndex = getAssistantTurnUsageMessages(groups);

  expect(groups.map((group) => group.type)).toEqual([
    "human",
    "assistant:processing",
    "assistant",
    "human",
    "assistant",
  ]);

  expect(
    usageMessagesByGroupIndex.map(
      (groupMessages) => groupMessages?.map((message) => message.id) ?? null,
    ),
  ).toEqual([null, null, ["ai-1", "ai-2"], null, ["ai-3"]]);
});

describe("branchable assistant groups", () => {
  const messages = [
    { id: "human-1", type: "human", content: "First question" },
    { id: "ai-history", type: "ai", content: "Historical final answer" },
    { id: "human-2", type: "human", content: "Complex question" },
    { id: "ai-intermediate", type: "ai", content: "Intermediate answer" },
    {
      id: "ai-tool",
      type: "ai",
      content: "",
      tool_calls: [{ id: "tool-1", name: "write_todos", args: {} }],
    },
    {
      id: "tool-result",
      type: "tool",
      name: "write_todos",
      tool_call_id: "tool-1",
      content: "Todos updated",
    },
    { id: "ai-final", type: "ai", content: "Final answer" },
  ] as Message[];

  test("keeps historical turns branchable and selects only the final AI group in the current completed turn", () => {
    const groups = getMessageGroups(messages);

    expect([...getBranchableAssistantGroupIds(groups, false)]).toEqual([
      "ai-history",
      "ai-final",
    ]);
  });

  test("does not expose the current turn while it is still loading", () => {
    const groups = getMessageGroups(messages);

    expect([...getBranchableAssistantGroupIds(groups, true)]).toEqual([
      "ai-history",
    ]);
  });

  test("does not expose a completed turn that ends in processing", () => {
    const groups = getMessageGroups(messages.slice(0, -1));

    expect([...getBranchableAssistantGroupIds(groups, false)]).toEqual([
      "ai-history",
    ]);
  });
});

describe("latest editable user turn", () => {
  test("returns the latest completed human turn", () => {
    const messages = [
      { id: "human-1", type: "human", content: "First question" },
      { id: "ai-history", type: "ai", content: "Historical answer" },
      { id: "human-2", type: "human", content: "Use tools" },
      {
        id: "ai-tool",
        type: "ai",
        content: "",
        tool_calls: [{ id: "tool-1", name: "web_search", args: {} }],
      },
      {
        id: "tool-result",
        type: "tool",
        name: "web_search",
        tool_call_id: "tool-1",
        content: "result",
      },
      { id: "ai-final", type: "ai", content: "Final answer" },
    ] as Message[];

    const turn = getLatestEditableTurn(getMessageGroups(messages), false);

    expect(turn?.humanMessage.id).toBe("human-2");
  });

  test("returns null while the current turn is loading", () => {
    const messages = [
      { id: "human-1", type: "human", content: "Question" },
      { id: "ai-1", type: "ai", content: "Answer" },
    ] as Message[];

    expect(getLatestEditableTurn(getMessageGroups(messages), true)).toBeNull();
  });

  test("returns null when the latest turn has no final assistant text", () => {
    const messages = [
      { id: "human-1", type: "human", content: "Question" },
      {
        id: "ai-tool",
        type: "ai",
        content: "",
        tool_calls: [{ id: "tool-1", name: "web_search", args: {} }],
      },
      {
        id: "tool-result",
        type: "tool",
        name: "web_search",
        tool_call_id: "tool-1",
        content: "result",
      },
    ] as Message[];

    expect(getLatestEditableTurn(getMessageGroups(messages), false)).toBeNull();
  });

  test("returns null when the latest assistant text still has tool calls", () => {
    const messages = [
      { id: "human-1", type: "human", content: "Question" },
      {
        id: "ai-tool",
        type: "ai",
        content: "Let me search first.",
        tool_calls: [{ id: "tool-1", name: "web_search", args: {} }],
      },
    ] as Message[];

    expect(getLatestEditableTurn(getMessageGroups(messages), false)).toBeNull();
  });

  test("returns null when a later human turn is incomplete", () => {
    const messages = [
      { id: "human-1", type: "human", content: "First question" },
      { id: "ai-1", type: "ai", content: "First answer" },
      { id: "human-2", type: "human", content: "Follow up" },
    ] as Message[];

    expect(getLatestEditableTurn(getMessageGroups(messages), false)).toBeNull();
  });
});

test("reasoning + content (no tool calls) yields a single assistant bubble, not a duplicate processing group", () => {
  // Regression for #3868: in thinking/pro/ultra modes the final assistant
  // message carries both reasoning_content and answer text. It must surface its
  // reasoning exactly once — inside the assistant bubble's <Reasoning>
  // collapsible. Routing the same message into a processing group as well makes
  // the ChainOfThought panel above the bubble paint the identical reasoning a
  // second time.
  const messages = [
    { id: "human-1", type: "human", content: "Why is the sky blue?" },
    {
      id: "ai-1",
      type: "ai",
      content: "Rayleigh scattering makes the sky blue.",
      additional_kwargs: { reasoning_content: "Recall Rayleigh scattering." },
    },
  ] as Message[];

  const groups = getMessageGroups(messages);

  expect(groups.map((group) => group.type)).toEqual(["human", "assistant"]);

  // The reasoning-bearing message lands in exactly one group, so turn-usage
  // aggregation never double-counts it (see #2770).
  const turnUsage = getAssistantTurnUsageMessages(groups);
  expect(turnUsage.at(-1)?.map((message) => message.id)).toEqual(["ai-1"]);
});

test("keeps unresolved streaming text in the processing group when tool calls arrive later", () => {
  const textOnlyMessages = [
    { id: "human-1", type: "human", content: "Create a presentation" },
    {
      id: "ai-1",
      type: "ai",
      content: "I will inspect the source material first.",
    },
  ] as Message[];

  const textOnlyGroups = getMessageGroups(textOnlyMessages, {
    isCurrentTurnLoading: true,
  });
  expect(textOnlyGroups.map((group) => group.type)).toEqual([
    "human",
    "assistant:processing",
  ]);

  const withToolCall = [
    textOnlyMessages[0],
    {
      ...textOnlyMessages[1],
      tool_calls: [
        { id: "call-1", name: "read_file", args: { path: "slides.md" } },
      ],
    },
  ] as Message[];
  const toolCallGroups = getMessageGroups(withToolCall, {
    isCurrentTurnLoading: true,
  });
  expect(toolCallGroups.map((group) => group.type)).toEqual([
    "human",
    "assistant:processing",
  ]);
  expect(toolCallGroups[1]?.id).toBe(textOnlyGroups[1]?.id);

  expect(getMessageGroups(textOnlyMessages).map((group) => group.type)).toEqual(
    ["human", "assistant"],
  );
});

test("keeps post-tool streaming text in the processing group until the turn settles", () => {
  const messages = [
    { id: "human-1", type: "human", content: "Inspect and summarize" },
    {
      id: "ai-1",
      type: "ai",
      content: "I will inspect the current implementation.",
      tool_calls: [
        { id: "call-1", name: "read_file", args: { path: "source.ts" } },
      ],
    },
    {
      id: "tool-1",
      type: "tool",
      name: "read_file",
      tool_call_id: "call-1",
      content: "file contents",
    },
    {
      id: "ai-2",
      type: "ai",
      content: "Here is the final streamed answer.",
    },
  ] as Message[];

  const loadingGroups = getMessageGroups(messages, {
    isCurrentTurnLoading: true,
  });
  expect(loadingGroups.map((group) => group.type)).toEqual([
    "human",
    "assistant:processing",
  ]);
  expect(loadingGroups[1]?.messages.map((message) => message.id)).toEqual([
    "ai-1",
    "tool-1",
    "ai-2",
  ]);

  expect(getMessageGroups(messages).map((group) => group.type)).toEqual([
    "human",
    "assistant:processing",
    "assistant",
  ]);
});

test("keeps tool-call reasoning in the processing group while the final answer's reasoning rides its own bubble", () => {
  // Companion to #3868: only the message that also becomes an assistant bubble
  // (content, no tool calls) is pulled out of the processing group. Reasoning
  // attached to an intermediate tool-calling step still belongs above, with its
  // tool steps.
  const messages = [
    { id: "human-1", type: "human", content: "Search and summarize" },
    {
      id: "ai-1",
      type: "ai",
      content: "",
      additional_kwargs: { reasoning_content: "I should search first." },
      tool_calls: [{ id: "tool-1", name: "web_search", args: { query: "x" } }],
    },
    {
      id: "tool-1-result",
      type: "tool",
      name: "web_search",
      tool_call_id: "tool-1",
      content: "[]",
    },
    {
      id: "ai-2",
      type: "ai",
      content: "Here is the summary.",
      additional_kwargs: { reasoning_content: "Synthesize the findings." },
    },
  ] as Message[];

  const groups = getMessageGroups(messages);

  expect(groups.map((group) => group.type)).toEqual([
    "human",
    "assistant:processing",
    "assistant",
  ]);
  expect(groups[1]?.messages.map((message) => message.id)).toEqual([
    "ai-1",
    "tool-1-result",
  ]);
  expect(groups[2]?.messages.map((message) => message.id)).toEqual(["ai-2"]);
});

describe("inline <think> tag splitting", () => {
  test("strips a fully closed <think> block from AI content", () => {
    const message = aiMessage("<think>internal reasoning</think>final answer");
    expect(extractContentFromMessage(message)).toBe("final answer");
    expect(extractReasoningContentFromMessage(message)).toBe(
      "internal reasoning",
    );
  });

  test("strips multiple closed <think> blocks and joins their reasoning", () => {
    const message = aiMessage(
      "<think>step one</think>between<think>step two</think>after",
    );
    expect(extractContentFromMessage(message)).toBe("betweenafter");
    expect(extractReasoningContentFromMessage(message)).toBe(
      "step one\n\nstep two",
    );
  });

  test("during streaming, an unclosed <think> tag does not leak its tail into content", () => {
    // Simulates accumulated content mid-stream, before </think> arrives.
    const message = aiMessage(
      "<think>I need to analyze the user's question step by",
    );
    expect(extractContentFromMessage(message)).toBe("");
    expect(extractContentFromMessage(message)).not.toContain("<think>");
    expect(extractReasoningContentFromMessage(message)).toBe(
      "I need to analyze the user's question step by",
    );
  });

  test("preamble before an unclosed <think> stays in content", () => {
    const message = aiMessage(
      "Here is part of the answer.<think>but wait, let me reconsider",
    );
    expect(extractContentFromMessage(message)).toBe(
      "Here is part of the answer.",
    );
    expect(extractReasoningContentFromMessage(message)).toBe(
      "but wait, let me reconsider",
    );
  });

  test("closed <think> followed by a trailing unclosed <think> merges both into reasoning", () => {
    const message = aiMessage(
      "<think>first step</think>partial answer<think>second step still streaming",
    );
    expect(extractContentFromMessage(message)).toBe("partial answer");
    expect(extractReasoningContentFromMessage(message)).toBe(
      "first step\n\nsecond step still streaming",
    );
  });

  test("hasReasoning recognises an unclosed <think> tag mid-stream", () => {
    expect(hasReasoning(aiMessage("<think>thinking in progress"))).toBe(true);
  });

  test("hasContent excludes an unclosed <think> tail when no preamble exists", () => {
    expect(hasContent(aiMessage("<think>thinking in progress"))).toBe(false);
  });

  test("hasContent stays true when preamble precedes an unclosed <think>", () => {
    expect(hasContent(aiMessage("preamble<think>still thinking"))).toBe(true);
  });

  test("a lone <think> open tag with no body yields no reasoning and no content", () => {
    const message = aiMessage("<think>");
    expect(extractContentFromMessage(message)).toBe("");
    expect(extractReasoningContentFromMessage(message)).toBeNull();
    expect(hasReasoning(message)).toBe(false);
  });

  test("a literal <think> inside markdown inline code is not treated as reasoning", () => {
    const message = aiMessage(
      "Use `<think>` markers to delimit reasoning sections.",
    );
    expect(extractContentFromMessage(message)).toBe(
      "Use `<think>` markers to delimit reasoning sections.",
    );
    expect(extractReasoningContentFromMessage(message)).toBeNull();
    expect(hasReasoning(message)).toBe(false);
  });

  test("a closed <think> pair inside markdown inline code stays in content", () => {
    // The model talking about the tag literally, e.g. when explaining prompt
    // engineering. Hollowing the code span out into the reasoning panel
    // corrupts the visible answer.
    const message = aiMessage(
      "Wrap your reasoning in `<think>your reasoning</think>` before answering.",
    );
    expect(extractContentFromMessage(message)).toBe(
      "Wrap your reasoning in `<think>your reasoning</think>` before answering.",
    );
    expect(extractReasoningContentFromMessage(message)).toBeNull();
    expect(hasReasoning(message)).toBe(false);
  });

  test("a closed <think> pair outside code is still stripped when another sits in inline code", () => {
    const message = aiMessage(
      "<think>real reasoning</think>Use `<think>text</think>` markers.",
    );
    expect(extractContentFromMessage(message)).toBe(
      "Use `<think>text</think>` markers.",
    );
    expect(extractReasoningContentFromMessage(message)).toBe("real reasoning");
  });

  test("a backtick-prefixed <think> mid-stream is not split into reasoning", () => {
    // Simulates the moment the model has emitted the opening backtick and
    // `<think>` for a literal documentation reference, before the closing
    // backtick arrives. The pre-fix behaviour would have permanently
    // truncated the content here.
    const message = aiMessage("Documentation: `<think>");
    expect(extractContentFromMessage(message)).toBe("Documentation: `<think>");
    expect(extractReasoningContentFromMessage(message)).toBeNull();
  });

  test("re-splits when the same message object gets new content", () => {
    // Streaming replaces `content` on the accumulating message, so a split
    // cached against the message object must be keyed by the content it was
    // derived from.
    const message = aiMessage("<think>first</think>one");

    expect(extractContentFromMessage(message)).toBe("one");
    expect(extractReasoningContentFromMessage(message)).toBe("first");

    (message as { content: string }).content = "<think>second</think>two";

    expect(extractContentFromMessage(message)).toBe("two");
    expect(extractReasoningContentFromMessage(message)).toBe("second");
    expect(hasReasoning(message)).toBe(true);
  });
});

describe("isHiddenFromUIMessage", () => {
  function contentReadCounter(
    message: Omit<Message, "content">,
    content: string,
  ) {
    const counter = { reads: 0 };
    const probe = {
      ...message,
      get content() {
        counter.reads++;
        return content;
      },
    } as unknown as Message;
    return { probe, counter };
  }

  test("does not read AI content to decide visibility", () => {
    // Only the human branch consults the text, but this predicate runs over
    // every message on every stream chunk (grouping, dedup, human-input
    // state), so reading AI content here costs a full content scan per
    // message per chunk.
    const { probe, counter } = contentReadCounter(
      { id: "ai-visible", type: "ai" } as Message,
      "<think>long reasoning</think>a long streamed answer",
    );

    expect(isHiddenFromUIMessage(probe)).toBe(false);
    expect(counter.reads).toBe(0);
  });

  test("does not read content when a control message name already hides it", () => {
    const { probe, counter } = contentReadCounter(
      { id: "summary-1", type: "ai", name: "summary" } as Message,
      "compressed context",
    );

    expect(isHiddenFromUIMessage(probe)).toBe(true);
    expect(counter.reads).toBe(0);
  });

  test("still hides a human message that is only slash skill activation", () => {
    const message = {
      id: "slash-activation",
      type: "human",
      content:
        "<slash_skill_activation>\n<skill_content># SKILL.md</skill_content>\n</slash_skill_activation>",
    } as Message;

    expect(isHiddenFromUIMessage(message)).toBe(true);
  });

  test("keeps a human message that carries real text alongside the activation", () => {
    const message = {
      id: "slash-activation-with-task",
      type: "human",
      content:
        "<slash_skill_activation>\n<skill_content># SKILL.md</skill_content>\n</slash_skill_activation>\nreal user task",
    } as Message;

    expect(isHiddenFromUIMessage(message)).toBe(false);
  });

  test("hides any message flagged with hide_from_ui", () => {
    const message = {
      id: "hidden-1",
      type: "human",
      content: "internal reply",
      additional_kwargs: { hide_from_ui: true },
    } as Message;

    expect(isHiddenFromUIMessage(message)).toBe(true);
  });
});

describe("human message internal context stripping", () => {
  test("strips legacy uploaded_files context from copy data", () => {
    // Display-only backward compatibility (#4212): pre-#4174 history still
    // carries <uploaded_files> blocks, which copy data must strip rather
    // than leak as raw XML with server-side paths.
    const message = {
      id: "human-with-legacy-upload",
      type: "human",
      content:
        "<uploaded_files>\nThe following files were uploaded in this message:\n\n- paper.pdf (1.0 MB)\n  Path: /mnt/user-data/uploads/paper.pdf\n</uploaded_files>\n\nSummarize this paper",
    } as Message;

    expect(getMessageCopyData(message)).toBe("Summarize this paper");
  });

  test("strips current_uploads context from copy data", () => {
    // Mirrors the block UploadsMiddleware emits since #4174, including the
    // trailing usage-guidance lines.
    const message = {
      id: "human-with-current-uploads",
      type: "human",
      content:
        "<current_uploads>\nThe following files were uploaded in this message:\n\n- paper.docx (177.6 KB)\n  Path: /mnt/user-data/uploads/paper.docx\n\nTo work with these files:\n- Use `grep` to search for keywords\n  (e.g. `grep(pattern='revenue', path='/mnt/user-data/uploads/')`).\n</current_uploads>\n\nMake a slide deck from this",
    } as Message;

    expect(getMessageCopyData(message)).toBe("Make a slide deck from this");
  });

  test("parses uploaded files from a current_uploads block", () => {
    const content =
      "<current_uploads>\nThe following files were uploaded in this message:\n\n- paper.docx (177.6 KB)\n  Path: /mnt/user-data/uploads/paper.docx\n  Document outline (use `read_file` with line ranges to read sections):\n    L1: Introduction\n- data.xlsx (12.0 KB)\n  Path: /mnt/user-data/uploads/data.xlsx\n</current_uploads>\n\nSummarize";

    // size is bytes (FileInMessage contract): the block's "177.6 KB" /
    // "12.0 KB" are converted back from the human-readable form the backend
    // emits, so formatBytes re-renders them at the original magnitude.
    expect(parseUploadedFiles(content)).toEqual([
      {
        filename: "paper.docx",
        size: Math.round(177.6 * 1024), // 181862
        path: "/mnt/user-data/uploads/paper.docx",
      },
      {
        filename: "data.xlsx",
        size: 12 * 1024, // 12288
        path: "/mnt/user-data/uploads/data.xlsx",
      },
    ]);
  });

  test("parses uploaded filenames that contain parentheses", () => {
    // Browsers name duplicate downloads "photo (1).png"; the backend emits the
    // filename verbatim, so the parser must not stop the name at the first "(".
    const content =
      "<current_uploads>\nThe following files were uploaded in this message:\n\n- photo (1).png (12.3 KB)\n  Path: /mnt/user-data/uploads/photo (1).png\n- report (final) (2).docx (1.5 MB)\n  Path: /mnt/user-data/uploads/report (final) (2).docx\n- normal.pdf (3.0 KB)\n  Path: /mnt/user-data/uploads/normal.pdf\n</current_uploads>\n\nSummarize";

    expect(parseUploadedFiles(content)).toEqual([
      {
        filename: "photo (1).png",
        size: Math.round(12.3 * 1024),
        path: "/mnt/user-data/uploads/photo (1).png",
      },
      {
        filename: "report (final) (2).docx",
        size: Math.round(1.5 * 1024 * 1024),
        path: "/mnt/user-data/uploads/report (final) (2).docx",
      },
      {
        filename: "normal.pdf",
        size: 3 * 1024,
        path: "/mnt/user-data/uploads/normal.pdf",
      },
    ]);
  });

  test("stripInternalMarkers removes current_uploads blocks on export", () => {
    const content =
      "<current_uploads>\n- paper.docx (177.6 KB)\n  Path: /mnt/user-data/uploads/paper.docx\n</current_uploads>\n\nExport me";

    expect(stripInternalMarkers(content)).toBe("Export me");
  });

  test("strips slash skill activation context from display content", () => {
    const content =
      "<slash_skill_activation>\n<skill_content># Secret SKILL.md</skill_content>\n</slash_skill_activation>\nreal user task";

    expect(stripUploadedFilesTag(content)).toBe("real user task");
  });

  test("hides leaked slash skill activation messages with no user text", () => {
    const messages = [
      {
        id: "slash-activation",
        type: "human",
        content:
          "<slash_skill_activation>\n<skill_content># Secret SKILL.md</skill_content>\n</slash_skill_activation>",
      },
      {
        id: "ai-1",
        type: "ai",
        content: "Public answer",
      },
    ] as Message[];

    const groups = getMessageGroups(messages);

    expect(groups.map((group) => group.type)).toEqual(["assistant"]);
    expect(
      groups.flatMap((group) => group.messages).map((message) => message.id),
    ).toEqual(["ai-1"]);
  });
});

test("hides internal todo reminder messages from message groups", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Audit the middleware",
    },
    {
      id: "todo-reminder-1",
      type: "human",
      name: "todo_completion_reminder",
      content: "<system_reminder>finish todos</system_reminder>",
    },
    {
      id: "todo-reminder-2",
      type: "human",
      name: "todo_reminder",
      content: "<system_reminder>remember todos</system_reminder>",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Done",
    },
  ] as Message[];

  const groups = getMessageGroups(messages);

  expect(groups.map((group) => group.type)).toEqual(["human", "assistant"]);
  expect(
    groups.flatMap((group) => group.messages).map((message) => message.id),
  ).toEqual(["human-1", "ai-1"]);
});

test("hides assistant copy data while that turn is streaming", () => {
  const messages = [
    {
      id: "ai-1",
      type: "ai",
      content: "Partial answer",
    },
  ] as Message[];

  expect(getAssistantTurnCopyData(messages)).toBe("Partial answer");
  expect(getAssistantTurnCopyData(messages, { isStreaming: true })).toBeNull();
});

test("falls back to reasoning for a reasoning-only assistant turn's copy data", () => {
  // A turn can end with reasoning but no answer text (e.g. stopped during
  // thinking). getMessageCopyData already copies the reasoning in that case;
  // the turn-level copy button must not disappear instead.
  const messages = [
    {
      id: "ai-1",
      type: "ai",
      content: "",
      additional_kwargs: { reasoning_content: "the actual reasoning" },
    },
  ] as Message[];

  expect(getAssistantTurnCopyData(messages)).toBe("the actual reasoning");
});

test("settled copy data is derived once per messages array reference (#5094)", () => {
  // Settled group arrays keep their identity across streaming chunks, and the
  // copy button re-renders per chunk. Reading `content` through a getter
  // proves the second settled call is served from the array-reference cache
  // instead of re-running the O(turn bytes) extraction.
  let contentReads = 0;
  const message = {
    id: "ai-1",
    type: "ai",
    get content() {
      contentReads += 1;
      return "Final answer";
    },
  } as unknown as Message;
  const messages = [message];

  expect(getAssistantTurnCopyData(messages)).toBe("Final answer");
  const readsAfterFirstCall = contentReads;
  expect(readsAfterFirstCall).toBeGreaterThan(0);

  expect(getAssistantTurnCopyData(messages)).toBe("Final answer");
  expect(contentReads).toBe(readsAfterFirstCall);
});

test("copy-data cache does not leak across array references", () => {
  const first = [
    { id: "ai-1", type: "ai", content: "first answer" },
  ] as Message[];
  const second = [
    { id: "ai-2", type: "ai", content: "second answer" },
  ] as Message[];

  expect(getAssistantTurnCopyData(first)).toBe("first answer");
  expect(getAssistantTurnCopyData(second)).toBe("second answer");
  // The streaming short-circuit stays ahead of the cache.
  expect(getAssistantTurnCopyData(second, { isStreaming: true })).toBeNull();
  expect(getAssistantTurnCopyData(second)).toBe("second answer");
});

test("null copy data is not cached for a reference", () => {
  // A turn with no copyable AI text must keep recomputing (and stay null)
  // rather than a cached null hiding a later value — the same array can be
  // re-used once messages are appended to a rebuilt group.
  const messages = [
    { id: "human-1", type: "human", content: "hi" },
  ] as Message[];

  expect(getAssistantTurnCopyData(messages)).toBeNull();
  expect(getAssistantTurnCopyData(messages)).toBeNull();
});

test("marks the latest assistant message as streaming", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Hello",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Still generating",
    },
  ] as Message[];
  const groups = getMessageGroups(messages);
  const assistantGroupIndex = groups.findIndex(
    (group) => group.type === "assistant",
  );

  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndex]?.messages ?? [],
      getStreamingMessageLookup(messages, true, () => ({
        streamMetadata: { langgraph_node: "agent" },
      })),
    ),
  ).toBe(true);
  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndex]?.messages ?? [],
      getStreamingMessageLookup(messages, false, () => ({
        streamMetadata: { langgraph_node: "agent" },
      })),
    ),
  ).toBe(false);
});

test("compares stream metadata snapshots by keys and metadata identity", () => {
  const identifiedMessage = {
    id: "ai-1",
    type: "ai",
    content: "Completed answer",
  } as Message;
  const anonymousMessage = {
    type: "ai",
    content: "Anonymous answer",
  } as Message;
  const identifiedMetadata = { langgraph_node: "agent" };
  const anonymousMetadata = { langgraph_node: "agent" };
  const messages = [identifiedMessage, anonymousMessage];
  const snapshot = getStreamMetadataSnapshot(messages, (message) => ({
    streamMetadata:
      message === identifiedMessage ? identifiedMetadata : anonymousMetadata,
  }));
  const equivalentSnapshot = getStreamMetadataSnapshot(messages, (message) => ({
    streamMetadata:
      message === identifiedMessage ? identifiedMetadata : anonymousMetadata,
  }));
  const changedSnapshot = getStreamMetadataSnapshot(messages, (message) => ({
    streamMetadata:
      message === identifiedMessage
        ? { ...identifiedMetadata }
        : anonymousMetadata,
  }));
  const missingSnapshot = getStreamMetadataSnapshot(
    [identifiedMessage],
    () => ({ streamMetadata: identifiedMetadata }),
  );

  expect(areStreamMetadataSnapshotsEqual(snapshot, equivalentSnapshot)).toBe(
    true,
  );
  expect(areStreamMetadataSnapshotsEqual(snapshot, changedSnapshot)).toBe(
    false,
  );
  expect(areStreamMetadataSnapshotsEqual(snapshot, missingSnapshot)).toBe(
    false,
  );
});

test("ignores stream metadata retained from a completed turn", () => {
  const completedMetadata = { langgraph_node: "agent", langgraph_step: 1 };
  const activeMetadata = { langgraph_node: "agent", langgraph_step: 2 };
  const completedMessages = [
    {
      id: "human-1",
      type: "human",
      content: "Hello",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Completed answer",
    },
  ] as Message[];
  const settledMetadata = getStreamMetadataSnapshot(
    completedMessages,
    (message) =>
      message.id === "ai-1" ? { streamMetadata: completedMetadata } : undefined,
  );
  const messages = [
    ...completedMessages,
    {
      id: "human-2",
      type: "human",
      content: "Continue",
    },
    {
      id: "ai-2",
      type: "ai",
      content: "Still generating",
    },
  ] as Message[];
  const groups = getMessageGroups(messages).filter(
    (group) => group.type === "assistant",
  );
  const streamingMessages = getStreamingMessageLookup(
    messages,
    true,
    (message) => {
      if (message.id === "ai-1") {
        return { streamMetadata: completedMetadata };
      }
      if (message.id === "ai-2") {
        return { streamMetadata: activeMetadata };
      }
      return undefined;
    },
    settledMetadata,
  );

  expect(
    isAssistantMessageGroupStreaming(
      groups[0]?.messages ?? [],
      streamingMessages,
    ),
  ).toBe(false);
  expect(
    isAssistantMessageGroupStreaming(
      groups[1]?.messages ?? [],
      streamingMessages,
    ),
  ).toBe(true);
});

test("treats updated metadata for the same message id as active", () => {
  const message = {
    id: "ai-1",
    type: "ai",
    content: "Partial answer",
  } as Message;
  const completedMetadata = { langgraph_node: "agent", langgraph_step: 1 };
  const activeMetadata = { langgraph_node: "agent", langgraph_step: 2 };
  const settledMetadata = getStreamMetadataSnapshot([message], () => ({
    streamMetadata: completedMetadata,
  }));

  expect(
    isAssistantMessageGroupStreaming(
      [message],
      getStreamingMessageLookup(
        [message],
        true,
        () => ({ streamMetadata: activeMetadata }),
        settledMetadata,
      ),
    ),
  ).toBe(true);
});

test("keeps previous assistant copyable while waiting for a new visible answer", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Hello",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Completed answer",
    },
    {
      id: "opt-human-1",
      type: "human",
      content: "Continue",
    },
  ] as Message[];
  const groups = getMessageGroups(messages);
  const assistantGroupIndex = groups.findIndex(
    (group) => group.type === "assistant",
  );

  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndex]?.messages ?? [],
      getStreamingMessageLookup(messages, true),
    ),
  ).toBe(false);
});

test("keeps previous assistant copyable while a hidden send is starting", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Hello",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Completed answer",
    },
  ] as Message[];
  const groups = getMessageGroups(messages);
  const assistantGroupIndex = groups.findIndex(
    (group) => group.type === "assistant",
  );

  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndex]?.messages ?? [],
      getStreamingMessageLookup(messages, true),
    ),
  ).toBe(false);
});

test("keeps previous assistant copyable after a hidden send is appended", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Hello",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Completed answer",
    },
    {
      id: "human-hidden",
      type: "human",
      content: "Save this agent",
      additional_kwargs: { hide_from_ui: true },
    },
  ] as Message[];
  const groups = getMessageGroups(messages);
  const assistantGroupIndex = groups.findIndex(
    (group) => group.type === "assistant",
  );

  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndex]?.messages ?? [],
      getStreamingMessageLookup(messages, true),
    ),
  ).toBe(false);
});

test("uses stream metadata to identify an assistant before optimistic input", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Hello",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Completed answer",
    },
    {
      id: "ai-2",
      type: "ai",
      content: "Still generating",
    },
    {
      id: "opt-human-1",
      type: "human",
      content: "Continue",
    },
  ] as Message[];
  const assistantGroups = getMessageGroups(messages).filter(
    (group) => group.type === "assistant",
  );
  const groups = getMessageGroups(messages);
  const assistantGroupIndexes = groups
    .map((group, index) => (group.type === "assistant" ? index : -1))
    .filter((index) => index >= 0);

  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndexes[0] ?? -1]?.messages ?? [],
      getStreamingMessageLookup(messages, true, (message) =>
        message.id === "ai-2"
          ? { streamMetadata: { langgraph_node: "agent" } }
          : undefined,
      ),
    ),
  ).toBe(false);
  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndexes[1] ?? -1]?.messages ?? [],
      getStreamingMessageLookup(messages, true, (message) =>
        message.id === "ai-2"
          ? { streamMetadata: { langgraph_node: "agent" } }
          : undefined,
      ),
    ),
  ).toBe(true);
  expect(assistantGroups.map((group) => group.id)).toEqual(["ai-1", "ai-2"]);
});

test("does not mark a completed assistant group streaming from a later processing group", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Hello",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Visible answer",
    },
    {
      id: "ai-2",
      type: "ai",
      content: "",
      tool_calls: [{ id: "tool-1", name: "web_search", args: {} }],
    },
  ] as Message[];
  const groups = getMessageGroups(messages);
  const assistantGroupIndex = groups.findIndex(
    (group) => group.type === "assistant",
  );

  expect(groups.map((group) => group.type)).toEqual([
    "human",
    "assistant",
    "assistant:processing",
  ]);
  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndex]?.messages ?? [],
      getStreamingMessageLookup(messages, true, (message) =>
        message.id === "ai-2"
          ? { streamMetadata: { langgraph_node: "agent" } }
          : undefined,
      ),
    ),
  ).toBe(false);
});

test("keeps streaming assistant hidden when a hidden control message follows it", () => {
  const messages = [
    {
      id: "human-1",
      type: "human",
      content: "Hello",
    },
    {
      id: "ai-1",
      type: "ai",
      content: "Still generating",
    },
    {
      id: "human-hidden",
      type: "human",
      content: "Save this agent",
      additional_kwargs: { hide_from_ui: true },
    },
  ] as Message[];
  const groups = getMessageGroups(messages);
  const assistantGroupIndex = groups.findIndex(
    (group) => group.type === "assistant",
  );

  expect(
    isAssistantMessageGroupStreaming(
      groups[assistantGroupIndex]?.messages ?? [],
      getStreamingMessageLookup(messages, true, (message) =>
        message.id === "ai-1"
          ? { streamMetadata: { langgraph_node: "agent" } }
          : undefined,
      ),
    ),
  ).toBe(true);
});

describe("multi-part content with bare-string continuations", () => {
  // Gemini streams the first content block as a {type:"text"} object carrying
  // the thinking signature, then emits continuation deltas as plain strings.
  // LangChain's Python merge_content preserves these as bare-string elements,
  // so the finalized message content is [{type:"text", ...}, "...rest..."].
  const geminiMessage = {
    id: "ai-1",
    type: "ai",
    content: [
      {
        type: "text",
        text: "First block carrying the signature.",
        extras: { signature: "abc123" },
        index: 0,
      },
      "Continuation streamed as a bare string.",
    ],
  } as unknown as Message;

  test("extractContentFromMessage includes the bare-string parts", () => {
    expect(extractContentFromMessage(geminiMessage)).toBe(
      "First block carrying the signature.\nContinuation streamed as a bare string.",
    );
  });

  test("extractTextFromMessage includes the bare-string parts", () => {
    expect(extractTextFromMessage(geminiMessage)).toBe(
      "First block carrying the signature.\nContinuation streamed as a bare string.",
    );
  });
});

describe("orphan tool messages", () => {
  // LangGraph stream-mode "messages-tuple" can emit tool-result events out of order or
  // replayed from subagent state (e.g. bash subagent under LocalSandboxProvider with
  // allow_host_bash). When that happens, the tool message arrives after a terminal
  // assistant/human group, so getMessageGroups' lastOpenGroup() returns null.
  //
  // The previous behaviour was console.error + drop, which silently hid the tool
  // result from the UI. The fix falls back to attaching the orphan tool to the most
  // recent group so the user can still see what the agent did.

  test("attaches orphan tool message to the most recent group instead of dropping it", () => {
    const messages = [
      { id: "h-1", type: "human", content: "Run something" },
      {
        id: "ai-1",
        type: "ai",
        content: "ok",
        tool_calls: [{ id: "call-1", name: "bash", args: {} }],
      },
      {
        id: "t-1",
        type: "tool",
        name: "bash",
        tool_call_id: "call-1",
        content: "output-1",
      },
      { id: "ai-2", type: "ai", content: "Done." }, // terminal assistant group
      // Orphan tool: arrives after a terminal group, no preceding processing group
      {
        id: "t-2",
        type: "tool",
        name: "bash",
        tool_call_id: "call-2",
        content: "output-2",
      },
    ] as Message[];

    const groups = getMessageGroups(messages);

    // Expect groups: human, assistant:processing (ai-1 + t-1), assistant (ai-2), and
    // t-2 should be attached to the last group (assistant), not dropped.
    const types = groups.map((g) => g.type);
    expect(types).toEqual(["human", "assistant:processing", "assistant"]);

    // t-2 must be retrievable from one of the groups — must NOT be silently dropped
    const allMessages = groups.flatMap((g) => g.messages);
    const t2 = allMessages.find((m) => m.id === "t-2");
    expect(t2).toBeDefined();
    expect(t2?.type).toBe("tool");
  });

  test("opens a processing group for a leading orphan tool message", () => {
    // History pagination cuts by event seq, not turn boundaries, so the first
    // loaded page can begin mid-turn with tool results whose AI tool-call
    // message sits on an unloaded older page (#4399). They must stay visible
    // in a processing group, not be dropped with a console error per render.
    const messages = [
      {
        id: "t-lead-1",
        type: "tool",
        name: "bash",
        tool_call_id: "call-old-1",
        content: "output from unloaded turn",
      },
      {
        id: "t-lead-2",
        type: "tool",
        name: "bash",
        tool_call_id: "call-old-2",
        content: "second output",
      },
      { id: "ai-1", type: "ai", content: "Done." },
    ] as Message[];

    const groups = getMessageGroups(messages);

    expect(groups.map((g) => g.type)).toEqual([
      "assistant:processing",
      "assistant",
    ]);
    // Both leading orphans cluster into the same processing group.
    expect(groups[0]?.messages.map((m) => m.id)).toEqual([
      "t-lead-1",
      "t-lead-2",
    ]);
  });

  test("leading orphan absorbs the following real AI turn into one processing group", () => {
    // A leading orphan followed by a real tool-call turn must accumulate into
    // a single processing group via the assistant:processing branch, not
    // strand the orphan as a separate empty group beside the real turn.
    const messages = [
      {
        id: "t-lead",
        type: "tool",
        name: "bash",
        tool_call_id: "call-old",
        content: "output from unloaded turn",
      },
      {
        id: "ai-1",
        type: "ai",
        content: "running",
        tool_calls: [{ id: "call-1", name: "bash", args: {} }],
      },
      {
        id: "t-1",
        type: "tool",
        name: "bash",
        tool_call_id: "call-1",
        content: "output-1",
      },
    ] as Message[];

    const groups = getMessageGroups(messages);

    // One processing group holds the orphan, the AI turn, and its tool result.
    expect(groups.map((g) => g.type)).toEqual(["assistant:processing"]);
    expect(groups[0]?.messages.map((m) => m.id)).toEqual([
      "t-lead",
      "ai-1",
      "t-1",
    ]);
  });

  test("tool message preceded only by hidden messages gets a group", () => {
    // messages is non-empty, but every earlier message is hidden from the UI —
    // groups is still empty when the tool message arrives.
    const messages = [
      {
        id: "hidden-1",
        type: "human",
        content:
          "<slash_skill_activation>\n<skill_content># SKILL.md</skill_content>\n</slash_skill_activation>",
      },
      {
        id: "t-1",
        type: "tool",
        name: "bash",
        tool_call_id: "call-1",
        content: "output",
      },
    ] as Message[];

    const groups = getMessageGroups(messages);

    expect(groups.map((g) => g.type)).toEqual(["assistant:processing"]);
    expect(groups[0]?.messages.map((m) => m.id)).toEqual(["t-1"]);
  });

  test("replayed tool with same tool_call_id is not lost (duplicate stream events)", () => {
    // LangGraph subagent state restoration can replay tool-result events. The
    // frontend log shows the same tool_call_id arriving twice. Both occurrences
    // should be visible in the UI, not just the first.
    const messages = [
      { id: "h-1", type: "human", content: "q" },
      {
        id: "ai-1",
        type: "ai",
        content: "",
        tool_calls: [{ id: "call-x", name: "bash", args: {} }],
      },
      {
        id: "t-1a",
        type: "tool",
        name: "bash",
        tool_call_id: "call-x",
        content: "first delivery",
      },
      // Terminal assistant group ends the turn and closes the processing group.
      // Without this interleave the replayed t-1b would still take the
      // unchanged happy path; with it, t-1b arrives when lastOpenGroup()
      // returns null and must take the new fallback branch to be visible.
      { id: "ai-2", type: "ai", content: "Done." },
      // Replayed tool-result for the original tool_call — must reach the new
      // else-if (groups.length > 0) branch instead of being dropped.
      {
        id: "t-1b",
        type: "tool",
        name: "bash",
        tool_call_id: "call-x",
        content: "first delivery",
      },
    ] as Message[];

    const groups = getMessageGroups(messages);
    const allMessages = groups.flatMap((g) => g.messages);

    // Strict assertion: the replayed tool message must be reachable from a
    // group (i.e. attached via the new fallback). Before the fix this was
    // silently dropped by console.error.
    const t1b = allMessages.find((m) => m.id === "t-1b");
    expect(t1b).toBeDefined();
    expect(t1b?.type).toBe("tool");
  });
});

describe("a present_files result carries the tag its call already draws", () => {
  test("is not read as a second presentation", () => {
    const messages = [
      { id: "human-1", type: "human", content: "Make the report" },
      {
        id: "ai-1",
        type: "ai",
        content: "",
        tool_calls: [
          {
            id: "call-1",
            name: "present_files",
            args: { filepaths: ["/mnt/user-data/outputs/r.pdf"] },
          },
        ],
      },
      {
        id: "tool-1",
        type: "tool",
        name: "present_files",
        tool_call_id: "call-1",
        content: "Successfully presented files",
        additional_kwargs: {
          presented_files: ["/mnt/user-data/outputs/r.pdf"],
        },
      },
    ] as Message[];
    expect(hasPresentFiles(messages[2]!)).toBe(false);
    expect(extractPresentFilesFromMessage(messages[2]!)).toEqual([]);
    const presentGroups = getMessageGroups(messages).filter(
      (group) => group.type === "assistant:present-files",
    );
    expect(presentGroups).toHaveLength(1);
  });
});

describe("files a tool result presented on the run's behalf", () => {
  // A bash call that named files under `present` has its ToolMessage tagged
  // with `additional_kwargs.presented_files` once the backend validated them;
  // the client must draw those files exactly as it draws a `present_files`
  // call's.
  const messages = [
    { id: "human-1", type: "human", content: "Make the report" },
    {
      id: "ai-1",
      type: "ai",
      content: "",
      tool_calls: [
        {
          id: "call-1",
          name: "bash",
          args: { command: "python report.py build …" },
        },
      ],
    },
    {
      id: "tool-1",
      type: "tool",
      name: "bash",
      tool_call_id: "call-1",
      content: "Built draft 1\n\nPresented to the user: 2 files",
      additional_kwargs: {
        presented_files: [
          "/mnt/user-data/outputs/r/r.report.json",
          "/mnt/user-data/outputs/r/r.pdf",
        ],
      },
    },
    { id: "ai-2", type: "ai", content: "Done." },
  ] as Message[];

  test("the tool result keeps its step and also opens a present-files group", () => {
    const groups = getMessageGroups(messages);
    expect(groups.map((group) => group.type)).toEqual([
      "human",
      "assistant:processing",
      "assistant:present-files",
      "assistant",
    ]);
    expect(groups[1]?.messages.map((message) => message.id)).toEqual([
      "ai-1",
      "tool-1",
    ]);
    expect(groups[2]?.messages.map((message) => message.id)).toEqual([
      "tool-1",
    ]);
  });

  test("a second presentation in the same turn joins the first row rather than repeating it", () => {
    const call = (id: string, name: string, args: Record<string, unknown>) =>
      ({ id, name, args }) as never;
    const second = {
      id: "tool-2",
      type: "tool",
      name: "bash",
      tool_call_id: "call-2",
      content: "Draft 2\n\nPresented to the user: 1 file",
      additional_kwargs: {
        presented_files: [
          "/mnt/user-data/outputs/r/r.report.json",
          "/mnt/user-data/outputs/r/r.pdf",
        ],
      },
    } as Message;
    const secondCall = {
      id: "ai-1b",
      type: "ai",
      content: "",
      tool_calls: [
        call("call-2", "bash", { command: "python report.py prose …" }),
      ],
    } as Message;
    const groups = getMessageGroups([
      ...messages.slice(0, 3),
      secondCall,
      second,
      messages[3]!,
    ]);
    expect(groups.map((group) => group.type)).toEqual([
      "human",
      "assistant:processing",
      "assistant:present-files",
      "assistant:processing",
      "assistant",
    ]);
    expect(groups[2]?.messages.map((message) => message.id)).toEqual([
      "tool-1",
      "tool-2",
    ]);

    const nextTurn = getMessageGroups([
      ...messages,
      { id: "h-2", type: "human", content: "again" } as Message,
      secondCall,
      second,
    ]);
    expect(
      nextTurn.filter((group) => group.type === "assistant:present-files"),
    ).toHaveLength(2);
  });

  test("a parallel sibling tool result stays with its step, not under the chips", () => {
    const parallel = {
      id: "ai-1",
      type: "ai",
      content: "",
      tool_calls: [
        {
          id: "call-1",
          name: "bash",
          args: { command: "python report.py build …" },
        },
        { id: "call-x", name: "ls", args: { path: "/mnt/user-data/outputs" } },
      ],
    } as Message;
    const sibling = {
      id: "tool-x",
      type: "tool",
      name: "ls",
      tool_call_id: "call-x",
      content: "r.pdf",
    } as Message;
    const groups = getMessageGroups([
      messages[0]!,
      parallel,
      messages[2]!,
      sibling,
      messages[3]!,
    ]);
    expect(groups.map((group) => group.type)).toEqual([
      "human",
      "assistant:processing",
      "assistant:present-files",
      "assistant",
    ]);
    expect(groups[1]?.messages.map((message) => message.id)).toEqual([
      "ai-1",
      "tool-1",
      "tool-x",
    ]);
    expect(groups[2]?.messages.map((message) => message.id)).toEqual([
      "tool-1",
    ]);
  });

  test("the presented files are read from the tool result's tag, in order", () => {
    const tool = messages[2]!;
    expect(hasPresentFiles(tool)).toBe(true);
    expect(extractPresentFilesFromMessage(tool)).toEqual([
      "/mnt/user-data/outputs/r/r.report.json",
      "/mnt/user-data/outputs/r/r.pdf",
    ]);
  });

  test("a tool result without the tag presents nothing, whatever its text says", () => {
    const untagged = {
      id: "tool-2",
      type: "tool",
      name: "bash",
      tool_call_id: "call-2",
      content: "Wrote /mnt/user-data/outputs/r/r.pdf\n",
    } as Message;
    expect(hasPresentFiles(untagged)).toBe(false);
    expect(extractPresentFilesFromMessage(untagged)).toEqual([]);
    const wrongShape = {
      ...untagged,
      additional_kwargs: { presented_files: "not-a-list" },
    } as Message;
    expect(hasPresentFiles(wrongShape)).toBe(false);
  });
});

describe("an answer the runtime tagged because the turn presented nothing", () => {
  // hartmesh-tenancy/DF22: a turn wrote a PDF, named it under no `present`
  // argument and never called `present_files`, so the runtime handed it over
  // and tagged the turn's final answer. The tag is the presentation whichever
  // message carries it, so the client must draw those files — and must still
  // show the answer, which is the message the tag is on.
  const messages = [
    { id: "human-1", type: "human", content: "Create pdf about muse agent" },
    {
      id: "ai-1",
      type: "ai",
      content: "Here is the report on Meta's Muse agent.",
      additional_kwargs: {
        presented_files: ["/mnt/user-data/outputs/Muse_Agent_Report.pdf"],
        presented_by: "runtime",
      },
    },
  ] as Message[];

  test("is read as a presentation", () => {
    expect(hasPresentFiles(messages[1]!)).toBe(true);
    expect(extractPresentFilesFromMessage(messages[1]!)).toEqual([
      "/mnt/user-data/outputs/Muse_Agent_Report.pdf",
    ]);
  });

  test("draws one group carrying both the answer and the file", () => {
    const groups = getMessageGroups(messages);
    const presentGroups = groups.filter(
      (group) => group.type === "assistant:present-files",
    );
    expect(presentGroups).toHaveLength(1);
    const group = presentGroups[0]!;
    // The renderer draws the group's first message as prose above the chips,
    // so the answer is not lost by being the message that carries the tag.
    expect(group.messages[0]!.id).toBe("ai-1");
    expect(hasContent(group.messages[0]!)).toBe(true);
    expect(extractContentFromMessage(group.messages[0]!)).toContain(
      "Here is the report",
    );
  });

  test("an untagged answer is still an ordinary answer", () => {
    const untagged = [
      { id: "human-1", type: "human", content: "hello" },
      { id: "ai-1", type: "ai", content: "Hi there." },
    ] as Message[];
    expect(hasPresentFiles(untagged[1]!)).toBe(false);
    expect(
      getMessageGroups(untagged).filter(
        (group) => group.type === "assistant:present-files",
      ),
    ).toHaveLength(0);
  });

  test("a malformed tag presents nothing", () => {
    const malformed = [
      { id: "human-1", type: "human", content: "hello" },
      {
        id: "ai-1",
        type: "ai",
        content: "Hi.",
        additional_kwargs: { presented_files: "/mnt/user-data/outputs/x.pdf" },
      },
    ] as Message[];
    expect(hasPresentFiles(malformed[1]!)).toBe(false);
    expect(extractPresentFilesFromMessage(malformed[1]!)).toEqual([]);
  });

  test("a call and a tag on one message list each file once", () => {
    const both = {
      id: "ai-1",
      type: "ai",
      content: "",
      tool_calls: [
        {
          id: "call-1",
          name: "present_files",
          args: { filepaths: ["/mnt/user-data/outputs/a.pdf"] },
        },
      ],
      additional_kwargs: {
        presented_files: [
          "/mnt/user-data/outputs/a.pdf",
          "/mnt/user-data/outputs/b.pdf",
        ],
      },
    } as Message;
    expect(extractPresentFilesFromMessage(both)).toEqual([
      "/mnt/user-data/outputs/a.pdf",
      "/mnt/user-data/outputs/b.pdf",
    ]);
  });
});
