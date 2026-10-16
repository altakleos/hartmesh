import { afterEach, expect, test, rs } from "@rstest/core";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";

import { useComposerAgentRefusal } from "@/core/threads/composer-agent";
import type { AgentThreadRunRefusal } from "@/core/threads/hooks";

afterEach(cleanup);
const saved = {
  filename: "statement.txt",
  path: "/mnt/user-data/uploads/statement.txt",
  size: 4,
  status: "uploaded" as const,
};
function setup() {
  const draft = { text: "Original draft", revision: 0 };
  const setText = rs.fn((text: string) => {
    draft.text = text;
  });
  const restoreFiles = rs.fn();
  const options = {
    threadId: "thread",
    getText: () => draft.text,
    getEditRevision: () => draft.revision,
    setText,
    restoreFiles,
  };
  const view = renderHook(
    ({
      threadId,
      refusal,
    }: {
      threadId: string;
      refusal: AgentThreadRunRefusal | null;
    }) => useComposerAgentRefusal({ ...options, threadId, refusal }),
    {
      initialProps: {
        threadId: "thread",
        refusal: null as AgentThreadRunRefusal | null,
      },
    },
  );
  act(() =>
    view.result.current.capture(
      "/review Original draft",
      draft.text,
      draft.revision,
    ),
  );
  const refusal: AgentThreadRunRefusal = {
    threadId: "thread",
    sequence: 1,
    refusal: { reason: "run-limit", extra: {} },
    restore: { text: "/review Original draft", files: [saved] },
  };
  return { view, draft, setText, restoreFiles, refusal };
}
test("restores the submitted editable draft and saved references after admitted-input cleanup without sending", async () => {
  const { view, draft, setText, restoreFiles, refusal } = setup();
  draft.text = "";
  view.rerender({ threadId: "thread", refusal });
  await waitFor(() => expect(setText).toHaveBeenCalledWith("Original draft"));
  expect(restoreFiles).toHaveBeenCalledWith([saved]);
  view.rerender({ threadId: "thread", refusal });
  expect(setText).toHaveBeenCalledTimes(1);
});
test("keeps subsequent human edits and attachment choices", async () => {
  const { view, draft, setText, restoreFiles, refusal } = setup();
  draft.text = "Newer draft";
  draft.revision++;
  view.rerender({ threadId: "thread", refusal });
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(setText).not.toHaveBeenCalled();
  expect(restoreFiles).not.toHaveBeenCalled();
});
test("a refusal without restoration metadata never resurrects partial-output input", async () => {
  const { view, setText, restoreFiles, refusal } = setup();
  view.rerender({
    threadId: "thread",
    refusal: { ...refusal, restore: undefined },
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(setText).not.toHaveBeenCalled();
  expect(restoreFiles).not.toHaveBeenCalled();
});
test("a scheduled restoration retires with the conversation", async () => {
  const { view, setText, restoreFiles, refusal } = setup();
  view.rerender({ threadId: "thread", refusal });
  view.rerender({ threadId: "another", refusal: null });
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(setText).not.toHaveBeenCalled();
  expect(restoreFiles).not.toHaveBeenCalled();
});
