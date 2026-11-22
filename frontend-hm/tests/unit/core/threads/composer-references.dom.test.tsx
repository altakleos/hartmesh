import { expect, it, rs } from "@rstest/core";
import { act, renderHook } from "@testing-library/react";

import { useComposerReferences } from "@/core/threads/composer-references";
import type * as referenceModule from "@/core/threads/conversation-references";
const lookup = rs.hoisted(() =>
  rs.fn<
    (
      id: string,
      account: string,
      instance: string | undefined,
      signal: AbortSignal,
    ) => Promise<referenceModule.ConversationReference | null>
  >(),
);
rs.mock("@/core/threads/conversation-references", () => ({
  ...rs.requireActual<typeof referenceModule>(
    "@/core/threads/conversation-references",
  ),
  resolveConversationReference: lookup,
}));
const id = "source";
const link = () => window.location.origin + "/workspace/chats/" + id;
const found = { id, title: "Budget review" };
function fixture() {
  const view = renderHook(
    ({ user, limit }) =>
      useComposerReferences(user, "dest", undefined, "current", limit),
    { initialProps: { user: "a", limit: 1 } },
  );
  const input = document.createElement("textarea");
  function paste() {
    const start = input.selectionStart,
      end = input.selectionEnd;
    const completion = view.result.current.pasted(
      link(),
      input,
      () => input.value,
      (value) => {
        input.value = value;
      },
    );
    input.setRangeText(link(), start, end, "end");
    return completion;
  }
  return { view, input, paste };
}
it("preserves the rest of a selected draft, deduplicates, removes and keeps refused links as plain paste", async () => {
  lookup.mockResolvedValue(found);
  const { view, input, paste } = fixture();
  input.value = "Ask about this please";
  input.setSelectionRange(10, 14);
  await act(paste);
  expect(input.value).toBe("Ask about  please");
  expect(view.result.current.references).toEqual([found]);
  input.setSelectionRange(0, 0);
  await act(paste);
  expect(view.result.current.references).toEqual([found]);
  expect(input.value).toContain(link());
  act(() => view.result.current.remove(view.result.current.references));
  expect(view.result.current.references).toEqual([]);
  lookup.mockRejectedValueOnce(new Error("404"));
  input.value = "";
  await act(paste);
  expect(input.value).toBe(link());
  expect(view.result.current.references).toEqual([]);
  view.unmount();
});
it.each(["account", "flag", "send", "edit"] as const)(
  "a delayed lookup cannot grant after %s changes",
  async (change) => {
    let release!: (ref: referenceModule.ConversationReference) => void;
    lookup.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve;
        }),
    );
    const { view, input, paste } = fixture();
    const completion = paste();
    if (change === "account") view.rerender({ user: "b", limit: 1 });
    if (change === "flag") view.rerender({ user: "a", limit: 0 });
    if (change === "send") act(() => view.result.current.cancelLookups());
    if (change === "edit") input.value += "changed";
    await act(async () => {
      release(found);
      await completion;
    });
    expect(view.result.current.references).toEqual([]);
    expect(input.value).toBe(change === "edit" ? link() + "changed" : link());
    view.unmount();
  },
);
