import { afterEach, beforeEach, expect, rs, test } from "@rstest/core";
import { cleanup, renderHook } from "@testing-library/react";

/**
 * The prewarm hook asks exactly once per new thread id -- a re-render, a
 * second page on the same thread, and an existing thread all ask nothing.
 */
const api = rs.hoisted(() => ({
  prewarmThreadWorkspace: rs.fn(async () => undefined),
}));

rs.mock("@/core/threads/api", () => ({
  prewarmThreadWorkspace: api.prewarmThreadWorkspace,
}));

import { usePrewarmWorkspace } from "@/components/workspace/chats/use-prewarm-workspace";

beforeEach(() => {
  api.prewarmThreadWorkspace.mockClear();
});

afterEach(() => {
  cleanup();
});

test("asks once for a new thread, and not again on re-render", () => {
  const { rerender } = renderHook(
    ({ threadId, enabled }) => usePrewarmWorkspace({ threadId, enabled }),
    { initialProps: { threadId: "thread-a", enabled: true } },
  );

  rerender({ threadId: "thread-a", enabled: true });
  rerender({ threadId: "thread-a", enabled: true });

  expect(api.prewarmThreadWorkspace).toHaveBeenCalledTimes(1);
  expect(api.prewarmThreadWorkspace).toHaveBeenCalledWith("thread-a");
});

test("asks again when the new thread id changes", () => {
  const { rerender } = renderHook(
    ({ threadId, enabled }) => usePrewarmWorkspace({ threadId, enabled }),
    { initialProps: { threadId: "thread-a", enabled: true } },
  );

  rerender({ threadId: "thread-b", enabled: true });

  expect(api.prewarmThreadWorkspace.mock.calls).toEqual([
    ["thread-a"],
    ["thread-b"],
  ]);
});

test("asks nothing for an existing thread or a mock page", () => {
  renderHook(() =>
    usePrewarmWorkspace({ threadId: "thread-old", enabled: false }),
  );
  renderHook(() => usePrewarmWorkspace({ threadId: undefined, enabled: true }));

  expect(api.prewarmThreadWorkspace).not.toHaveBeenCalled();
});

test("asks once a thread becomes new, not before", () => {
  const { rerender } = renderHook(
    ({ threadId, enabled }) => usePrewarmWorkspace({ threadId, enabled }),
    { initialProps: { threadId: "thread-a", enabled: false } },
  );
  expect(api.prewarmThreadWorkspace).not.toHaveBeenCalled();

  rerender({ threadId: "thread-a", enabled: true });

  expect(api.prewarmThreadWorkspace).toHaveBeenCalledTimes(1);
});
