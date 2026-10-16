import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  parts: [] as { id: string; file: File; filename: string }[],
  remove: rs.fn(),
}));
rs.mock("@/components/ai-elements/prompt-input", () => ({
  usePromptInputController: () => ({
    attachments: { files: state.parts, remove: state.remove },
  }),
}));
rs.mock("@/core/agent-instances/agent-files", () => ({
  uploadConversationFile: rs.fn(),
  putAgentFile: rs.fn(),
}));

import * as api from "@/core/agent-instances/agent-files";
import { InstanceApiError } from "@/core/agent-instances/api";
import { useConversationAttachments } from "@/core/agent-instances/conversation-attachments";
import { MAX_AGENT_FILE_BYTES } from "@/core/agent-instances/file-transfers";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";

const id = "a".repeat(32);
const saved = {
  path: "uploads/input (2).txt",
  sandbox_path: "/mnt/user-data/uploads/input (2).txt",
  size: 5,
  sha256: "b".repeat(64),
  deduplicated: false,
};
const changed = rs.fn();
function part(name = "input.txt") {
  const file = new File(["hello"], name);
  return { id: name, file, filename: name };
}
function show() {
  return renderHook(
    (props) => useConversationAttachments({ ...props, changed }),
    {
      initialProps: { instanceId: id, threadId: "thread", enabled: true },
      wrapper: ({ children }) => (
        <FileActionLifetimeProvider>{children}</FileActionLifetimeProvider>
      ),
    },
  );
}
beforeEach(() => {
  state.parts = [part()];
  state.remove.mockReset();
  changed.mockReset();
  rs.mocked(api.uploadConversationFile).mockReset().mockResolvedValue(saved);
  rs.mocked(api.putAgentFile).mockReset();
});
afterEach(cleanup);
test("uploads on attachment once, then sends only the returned saved name and sandbox reference", async () => {
  const view = show();
  await waitFor(() => expect(view.result.current.saved).toHaveLength(1));
  expect(api.uploadConversationFile).toHaveBeenCalledTimes(1);
  expect(
    rs.mocked(api.uploadConversationFile).mock.calls[0]?.slice(0, 3),
  ).toEqual([id, "thread", state.parts[0]!.file]);
  const capture = view.result.current.capture();
  expect(capture.files).toEqual([
    {
      filename: "input (2).txt",
      path: saved.sandbox_path,
      size: 5,
      status: "uploaded",
    },
  ]);
  view.rerender({ instanceId: id, threadId: "thread", enabled: true });
  expect(api.uploadConversationFile).toHaveBeenCalledTimes(1);
  expect(api.putAgentFile).not.toHaveBeenCalled();
});
test("a refused chip does not block text and a manual retry keeps its exact operation and file", async () => {
  rs.mocked(api.uploadConversationFile).mockRejectedValueOnce(
    new InstanceApiError(503, "private", "file-write-failed"),
  );
  const view = show();
  await waitFor(() =>
    expect(view.result.current.rows[0]?.status).toBe("failed"),
  );
  expect(view.result.current.uploading).toBe(false);
  expect(view.result.current.capture().files).toEqual([]);
  const first = rs.mocked(api.uploadConversationFile).mock.calls[0]!;
  act(() => view.result.current.retry(view.result.current.rows[0]!.id));
  await waitFor(() => expect(view.result.current.saved).toHaveLength(1));
  const second = rs.mocked(api.uploadConversationFile).mock.calls[1]!;
  expect(second.slice(0, 4)).toEqual(first.slice(0, 4));
});
test("removing an uploading chip cancels its request and ignores late completion", async () => {
  let finish!: (value: typeof saved) => void;
  rs.mocked(api.uploadConversationFile).mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = show();
  await waitFor(() => expect(view.result.current.uploading).toBe(true));
  const signal = rs.mocked(api.uploadConversationFile).mock.calls[0]![4]!
    .signal!;
  act(() => view.result.current.remove(view.result.current.rows[0]!.id));
  expect(signal.aborted).toBe(true);
  expect(state.remove).toHaveBeenCalledWith("input.txt");
  await act(async () => {
    finish(saved);
  });
  expect(view.result.current.saved).toEqual([]);
  expect(api.uploadConversationFile).toHaveBeenCalledTimes(1);
});
test("saved-chip removal and restoration never perform a server delete or a new upload", async () => {
  const view = show();
  await waitFor(() => expect(view.result.current.saved).toHaveLength(1));
  state.parts = [];
  view.rerender({ instanceId: id, threadId: "thread", enabled: true });
  await waitFor(() => expect(view.result.current.rows).toHaveLength(0));
  act(() =>
    view.result.current.restoreFiles([
      {
        path: saved.sandbox_path,
        filename: "input (2).txt",
        size: 5,
        status: "uploaded",
      },
    ]),
  );
  expect(view.result.current.restored).toHaveLength(1);
  const captured = view.result.current.capture();
  act(() =>
    view.result.current.restoreFiles([
      {
        path: "/mnt/user-data/uploads/later.txt",
        filename: "later.txt",
        size: 2,
        status: "uploaded",
      },
    ]),
  );
  act(() => captured.admitted());
  expect(view.result.current.restored.map((file) => file.filename)).toEqual([
    "later.txt",
  ]);
  act(() =>
    view.result.current.removeRestored("/mnt/user-data/uploads/later.txt"),
  );
  expect(view.result.current.saved).toEqual([]);
  expect(api.uploadConversationFile).toHaveBeenCalledTimes(1);
});
test("one Remove after a refused send removes the surviving saved chip and its restored reference", async () => {
  const view = show();
  await waitFor(() => expect(view.result.current.saved).toHaveLength(1));
  act(() =>
    view.result.current.restoreFiles(view.result.current.capture().files),
  );
  expect(view.result.current.restored).toHaveLength(0);
  state.remove.mockImplementationOnce((id: string) => {
    state.parts = state.parts.filter((part) => part.id !== id);
  });
  act(() => view.result.current.remove(view.result.current.rows[0]!.id));
  view.rerender({ instanceId: id, threadId: "thread", enabled: true });
  expect(view.result.current.rows).toHaveLength(0);
  expect(view.result.current.restored).toHaveLength(0);
  expect(view.result.current.capture().files).toEqual([]);
  expect(api.uploadConversationFile).toHaveBeenCalledTimes(1);
});
test("conversation retirement clears visible saved references and fences a pending old upload", async () => {
  let finish!: (value: typeof saved) => void;
  rs.mocked(api.uploadConversationFile).mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = show();
  await waitFor(() =>
    expect(api.uploadConversationFile).toHaveBeenCalledTimes(1),
  );
  const signal = rs.mocked(api.uploadConversationFile).mock.calls[0]![4]!
    .signal!;
  state.parts = [];
  view.rerender({ instanceId: id, threadId: "other", enabled: true });
  await act(async () => {
    finish(saved);
  });
  expect(signal.aborted).toBe(true);
  expect(view.result.current.saved).toEqual([]);
  expect(view.result.current.rows).toEqual([]);
  expect(changed).not.toHaveBeenCalled();
});
test("an oversized attachment stays as a refused chip without allocating or uploading its reported size", async () => {
  Object.defineProperty(state.parts[0]!.file, "size", {
    value: MAX_AGENT_FILE_BYTES + 1,
  });
  const view = show();
  await waitFor(() =>
    expect(view.result.current.rows[0]?.status).toBe("failed"),
  );
  expect(view.result.current.rows[0]?.error).toMatchObject({
    reason: "file-too-large",
    extra: { max_bytes: MAX_AGENT_FILE_BYTES },
  });
  expect(view.result.current.uploading).toBe(false);
  expect(api.uploadConversationFile).not.toHaveBeenCalled();
});
test("permission retirement aborts a transfer and regranting does not automatically repeat a write", async () => {
  rs.mocked(api.uploadConversationFile).mockImplementation(
    () =>
      new Promise(() => {
        /* Remains pending until its owner aborts. */
      }),
  );
  const view = show();
  await waitFor(() => expect(view.result.current.uploading).toBe(true));
  const signal = rs.mocked(api.uploadConversationFile).mock.calls[0]![4]!
    .signal!;
  view.rerender({ instanceId: id, threadId: "thread", enabled: false });
  await waitFor(() => expect(signal.aborted).toBe(true));
  view.rerender({ instanceId: id, threadId: "thread", enabled: true });
  expect(api.uploadConversationFile).toHaveBeenCalledTimes(1);
  expect(view.result.current.uploading).toBe(false);
});
