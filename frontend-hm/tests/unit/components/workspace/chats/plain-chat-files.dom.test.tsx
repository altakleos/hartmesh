import { afterEach, describe, expect, it } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { useCallback, useRef, useState } from "react";

import { useComposerStorageRefusal } from "@/core/threads/composer-storage";
import type { ThreadRunRefusal } from "@/core/threads/hooks";

const file = new File(["small fixture"], "statement.txt");
const part = {
  id: "attached-1",
  type: "file" as const,
  url: "blob:fixture",
  mediaType: "text/plain",
  filename: file.name,
  file,
};
const refusal = (
  phase: "send" | "upload",
  id = "thread-1",
): ThreadRunRefusal => ({
  sequence: 1,
  threadId: id,
  phase,
  newThread: true,
  message: { text: "original draft", files: [part] },
  refusal: {
    status: 507,
    code: "files-full",
    reason: null,
    detailCode: null,
    message: "raw",
  },
});

function Composer({
  incoming = null,
  threadId = "thread-1",
}: {
  incoming?: ThreadRunRefusal | null;
  threadId?: string;
}) {
  const [text, setText] = useState("original draft");
  const latest = useRef(text);
  latest.current = text;
  const edited = useRef(0);
  const readText = useCallback(() => latest.current, []);
  const readEdit = useCallback(() => edited.current, []);
  const storage = useComposerStorageRefusal({
    refusal: incoming,
    threadId,
    getText: readText,
    getEditRevision: readEdit,
    setText,
  });
  return (
    <>
      <textarea
        aria-label="Message"
        value={text}
        onChange={(event) => {
          edited.current++;
          setText(event.target.value);
        }}
      />
      <button
        onClick={() => {
          storage.capture(
            { text, files: [part] },
            text,
            [part],
            edited.current,
          );
          setText("");
        }}
      >
        Submit
      </button>
      <output data-testid="eligible">
        {storage.eligibleFiles([part], [part]).length}
      </output>
      <output data-testid="refused">
        {storage.failures[part.id]?.code ?? ""}
      </output>
    </>
  );
}
afterEach(cleanup);

describe("ordinary composer storage refusals", () => {
  it("restores the refused text after normal submit cleanup", async () => {
    const view = render(<Composer />);
    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    expect(screen.getByRole<HTMLTextAreaElement>("textbox").value).toBe("");
    view.rerender(<Composer incoming={refusal("send")} />);
    await waitFor(() =>
      expect(screen.getByRole<HTMLTextAreaElement>("textbox").value).toBe(
        "original draft",
      ),
    );
  });

  it("preserves later edits including an intentionally emptied draft", async () => {
    const view = render(<Composer />);
    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    fireEvent.change(screen.getByRole("textbox"), {
      target: { value: "later draft" },
    });
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "" } });
    view.rerender(<Composer incoming={refusal("send")} />);
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
    });
    expect(screen.getByRole<HTMLTextAreaElement>("textbox").value).toBe("");
  });

  it("marks refused upload chips and excludes them from the next send without blocking text", async () => {
    const view = render(<Composer />);
    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    view.rerender(<Composer incoming={refusal("upload")} />);
    await waitFor(() =>
      expect(screen.getByTestId("refused").textContent).toBe("files-full"),
    );
    expect(screen.getByTestId("eligible").textContent).toBe("0");
    expect(
      screen.getByRole<HTMLButtonElement>("button", { name: "Submit" })
        .disabled,
    ).toBe(false);
    await waitFor(() =>
      expect(screen.getByRole<HTMLTextAreaElement>("textbox").value).toBe(
        "original draft",
      ),
    );
  });

  it("ignores another conversation's refusal", async () => {
    const view = render(<Composer />);
    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    view.rerender(<Composer incoming={refusal("upload", "old-thread")} />);
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
    });
    expect(screen.getByRole<HTMLTextAreaElement>("textbox").value).toBe("");
    expect(screen.getByTestId("eligible").textContent).toBe("1");
  });
});
