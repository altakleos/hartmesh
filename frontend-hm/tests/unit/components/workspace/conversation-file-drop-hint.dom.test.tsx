import { afterEach, expect, test } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

import { ConversationFileDropHint } from "@/components/workspace/chats/conversation-file-drop-hint";
import { I18nProvider } from "@/core/i18n/context";

afterEach(cleanup);
function hint(enabled = true) {
  return (
    <I18nProvider initialLocale="en-US">
      <ConversationFileDropHint name="Researcher" enabled={enabled} />
    </I18nProvider>
  );
}
function drag(type: string, files = true) {
  fireEvent(
    window,
    Object.defineProperty(new Event(type), "dataTransfer", {
      value: {
        types: files ? ["Files"] : ["text/plain"],
      },
    }),
  );
}
test("file drags show the named hint until drop, including nested enters and leaves", () => {
  render(hint());
  drag("dragenter");
  drag("dragenter");
  drag("dragleave");
  expect(screen.getByRole("status").textContent).toBe(
    "Drop files here to give them to Researcher",
  );
  drag("drop");
  expect(screen.queryByRole("status")).toBeNull();
});
test("text drags do not show a file hint and permissions retire a visible hint", () => {
  const view = render(hint());
  drag("dragenter", false);
  expect(screen.queryByRole("status")).toBeNull();
  drag("dragenter");
  expect(screen.getByRole("status")).toBeTruthy();
  view.rerender(hint(false));
  expect(screen.queryByRole("status")).toBeNull();
  drag("dragenter");
  expect(screen.queryByRole("status")).toBeNull();
});
