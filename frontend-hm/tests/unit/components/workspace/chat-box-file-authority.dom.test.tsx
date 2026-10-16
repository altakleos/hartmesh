import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  open: true,
  selected: "/mnt/user-data/outputs/report.md" as string | null,
  close: rs.fn(),
  deselect: rs.fn(),
  mount: rs.fn(),
  panel: {
    current: {
      getSize: () => ({ asPercentage: 40 }),
      resize: rs.fn(),
      collapse: rs.fn(),
    },
  },
}));
rs.mock("next/navigation", () => ({
  usePathname: () => "/workspace/chats/thread",
}));
rs.mock("next/dynamic", () => ({
  default: () => () => {
    state.mount();
    return <div data-testid="file-panel">File contents</div>;
  },
}));
rs.mock("@/env", () => ({ env: { NEXT_PUBLIC_STATIC_WEBSITE_ONLY: "false" } }));
rs.mock("@/hooks/use-mobile", () => ({ useIsMobile: () => false }));
rs.mock("react-resizable-panels", () => ({ usePanelRef: () => state.panel }));
rs.mock("@/components/ui/resizable", () => ({
  ResizablePanelGroup: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  ResizablePanel: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  ResizableHandle: () => null,
}));
rs.mock("@/components/workspace/artifacts/context", () => ({
  useArtifacts: () => ({
    artifacts: ["/mnt/user-data/outputs/report.md"],
    open: state.open,
    selectedArtifact: state.selected,
    setOpen: state.close,
    deselect: state.deselect,
    setArtifacts: rs.fn(),
    select: rs.fn(),
  }),
}));
rs.mock("@/components/workspace/browser-view/context", () => ({
  useMaybeBrowserView: () => null,
}));
rs.mock("@/components/workspace/sidecar/context", () => ({
  useMaybeSidecar: () => null,
}));
rs.mock("@/components/workspace/messages/context", () => ({
  useThread: () => ({ thread: { values: {} } }),
}));

import { ChatBox } from "@/components/workspace/chats/chat-box";
import {
  ConversationFilesContext,
  type ConversationFilesState,
} from "@/components/workspace/chats/conversation-files";

beforeEach(() => {
  state.open = true;
  state.selected = "/mnt/user-data/outputs/report.md";
  state.close.mockReset().mockImplementation((open: boolean) => {
    state.open = open;
  });
  state.deselect.mockReset().mockImplementation(() => {
    state.selected = null;
  });
  state.mount.mockClear();
});
afterEach(cleanup);
const unknown: ConversationFilesState = {
  locked: true,
  sentence: null,
  agent: null,
};
function view(value: ConversationFilesState) {
  return (
    <ConversationFilesContext.Provider value={value}>
      <ChatBox threadId="thread">Conversation</ChatBox>
    </ConversationFilesContext.Provider>
  );
}
test("unknown binding hides file content without erasing the owned restored panel", () => {
  render(view(unknown));
  expect(state.close).not.toHaveBeenCalled();
  expect(state.deselect).not.toHaveBeenCalled();
  expect(state.mount).not.toHaveBeenCalled();
  expect(screen.queryByTestId("file-panel")).toBeNull();
});
test("a positively unbound conversation restores its saved panel after discovery", async () => {
  const rendered = render(view(unknown));
  expect(state.mount).not.toHaveBeenCalled();
  rendered.rerender(view({ ...unknown, locked: false }));
  await waitFor(() => expect(screen.getByTestId("file-panel")).toBeDefined());
  expect(state.selected).toBe("/mnt/user-data/outputs/report.md");
  expect(state.close).not.toHaveBeenCalled();
});
test("a positive agent binding clears the ordinary panel without mounting file content", () => {
  render(
    view({
      ...unknown,
      agent: {
        id: "a".repeat(32),
        name: "Researcher",
        custody: "personal",
        status: "active",
        requester_id: "reader",
      },
    }),
  );
  expect(state.close).toHaveBeenCalledWith(false);
  expect(state.deselect).toHaveBeenCalled();
  expect(state.mount).not.toHaveBeenCalled();
});
