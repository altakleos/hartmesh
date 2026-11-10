import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, screen, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  user: { id: "alice", system_role: "user" },
  t: {} as unknown,
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: state.user }),
}));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: state.t }) }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
import {
  openSettingsDialog,
  setSettingsDialogOpen,
} from "@/components/workspace/settings/settings-dialog-store";
import { TelegramRemovedNotices } from "@/components/workspace/telegram/removed-notices";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { createEnUS } from "@/core/i18n/locales/en-US";
import type { RemovedBot } from "@/core/telegram-bots/api";

import { clearClients, queries, renderWithClient } from "./support";
const event: RemovedBot = {
  id: "event-a",
  reason: "agent-deleted",
  agent_kind: "instance",
  agent_ref: "agent-a",
  agent_name: "Mira",
  bot_username: "mira_bot",
  at: new Date().toISOString(),
};
const view = (events: RemovedBot[]) => (
  <FileActionLifetimeProvider>
    <TelegramRemovedNotices events={events} />
  </FileActionLifetimeProvider>
);
beforeEach(() => {
  state.t = createEnUS("HartMesh");
  state.user = { id: "alice", system_role: "user" };
  openSettingsDialog("channels");
  rs.stubGlobal(
    "fetch",
    rs.fn().mockResolvedValue(new Response(null, { status: 204 })),
  );
  rs.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
});
afterEach(() => {
  cleanup();
  clearClients();
  setSettingsDialogOpen(false);
  rs.unstubAllGlobals();
  rs.restoreAllMocks();
});
it("renders a deletion notice, marks it once, and retains it after the server drops it", async () => {
  const tree = renderWithClient(view([event]));
  await screen.findByText(/Mira was deleted/);
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
  expect(rs.mocked(fetch).mock.calls[0]?.[0]).toBe(
    "/api/telegram-bots/removed/event-a/seen",
  );
  tree.rerender(
    <QueryClientProvider client={queries()}>{view([])}</QueryClientProvider>,
  );
  expect(screen.getByText(/Mira was deleted/)).toBeDefined();
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("waits for a visible open Telegram settings page before marking", async () => {
  rs.spyOn(document, "visibilityState", "get").mockReturnValue("hidden");
  renderWithClient(view([event]));
  await screen.findByText(/Mira was deleted/);
  expect(fetch).not.toHaveBeenCalled();
  await act(async () => {
    rs.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
});
it("neither renders nor marks unknown reasons or a closed settings page", () => {
  const tree = renderWithClient(view([{ ...event, reason: "given-away" }]));
  expect(screen.queryByText(/Mira was deleted/)).toBeNull();
  expect(fetch).not.toHaveBeenCalled();
  act(() => setSettingsDialogOpen(false));
  tree.rerender(
    <QueryClientProvider client={queries()}>
      {view([event])}
    </QueryClientProvider>,
  );
  expect(fetch).not.toHaveBeenCalled();
});
it("contains a missing receipt and retires the request with the component", async () => {
  let finish!: (response: Response) => void;
  rs.mocked(fetch).mockImplementation(
    () =>
      new Promise<Response>((resolve) => {
        finish = resolve;
      }),
  );
  const tree = renderWithClient(view([event]));
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
  const signal = rs.mocked(fetch).mock.calls[0]?.[1]?.signal;
  tree.unmount();
  expect(signal?.aborted).toBe(true);
  await act(async () => {
    finish(
      new Response(JSON.stringify({ reason: "not-found" }), { status: 404 }),
    );
  });
  expect(fetch).toHaveBeenCalledTimes(1);
});
