import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({
  user: { id: "alice", system_role: "user", permissions: ["agents:read"] },
}));
const i18n = rs.hoisted(() => ({ t: {} as unknown }));
const channels = rs.hoisted(() => ({
  enabled: true,
  providers: [] as unknown[],
  isLoading: false,
  error: null,
}));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: i18n.t }) }));
rs.mock("@/components/ui/sidebar", () => ({
  SidebarGroup: ({ children }: { children: ReactNode }) => (
    <section>{children}</section>
  ),
  SidebarGroupLabel: ({ children }: { children: ReactNode }) => (
    <h2>{children}</h2>
  ),
  SidebarMenu: ({ children }: { children: ReactNode }) => <ul>{children}</ul>,
  SidebarMenuItem: ({ children }: { children: ReactNode }) => (
    <li>{children}</li>
  ),
  useSidebar: () => ({ open: true }),
}));
rs.mock("@/core/channels/hooks", () => ({
  useChannelProviders: () => channels,
  useConfigureChannelProvider: () => ({}),
  useConnectChannelProvider: () => ({}),
}));
rs.mock("@/components/workspace/settings/settings-dialog-store", () => ({
  openSettingsDialog: rs.fn(),
}));

import { WorkspaceChannelsList } from "@/components/workspace/channels/workspace-channels-list";
import { fetch } from "@/core/api/fetcher";
import { useTelegram } from "@/core/features/hooks";
import { createEnUS } from "@/core/i18n/locales/en-US";

import {
  bot,
  instance,
  clearClients,
  json,
  listOf,
  renderWithClient,
  server,
} from "./support";

const t = createEnUS("HartMesh");
i18n.t = t;

function serve(telegram: unknown) {
  const { handle } = server({
    "GET /api/features": () =>
      json({ agents_api: { enabled: true }, telegram }),
    "GET /api/telegram-bots": () => json(listOf([])),
  });
  rs.mocked(fetch).mockImplementation(handle);
}

function Answered() {
  return useTelegram().known ? <output>answered</output> : null;
}

afterEach(() => {
  cleanup();
  clearClients();
  rs.mocked(fetch).mockReset();
});

describe("the channels group in the sidebar", () => {
  it("the group is drawn when no other channel is enabled", async () => {
    serve({ enabled: true, bots: "ready", reason: null });
    renderWithClient(<WorkspaceChannelsList />);
    expect(
      await screen.findByRole("heading", { name: t.sidebar.channels }),
    ).toBeTruthy();
    expect(await screen.findByRole("button", { name: /Connect/ })).toBeTruthy();
  });

  it("leaves the sidebar alone when Telegram is off and no channel is enabled", async () => {
    serve({ enabled: false, bots: "unavailable", reason: "off" });
    renderWithClient(
      <>
        <Answered />
        <WorkspaceChannelsList />
      </>,
    );
    await screen.findByText("answered");
    expect(screen.queryByRole("heading")).toBeNull();
  });
});

it.each([
  ["suspended", 7, "agent_unavailable", "1 connected"],
  ["archived", 7, "agent_unavailable", "1 connected"],
  ["suspended", 2, "agent_unavailable", "1 to check"],
  ["suspended", 7, "token_rejected", "1 to check"],
] as const)(
  "uses current instance facts in sidebar attention for %s/%s/%s",
  async (status, permissions, health, label) => {
    const target = instance({ status, permissions });
    const { calls, handle } = server({
      "GET /api/features": () =>
        json({ telegram: { enabled: true, bots: "ready", reason: null } }),
      "GET /api/telegram-bots": () =>
        json(
          listOf([
            bot({ agent_kind: "instance", agent_ref: target.id, health }),
          ]),
        ),
      [`GET /api/agent-instances/${target.id}?include_deleted=true`]: () =>
        json(target),
    });
    rs.mocked(fetch).mockImplementation(handle);
    renderWithClient(<WorkspaceChannelsList />);
    await waitFor(() =>
      expect(calls.some((call) => call.url.includes("include_deleted"))).toBe(
        true,
      ),
    );
    await screen.findByRole("button", { name: label });
  },
);
