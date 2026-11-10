import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { cleanup, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({ admin: false, t: {} as unknown }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "alice", system_role: state.admin ? "admin" : "user" },
  }),
}));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: state.t }) }));
rs.mock("@/core/features/hooks", () => ({
  useBranding: () => ({
    providerName: "Acme",
    supportURL: "https://help.example.test",
  }),
  useTelegram: () => ({ known: true }),
  useStorage: () => ({ reason: "host-unqualified" }),
}));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));

import { TelegramBotRow } from "@/components/workspace/telegram/bot-row";
import type { AgentInstance } from "@/core/agent-instances/api";
import { CLOSED_TELEGRAM } from "@/core/features/api";
import { createEnUS } from "@/core/i18n/locales/en-US";
import { pictureOf } from "@/core/telegram-bots/picture";
import { rowOf } from "@/core/telegram-bots/rows";

import { bot, clearClients, json, listOf, renderWithClient } from "./support";

const entry: AgentInstance = {
  id: "agent-a",
  name: "Mira Rose",
  custody: "personal",
  owner_id: "alice",
  creator_id: "alice",
  principal: { kind: "nonhuman", subject_id: "agent-a" },
  supervisor: { kind: "human", subject_id: "alice" },
  definition_revision: "revision",
  home_id: "home",
  status: "active",
  generation: 1,
  permissions: 7,
};
function show(instance: AgentInstance, unavailable = false) {
  const link = bot({ agent_kind: "instance", agent_ref: instance.id });
  const view = rowOf({
    name: instance.name,
    instance,
    bot: link,
    feature: { ...CLOSED_TELEGRAM, enabled: true, bots: "ready" },
    reader: { admin: state.admin, provider: "Acme" },
    now: Date.now(),
    ...(unavailable
      ? {
          execution: {
            execution: "unavailable",
            reason: "storage-unavailable",
          } as const,
        }
      : {}),
  });
  return renderWithClient(
    <ul>
      <TelegramBotRow
        row={{
          target: { kind: "agent", id: instance.id, name: instance.name },
          name: instance.name,
          instance,
          bot: link,
        }}
        view={view}
        now={Date.now()}
        disabledBecause={null}
        onDialog={rs.fn()}
      />
    </ul>,
  );
}
beforeEach(() => {
  state.admin = false;
  state.t = createEnUS("HartMesh");
  rs.stubGlobal(
    "fetch",
    rs.fn(async (url: string) =>
      url.startsWith("/api/v1/auth/users")
        ? json({
            users: [
              {
                id: "bob",
                name: "Bob",
                email: "bob@example.test",
                active: true,
              },
            ],
          })
        : json(listOf([])),
    ),
  );
});
afterEach(() => {
  cleanup();
  clearClients();
  rs.unstubAllGlobals();
});
it("shows an own assistant and the shared Telegram avatar", () => {
  show(entry);
  expect(screen.getByText("AI assistant")).toBeDefined();
  const picture = pictureOf(entry.name, entry.id);
  const swatch = document.createElement("span");
  swatch.style.backgroundColor = picture.color;
  expect(screen.getByText(picture.initials).style.backgroundColor).toBe(
    swatch.style.backgroundColor,
  );
  expect(
    rs
      .mocked(fetch)
      .mock.calls.some(
        ([url]) => typeof url === "string" && url.includes("/auth/users"),
      ),
  ).toBe(false);
});
it("resolves a colleague's name through the existing person directory", async () => {
  show({ ...entry, owner_id: "bob" });
  await screen.findByText("Bob's AI assistant");
  expect(
    rs
      .mocked(fetch)
      .mock.calls.some(([url]) => url === "/api/v1/auth/users?ids=bob"),
  ).toBe(true);
});
it.each([false, true])(
  "shows employee execution help for administrator=%s",
  (admin) => {
    state.admin = admin;
    show({ ...entry, custody: "company", owner_id: null }, true);
    expect(screen.getByText("AI employee")).toBeDefined();
    expect(
      screen.getByText("Connected. Mira Rose cannot work right now."),
    ).toBeDefined();
    if (admin)
      expect(
        screen.getByRole("link", { name: /Acme/ }).getAttribute("href"),
      ).toBe("https://help.example.test/");
    else expect(screen.getByText("Ask an administrator.")).toBeDefined();
  },
);
