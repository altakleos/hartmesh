import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

const auth = rs.hoisted(() => ({
  user: { id: "alice", system_role: "user", permissions: [] as string[] },
}));
const i18n = rs.hoisted(() => ({ t: {} as unknown }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: i18n.t }) }));
rs.mock("next/navigation", () => ({ usePathname: () => "/workspace/chats/t" }));

import { WebPageLinks } from "@/components/workspace/messages/web-page-links";
import { createEnUS } from "@/core/i18n/locales/en-US";
import type { AgentTarget, WebPageKey } from "@/core/telegram-bots/web-pages";

i18n.t = createEnUS("HartMesh");

const chat: AgentTarget = { kind: "chat" };
const custom: AgentTarget = { kind: "custom_agent", name: "helper" };
const links = () =>
  screen
    .queryAllByRole("link")
    .map((link) => [link.textContent, link.getAttribute("href")]);

afterEach(() => {
  cleanup();
  auth.user = { id: "alice", system_role: "user", permissions: [] };
});

function show(keys: WebPageKey[], target: AgentTarget = chat) {
  render(<WebPageLinks keys={keys} target={target} />);
}

describe("WebPageLinks", () => {
  it("draws one link per key with its label and address", () => {
    show(["memory", "telegram", "scheduled"]);
    expect(links()).toEqual([
      ["Open Memory", "/workspace/chats/t?settings=memory"],
      [
        "Open Telegram settings",
        "/workspace/chats/t?settings=channels&telegram=chat",
      ],
      ["Open scheduled tasks", "/workspace/scheduled-tasks"],
    ]);
    expect(screen.getByTestId("web-page-links")).toBeTruthy();
  });

  it("gives the time zone key the Telegram label and the plain section", () => {
    show(["timezone"]);
    expect(links()).toEqual([
      ["Open Telegram settings", "/workspace/chats/t?settings=channels"],
    ]);
  });

  it("leaves out a key with no address, and draws nothing with none left", () => {
    show(["memory"], custom);
    expect(screen.queryByTestId("web-page-links")).toBeNull();
  });

  it("offers an agent's Memory only with the memory ceiling", () => {
    const agent: AgentTarget = {
      kind: "agent",
      id: "a".repeat(32),
      name: "Maya",
      permissions: 2,
    };
    show(["memory"], agent);
    expect(links()).toEqual([]);
    cleanup();
    auth.user = {
      id: "alice",
      system_role: "user",
      permissions: ["memory:read"],
    };
    show(["memory"], agent);
    expect(links()).toHaveLength(1);
  });

  it("offers Add AI employee to an administrator only", () => {
    show(["addEmployee"]);
    expect(links()).toEqual([]);
    cleanup();
    auth.user = { id: "alice", system_role: "admin", permissions: [] };
    show(["addEmployee"]);
    expect(links()).toEqual([["Add AI employee", "/workspace/employees"]]);
  });
});
