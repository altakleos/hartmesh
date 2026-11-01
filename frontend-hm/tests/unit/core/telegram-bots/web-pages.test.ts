import { describe, expect, it } from "@rstest/core";

import {
  type AgentTarget,
  isWebPageKey,
  parseTelegramTarget,
  serverTargetOf,
  telegramTargetOf,
  webPageHref,
  type WebPageKey,
} from "@/core/telegram-bots/web-pages";

const id = "0123456789abcdef0123456789abcdef";
const targets: AgentTarget[] = [
  { kind: "chat" },
  { kind: "custom_agent", name: "research-helper" },
  { kind: "agent", id, name: "Maya" },
];

describe("telegram targets", () => {
  it("a target is written and read back, and holds no colon", () => {
    expect(targets.map(telegramTargetOf)).toEqual([
      "chat",
      "custom_agent.research-helper",
      `agent.${id}`,
    ]);
    expect(targets.some((item) => telegramTargetOf(item).includes(":"))).toBe(
      false,
    );
    expect(parseTelegramTarget("chat")).toEqual({ kind: "chat" });
    expect(parseTelegramTarget("custom_agent.research-helper")).toEqual({
      kind: "custom_agent",
      name: "research-helper",
    });
    expect(parseTelegramTarget(`agent.${id}`)).toEqual({ kind: "agent", id });
  });

  it("a malformed value gives null", () => {
    for (const value of [
      null,
      "",
      "chats",
      "custom_agent.",
      "custom_agent.a b",
      "custom_agent.a.b",
      "agent.short",
      `agent.${"z".repeat(32)}`,
    ]) {
      expect(parseTelegramTarget(value)).toBeNull();
    }
  });

  it("names a target as the server does", () => {
    expect(targets.map(serverTargetOf)).toEqual([
      { agent_kind: "lead_agent", agent_ref: "lead_agent" },
      { agent_kind: "custom_agent", agent_ref: "research-helper" },
      { agent_kind: "instance", agent_ref: id },
    ]);
  });
});

describe("webPageHref", () => {
  const chat: AgentTarget = { kind: "chat" };
  const custom: AgentTarget = { kind: "custom_agent", name: "helper" };
  const agent = (permissions?: number): AgentTarget => ({
    kind: "agent",
    id,
    name: "Maya",
    permissions,
  });
  const here = "/workspace/chats/thread";
  const instance = `/workspace/instances?instance=${id}`;

  it("gives every address of the table for each kind of agent", () => {
    const table: [WebPageKey, string | null, string | null, string | null][] = [
      ["access", `${instance}&tab=access`, null, null],
      ["files", `${instance}&tab=files`, null, null],
      ["memory", `${instance}&tab=memory`, `${here}?settings=memory`, null],
      [
        "conversations",
        `${instance}&tab=conversations`,
        "/workspace/chats",
        "/workspace/chats",
      ],
      ["work", `${instance}&tab=work`, null, null],
      ["agentSettings", `${instance}&tab=settings`, null, null],
      [
        "addAssistant",
        "/workspace/assistants",
        "/workspace/assistants",
        "/workspace/assistants",
      ],
      [
        "addEmployee",
        "/workspace/employees",
        "/workspace/employees",
        "/workspace/employees",
      ],
      [
        "telegram",
        `${here}?settings=channels&telegram=agent.${id}`,
        `${here}?settings=channels&telegram=chat`,
        `${here}?settings=channels&telegram=custom_agent.helper`,
      ],
      [
        "timezone",
        `${here}?settings=channels`,
        `${here}?settings=channels`,
        `${here}?settings=channels`,
      ],
      [
        "scheduled",
        "/workspace/scheduled-tasks",
        "/workspace/scheduled-tasks",
        "/workspace/scheduled-tasks",
      ],
      [
        "account",
        `${here}?settings=account`,
        `${here}?settings=account`,
        `${here}?settings=account`,
      ],
    ];
    expect(table).toHaveLength(12);
    for (const [key, forAgent, forChat, forCustom] of table) {
      expect(webPageHref(key, agent(6), here)).toBe(forAgent);
      expect(webPageHref(key, chat, here)).toBe(forChat);
      expect(webPageHref(key, custom, here)).toBe(forCustom);
    }
  });

  it("gives null where the reader cannot use the page", () => {
    for (const key of ["access", "agentSettings"] as const) {
      expect(webPageHref(key, agent(2), here)).toBeNull();
    }
    for (const key of ["files", "work", "memory"] as const) {
      expect(webPageHref(key, agent(4), here)).toBeNull();
    }
    for (const key of [
      "access",
      "agentSettings",
      "files",
      "work",
      "memory",
    ] as const) {
      expect(webPageHref(key, agent(undefined), here)).toBeNull();
    }
  });

  it("keeps the current path for an address that opens the settings", () => {
    expect(webPageHref("account", chat, "/workspace/agents")).toBe(
      "/workspace/agents?settings=account",
    );
  });

  it("knows the twelve keys and no other", () => {
    expect(isWebPageKey("telegram")).toBe(true);
    expect(isWebPageKey("constructor")).toBe(false);
    expect(isWebPageKey(1)).toBe(false);
  });
});
