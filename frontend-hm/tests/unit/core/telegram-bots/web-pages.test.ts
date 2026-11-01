import { describe, expect, it } from "@rstest/core";

import {
  type AgentTarget,
  parseTelegramTarget,
  serverTargetOf,
  telegramTargetOf,
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
