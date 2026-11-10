import { describe, expect, it } from "@rstest/core";

import type { AgentInstance } from "@/core/agent-instances/api";
import { CLOSED_TELEGRAM } from "@/core/features/api";
import { rowOf, targetRowsOf } from "@/core/telegram-bots/rows";

import { bot } from "../../components/workspace/telegram/support";

const instance: AgentInstance = {
  id: "agent-a",
  name: "Mira",
  custody: "company",
  owner_id: null,
  creator_id: "alice",
  principal: { kind: "nonhuman", subject_id: "agent-a" },
  supervisor: { kind: "human", subject_id: "alice" },
  definition_revision: "revision",
  home_id: "home",
  status: "active",
  generation: 1,
  permissions: 7,
};
const feature = {
  ...CLOSED_TELEGRAM,
  known: true,
  enabled: true,
  bots: "ready" as const,
};
const linked = () => bot({ agent_kind: "instance", agent_ref: instance.id });
const row = (change: Partial<Parameters<typeof rowOf>[0]> = {}) =>
  rowOf({
    name: instance.name,
    bot: linked(),
    feature,
    reader: { admin: false, provider: "Acme" },
    now: Date.now(),
    instance,
    ...change,
  });
describe("instance Telegram rows", () => {
  it.each([
    [15, "policy_off", "active", 7, "state.notAllowed", []],
    [16, "agent_unavailable", "active", 2, "state.noAccess", ["action.remove"]],
    [
      18,
      "agent_unavailable",
      "archived",
      7,
      "state.retired",
      ["action.remove"],
    ],
    [19, "agent_unavailable", "suspended", 7, "state.paused", ["action.open"]],
  ] as const)(
    "row %s follows current health, grants and lifecycle",
    (number, health, status, permissions, key, buttons) => {
      const view = row({
        bot: { ...linked(), health },
        instance: { ...instance, status, permissions },
      });
      expect(view.row).toBe(number);
      expect(view.lines.map((l) => l.key)).toEqual([key]);
      expect(view.buttons).toEqual(buttons);
    },
  );
  it("keeps lost access ahead of archived status and permits a cleared health to recover", () => {
    expect(
      row({ bot: { ...linked(), health: "agent_unavailable" }, instance: null })
        .row,
    ).toBe(16);
    expect(
      row({
        bot: { ...linked(), health: "agent_unavailable" },
        instance: { ...instance, status: "archived", permissions: 2 },
      }).row,
    ).toBe(16);
    expect(row({ bot: { ...linked(), health: "agent_unavailable" } }).row).toBe(
      29,
    );
  });
  it("execution outage precedes delivery degradation and protection", () => {
    const view = row({
      bot: { ...linked(), delivery_failed: 1, policy: "protected" },
      execution: { execution: "unavailable", reason: "operator-hold" },
    });
    expect(view.row).toBe(25);
    expect(view.lines[0]?.key).toBe("state.cannotWork");
    expect(row({ bot: { ...linked(), policy: "protected" } }).row).toBe(28);
    expect(
      row({
        bot: {
          ...linked(),
          acts_paused_since: new Date().toISOString(),
          policy: "protected",
        },
        execution: { execution: "unavailable", reason: "operator-hold" },
      }).row,
    ).toBe(24);
  });
  it("orders chat, own assistants, colleagues, employees, custom agents, then inaccessible bots", () => {
    const entries = [
      {
        ...instance,
        id: "other",
        name: "A",
        custody: "personal" as const,
        owner_id: "bob",
      },
      {
        ...instance,
        id: "own",
        name: "Z",
        custody: "personal" as const,
        owner_id: "alice",
      },
      { ...instance, id: "employee", name: "Employee" },
      { ...instance, id: "no-use", name: "Inventory", permissions: 0 },
      { ...instance, id: "deleted", status: "deleted" as const },
    ];
    const rows = targetRowsOf({
      chatName: "HartMesh",
      agentNames: ["Custom"],
      bots: [
        bot({ agent_kind: "instance", agent_ref: "lost", agent_name: "Lost" }),
      ],
      instances: entries,
      ownerId: "alice",
    });
    expect(rows.map((r) => r.name)).toEqual([
      "HartMesh",
      "Z",
      "A",
      "Employee",
      "Custom",
      "Lost",
    ]);
    expect(rows.at(-1)?.target).toEqual({
      kind: "agent",
      id: "lost",
      name: "Lost",
      permissions: 0,
    });
  });
});

it.each([
  [16, { ...instance, permissions: 0 }],
  [18, { ...instance, status: "archived" as const }],
  [19, { ...instance, status: "suspended" as const }],
  [16, null],
] as const)(
  "current instance authority shows row %s before health reconciliation",
  (expected, current) => {
    expect(row({ instance: current }).row).toBe(expected);
    expect(
      row({ instance: current, bot: { ...linked(), policy: "protected" } }).row,
    ).toBe(expected);
    expect(
      row({ instance: current, bot: { ...linked(), health: "conflict" } }).row,
    ).toBe(11);
  },
);
