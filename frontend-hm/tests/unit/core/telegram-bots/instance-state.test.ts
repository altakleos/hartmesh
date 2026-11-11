import { describe, expect, it } from "@rstest/core";

import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import {
  CLOSED_TELEGRAM,
  type AgentInstancesFeature,
} from "@/core/features/api";
import { instanceConnectBlock } from "@/core/telegram-bots/instance-state";

import { listOf } from "../../components/workspace/telegram/support";
const instance: AgentInstanceDetail = {
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
  activity: {
    running_runs: 0,
    running: [],
    last_active_at: null,
    open_conversations: 0,
  },
  home: { state: "available", workspace: "absent" },
  execution: { execution: "ready", reason: null },
};
const telegram = {
  ...CLOSED_TELEGRAM,
  known: true,
  enabled: true,
  bots: "ready" as const,
};
const feature: AgentInstancesFeature = {
  known: true,
  enabled: true,
  execution: "ready",
  reason: null,
  assistants: { perUser: 3, allowed: 3, mine: 0, inUse: 0 },
  employees: { allowed: 5, inUse: 0 },
};
const input = { instance, telegram, feature, bots: listOf([]), canRead: true };
describe("instance Telegram Connect availability", () => {
  it("permits a paused instance and a busy Home", () => {
    const paused: AgentInstanceDetail = {
      ...instance,
      status: "suspended",
      home: { ...instance.home!, state: "busy" },
    };
    expect(instanceConnectBlock(input)).toBeNull();
    expect(
      instanceConnectBlock({
        ...input,
        instance: paused,
      }),
    ).toBeNull();
  });
  it("uses section, policy, access, lifecycle, kind, execution and quota precedence", () => {
    const disabled = {
      ...input,
      instance: { ...instance, status: "archived" as const, permissions: 0 },
      feature: {
        ...feature,
        execution: "unavailable" as const,
        reason: "operator-hold" as const,
        employees: { allowed: 0, inUse: 0 },
      },
      bots: listOf([], {
        limits: { per_person: 0, used: 0 },
        policies: [{ instance_id: instance.id, policy: "off" }],
      }),
    };
    expect(
      instanceConnectBlock({
        ...disabled,
        telegram: { ...telegram, enabled: false },
      })?.kind,
    ).toBe("telegram");
    expect(instanceConnectBlock(disabled)?.kind).toBe("policy");
    const noPolicy = { ...disabled, bots: { ...disabled.bots, policies: [] } };
    expect(instanceConnectBlock(noPolicy)?.kind).toBe("use");
    expect(
      instanceConnectBlock({
        ...noPolicy,
        instance: { ...noPolicy.instance, permissions: 7 },
      })?.kind,
    ).toBe("retired");
    expect(instanceConnectBlock({ ...noPolicy, instance })?.kind).toBe(
      "company-off",
    );
    const runsOff = {
      ...noPolicy,
      instance,
      feature: { ...noPolicy.feature, employees: feature.employees },
    };
    expect(instanceConnectBlock(runsOff)).toEqual({
      kind: "execution",
      reason: "operator-hold",
    });
    expect(instanceConnectBlock({ ...runsOff, feature })?.kind).toBe("limit");
  });
  it("separates each kind's count and honors per-instance execution", () => {
    expect(
      instanceConnectBlock({
        ...input,
        feature: {
          ...feature,
          assistants: { ...feature.assistants, perUser: 0 },
        },
      }),
    ).toBeNull();
    expect(
      instanceConnectBlock({
        ...input,
        instance: { ...instance, custody: "personal" },
        feature: {
          ...feature,
          assistants: { ...feature.assistants, perUser: 0 },
        },
      })?.kind,
    ).toBe("personal-off");
    expect(
      instanceConnectBlock({
        ...input,
        instance: {
          ...instance,
          execution: { execution: "unavailable", reason: "host-replaced" },
        },
      }),
    ).toEqual({ kind: "execution", reason: "host-replaced" });
  });
  it("fails closed before discovery or read authority", () => {
    expect(instanceConnectBlock({ ...input, bots: undefined })?.kind).toBe(
      "loading",
    );
    expect(instanceConnectBlock({ ...input, canRead: false })?.kind).toBe(
      "ceiling",
    );
  });
});
