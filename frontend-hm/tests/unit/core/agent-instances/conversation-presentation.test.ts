import { expect, test } from "@rstest/core";

import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import {
  conversationIdentity,
  conversationSendBlock,
  conversationStopAuthority,
} from "@/core/agent-instances/conversation-presentation";
const binding = {
  id: "a".repeat(32),
  name: "Researcher",
  status: "active" as const,
  custody: "personal" as const,
  requester_id: "reader",
};
const detail: AgentInstanceDetail = {
  ...binding,
  principal: { kind: "nonhuman", subject_id: "ai" },
  creator_id: "reader",
  definition_revision: "revision",
  home_id: "b".repeat(32),
  generation: 1,
  permissions: 7,
  owner_id: "reader",
  supervisor: { kind: "human", subject_id: "reader" },
  execution: { execution: "ready", reason: null },
  home: { state: "available", workspace: "absent" },
  activity: {
    running_runs: 0,
    running: [],
    open_conversations: 1,
    last_active_at: null,
  },
};
test.each([{ isNew: true }, { isMock: true }, { createdHere: "thread" }])(
  "ordinary local/new/mock conversations retain their capabilities before metadata",
  (extra) => {
    expect(
      conversationIdentity({
        threadId: "thread",
        isNew: false,
        isMock: false,
        row: undefined,
        rowKnown: false,
        ...extra,
      }).legacyCapabilities,
    ).toBe(true);
  },
);
test.each([
  { row: undefined, rowKnown: false },
  { row: {}, rowKnown: true },
  { row: { agent_instance: null }, rowKnown: false },
])("uncertain existing rows cannot authorize legacy controls: %j", (input) => {
  expect(
    conversationIdentity({
      threadId: "thread",
      isNew: false,
      isMock: false,
      ...input,
    }).legacyCapabilities,
  ).toBe(false);
});
test("a loaded canonical ordinary row keeps controls and an agent row binds identity independently of feature flags", () => {
  expect(
    conversationIdentity({
      threadId: "thread",
      isNew: false,
      isMock: false,
      row: { agent_instance: null },
      rowKnown: true,
    }).legacyCapabilities,
  ).toBe(true);
  expect(
    conversationIdentity({
      threadId: "thread",
      isNew: false,
      isMock: false,
      row: { agent_instance: binding },
      rowKnown: true,
    }),
  ).toMatchObject({ legacyCapabilities: false, agent: binding });
});
test.each(["deleted", "archived", "suspended"] as const)(
  "status %s has priority over feature or access refusal",
  (status) => {
    expect(
      conversationSendBlock({
        binding: { ...binding, status },
        detail: null,
        feature: { known: true, enabled: false },
        detailStatus: "not-found",
        pendingLifecycle: false,
      }),
    ).toBe(status);
  },
);
test("a forbidden detail response with storage off is not mislabeled as withdrawn access", () => {
  expect(
    conversationSendBlock({
      binding,
      detail: null,
      feature: { known: true, enabled: false },
      detailStatus: "forbidden",
      pendingLifecycle: false,
    }),
  ).toBe("model-off");
  expect(
    conversationSendBlock({
      binding,
      detail: null,
      feature: { known: true, enabled: true },
      detailStatus: "forbidden",
      pendingLifecycle: false,
    }),
  ).toBe("unavailable");
  expect(
    conversationSendBlock({
      binding,
      detail: null,
      feature: { known: true, enabled: true },
      detailStatus: "not-found",
      pendingLifecycle: false,
    }),
  ).toBe("withdrawn");
});
test("a starting workspace can admit messages, while missing Use cannot", () => {
  expect(
    conversationSendBlock({
      binding,
      detail: {
        ...detail,
        home: { state: "available", workspace: "starting" },
      },
      feature: { known: true, enabled: true },
      detailStatus: "ready",
      pendingLifecycle: false,
    }),
  ).toBeNull();
  expect(
    conversationSendBlock({
      binding,
      detail: { ...detail, permissions: 2 },
      feature: { known: true, enabled: true },
      detailStatus: "ready",
      pendingLifecycle: false,
    }),
  ).toBe("no-use");
});
test("only authorized inspectors/managers see pending lifecycle instead of the ordinary paused notice", () => {
  expect(
    conversationSendBlock({
      binding,
      detail: { ...detail, status: "suspended" },
      feature: { known: true, enabled: true },
      detailStatus: "ready",
      pendingLifecycle: true,
    }),
  ).toBe("changing");
  expect(
    conversationSendBlock({
      binding,
      detail: { ...detail, status: "suspended", permissions: 1 },
      feature: { known: true, enabled: true },
      detailStatus: "ready",
      pendingLifecycle: true,
    }),
  ).toBe("suspended");
});
test("an assignment reply uses Work stop authority, while normal replies use their actual sender", () => {
  const running = {
    run_id: "run",
    thread_id: "thread",
    requester_id: "other",
    work_id: "work",
    objective: "Research",
  };
  expect(
    conversationStopAuthority({
      threadId: "thread",
      binding,
      detail: {
        ...detail,
        permissions: 3,
        activity: { ...detail.activity, running: [running] },
      },
      userId: "reader",
      sentHere: false,
      canCancel: true,
    }),
  ).toEqual({ allowed: false, workId: "work" });
  expect(
    conversationStopAuthority({
      threadId: "thread",
      binding,
      detail: {
        ...detail,
        activity: { ...detail.activity, running: [running] },
      },
      userId: "reader",
      sentHere: false,
      canCancel: true,
    }),
  ).toEqual({ allowed: true, workId: "work" });
  expect(
    conversationStopAuthority({
      threadId: "thread",
      binding,
      detail: {
        ...detail,
        permissions: 3,
        activity: {
          ...detail.activity,
          running: [{ ...running, work_id: null }],
        },
      },
      userId: "reader",
      sentHere: false,
      canCancel: true,
    }).allowed,
  ).toBe(false);
});
