import { describe, expect, it } from "@rstest/core";

import * as api from "@/core/features/api";

const full = {
  enabled: true,
  execution: "ready",
  reason: null,
  ai_assistants: { per_user: 2, allowed: 6, in_use: 4, mine: 1 },
  ai_employees: { allowed: 10, in_use: 5 },
};
const features = (value: unknown, people: unknown = "exact") =>
  ({
    agents_api: { enabled: true },
    agent_instances: value,
    people_search: people,
  }) as unknown as api.FeaturesResponse;
const emptyCounts = {
  assistants: { perUser: 0, allowed: 0, inUse: 0, mine: 0 },
  employees: { allowed: 0, inUse: 0 },
};
const closed = {
  known: false,
  enabled: false,
  execution: "unavailable",
  reason: null,
  ...emptyCounts,
};

describe("agent instance feature discovery", () => {
  it("reads current execution and admitted counts without storage geometry", () => {
    expect(
      api.selectAgentInstances(
        features({ ...full, prepared_spaces: 300, capacity_bytes: 999999 }),
      ),
    ).toEqual({
      known: true,
      enabled: true,
      execution: "ready",
      reason: null,
      assistants: { perUser: 2, allowed: 6, inUse: 4, mine: 1 },
      employees: { allowed: 10, inUse: 5 },
    });
  });

  it.each([
    undefined,
    null,
    [],
    true,
    "enabled",
    {},
    { ...full, enabled: "true" },
    { ...full, enabled: undefined },
    { ...full, execution: "starting" },
    { ...full, execution: undefined },
  ])("closes absent and unreadable deployment answers: %j", (value) => {
    expect(api.selectAgentInstances(features(value))).toEqual(closed);
  });

  it("closes a failed or not-yet-completed request", () => {
    expect(api.selectAgentInstances(undefined)).toEqual(closed);
  });

  it.each([
    "storage-unavailable",
    "agents-none",
    "operator-hold",
    "host-replaced",
    "space-unverified",
    "space-not-assigned",
  ])("preserves the known unavailable execution reason %s", (reason) => {
    expect(
      api.selectAgentInstances(
        features({ ...full, execution: "unavailable", reason }),
      ),
    ).toMatchObject({ known: true, execution: "unavailable", reason });
  });

  it("keeps a readable disabled answer distinct from an unknown one", () => {
    expect(
      api.selectAgentInstances(features({ ...full, enabled: false })),
    ).toMatchObject({ known: true, enabled: false });
    expect(
      api.selectAgentInstances(features({ ...full, reason: "future-reason" })),
    ).toMatchObject({ known: true, reason: null });
  });

  it.each([undefined, null, "2", true, -1, 1.5, Infinity, NaN, 2 ** 53])(
    "does not coerce or publish invalid counts: %j",
    (count) => {
      expect(
        api.selectAgentInstances(
          features({
            ...full,
            ai_assistants: {
              per_user: count,
              allowed: count,
              in_use: count,
              mine: count,
            },
            ai_employees: { allowed: count, in_use: count },
          }),
        ),
      ).toEqual({
        known: true,
        enabled: true,
        execution: "ready",
        reason: null,
        ...emptyCounts,
      });
    },
  );

  it("reads missing count objects as zero without hiding durable history", () => {
    expect(
      api.selectAgentInstances(
        features({ enabled: true, execution: "unavailable" }),
      ),
    ).toEqual({
      known: true,
      enabled: true,
      execution: "unavailable",
      reason: null,
      ...emptyCounts,
    });
  });

  it.each([undefined, null, true, "", "prefix ", "PREFIX", "future"])(
    "defaults people search to exact for %j",
    (value) => {
      expect(api.selectPeopleSearch(features(full, value))).toBe("exact");
    },
  );

  it("offers prefix search only when explicitly reported", () => {
    expect(api.selectPeopleSearch(features(full, "prefix"))).toBe("prefix");
    expect(api.selectPeopleSearch(undefined)).toBe("exact");
  });

  it("exposes the storage selector without changing the existing file states", () => {
    expect(api.selectStorage(undefined)).toEqual({
      known: false,
      enabled: false,
      state: "unavailable",
      reason: null,
      own: "ready",
      userSpaces: false,
    });
    expect(
      api.selectStorage({
        ...features(full),
        storage_spaces: {
          enabled: true,
          state: "incomplete",
          user_spaces: true,
          own: "not-assigned",
          reason: "future-reason",
        },
      }),
    ).toEqual({
      known: true,
      enabled: true,
      state: "incomplete",
      reason: null,
      own: "not-assigned",
      userSpaces: true,
    });
  });
});
