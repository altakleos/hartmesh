import { describe, expect, it } from "@rstest/core";

import {
  selectSeats,
  selectStorageState,
  type FeaturesResponse,
} from "@/core/features/api";

const features = (
  storage: unknown,
  seats: unknown = { allowed: 3, in_use: 2 },
) =>
  ({
    agents_api: { enabled: true },
    storage_spaces: storage,
    accounts: { seats },
  }) as unknown as FeaturesResponse;

describe("managed storage state", () => {
  it("reads state, reason and personal files without exposing storage geometry", () => {
    expect(
      selectStorageState(
        features({
          enabled: true,
          state: "incomplete",
          reason: null,
          own: "not-assigned",
          user_spaces: true,
        }),
      ),
    ).toEqual({
      known: true,
      enabled: true,
      state: "incomplete",
      reason: null,
      own: "not-assigned",
      userSpaces: true,
    });
    expect(
      selectStorageState(
        features({
          enabled: true,
          state: "unavailable",
          reason: "disk-too-small",
          own: "no-seat",
        }),
      ).reason,
    ).toBe("disk-too-small");
  });

  it.each([
    undefined,
    null,
    [],
    true,
    { enabled: "true", state: "ready" },
    { enabled: true },
    { enabled: true, state: "running" },
  ])("fails closed for unreadable state %j", (storage) => {
    expect(selectStorageState(features(storage))).toEqual({
      known: false,
      enabled: false,
      state: "unavailable",
      reason: null,
      own: "ready",
      userSpaces: false,
    });
  });

  it("does not invent missing personal files for unknown own and reason values", () => {
    expect(
      selectStorageState(
        features({
          enabled: true,
          state: "ready",
          reason: "future-reason",
          own: "future-state",
        }),
      ),
    ).toMatchObject({
      known: true,
      state: "ready",
      reason: null,
      own: "ready",
    });
  });

  it("reads an explicit legacy off envelope without treating its zero seats as full", () => {
    const off = features(
      {
        enabled: false,
        state: "off",
        reason: null,
        own: "ready",
        user_spaces: false,
      },
      { allowed: 0, in_use: 0 },
    );
    expect(selectStorageState(off).state).toBe("off");
    expect(selectSeats(off)).toBeNull();
  });

  it("reads whole seat counts only for enabled managed storage", () => {
    expect(selectSeats(features({ enabled: true, state: "ready" }))).toEqual({
      allowed: 3,
      inUse: 2,
    });
    for (const seats of [
      null,
      {},
      { allowed: -1, in_use: 0 },
      { allowed: 2.5, in_use: 0 },
      { allowed: 2, in_use: "1" },
      { allowed: Infinity, in_use: 0 },
    ]) {
      expect(
        selectSeats(features({ enabled: true, state: "ready" }, seats)),
      ).toBeNull();
    }
    expect(
      selectSeats(features({ enabled: false, state: "unavailable" })),
    ).toBeNull();
  });
});
