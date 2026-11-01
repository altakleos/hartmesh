import { afterEach, expect, rs, test } from "@rstest/core";

import {
  DEFAULT_LOCAL_SETTINGS,
  getLocalSettings,
  getThreadModelName,
  LOCAL_SETTINGS_KEY,
  saveLocalSettings,
  saveThreadModelName,
} from "@/core/settings/local";

afterEach(() => {
  rs.unstubAllGlobals();
});

test("defaults token usage to header total plus per-turn breakdown", () => {
  expect(DEFAULT_LOCAL_SETTINGS.tokenUsage).toEqual({
    headerTotal: true,
    inlineMode: "per_turn",
  });
});

test("keeps the Telegram offer's dismissal and defaults settings saved before it", () => {
  const stored = new Map<string, string>();
  rs.stubGlobal("window", {
    localStorage: {
      getItem: (key: string) => stored.get(key) ?? null,
      setItem: (key: string, value: string) => stored.set(key, value),
    },
  });

  stored.set(LOCAL_SETTINGS_KEY, JSON.stringify({ notification: {} }));
  expect(getLocalSettings().telegram).toEqual({ offerDismissed: false });

  saveLocalSettings({
    ...DEFAULT_LOCAL_SETTINGS,
    telegram: { offerDismissed: true },
  });
  expect(getLocalSettings().telegram).toEqual({ offerDismissed: true });
});

test("falls back when localStorage access is blocked", () => {
  rs.stubGlobal("window", {
    get localStorage() {
      throw new DOMException("Blocked", "SecurityError");
    },
  });

  expect(getLocalSettings()).toEqual(DEFAULT_LOCAL_SETTINGS);
  expect(getThreadModelName("thread-1")).toBeUndefined();
  expect(() => saveLocalSettings(DEFAULT_LOCAL_SETTINGS)).not.toThrow();
  expect(() => saveThreadModelName("thread-1", "model-1")).not.toThrow();
});
