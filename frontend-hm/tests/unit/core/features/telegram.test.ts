import { describe, expect, it } from "@rstest/core";

import {
  CLOSED_TELEGRAM,
  type FeaturesResponse,
  selectTelegram,
} from "@/core/features/api";

const features = (telegram: unknown) =>
  ({
    agents_api: { enabled: true },
    telegram,
  }) as unknown as FeaturesResponse;
const ready = { enabled: true, bots: "ready", reason: null };

describe("the telegram feature", () => {
  it("returns the closed value without a telegram object", () => {
    expect(selectTelegram(undefined)).toEqual(CLOSED_TELEGRAM);
    expect(selectTelegram(features(null))).toEqual(CLOSED_TELEGRAM);
    expect(
      selectTelegram({ agents_api: { enabled: true } } as FeaturesResponse),
    ).toEqual(CLOSED_TELEGRAM);
    expect(CLOSED_TELEGRAM).toEqual({
      known: false,
      enabled: false,
      bots: "unavailable",
      reason: null,
      reasonCode: null,
      voice: "unavailable",
      voiceProvider: null,
    });
  });

  it("returns the closed value for a wrong type", () => {
    expect(selectTelegram(features({ ...ready, enabled: "true" }))).toEqual(
      CLOSED_TELEGRAM,
    );
    expect(selectTelegram(features({ ...ready, bots: "yes" }))).toEqual(
      CLOSED_TELEGRAM,
    );
    expect(selectTelegram(features("on"))).toEqual(CLOSED_TELEGRAM);
  });

  it("keeps an unknown reason as reasonCode", () => {
    const known = selectTelegram(
      features({ enabled: true, bots: "unavailable", reason: "host-changed" }),
    );
    expect(known.reason).toBe("host-changed");
    expect(known.reasonCode).toBe("host-changed");
    const unknown = selectTelegram(
      features({ enabled: true, bots: "unavailable", reason: "new-reason" }),
    );
    expect(unknown.known).toBe(true);
    expect(unknown.reason).toBeNull();
    expect(unknown.reasonCode).toBe("new-reason");
  });

  it("reads voice and the transcriber", () => {
    expect(
      selectTelegram(
        features({ ...ready, voice: "available", voice_provider: "gemini" }),
      ),
    ).toMatchObject({ voice: "available", voiceProvider: "gemini" });
    expect(
      selectTelegram(
        features({ ...ready, voice: "maybe", voice_provider: "whisper" }),
      ),
    ).toMatchObject({ voice: "unavailable", voiceProvider: null });
  });
});
