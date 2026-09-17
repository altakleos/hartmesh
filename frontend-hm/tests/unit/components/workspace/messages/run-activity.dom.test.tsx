import { afterEach, describe, expect, it } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

import { RunActivity } from "@/components/workspace/messages/run-duration";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

function renderActivity(label?: string | null) {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <RunActivity startTime={Date.now()} label={label} />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  cleanup();
});

describe("RunActivity", () => {
  it("says what the run is doing when the stream said", () => {
    renderActivity(enUS.runProgress.preparing);
    expect(screen.getByTestId("run-activity").textContent).toContain(
      "Preparing your workspace…",
    );
    expect(screen.queryByText("Working…")).toBeNull();
  });

  it("falls back to Working… when no stage has been heard", () => {
    renderActivity(null);
    expect(screen.getByTestId("run-activity").textContent).toContain(
      "Working…",
    );
  });
});
