import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

let search = "";
const presentation: {
  profile?: "business" | "developer";
  starters?: { id: string; title: string; prompt: string }[];
  isLoading?: boolean;
} = {};

rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(search),
}));
rs.mock("@/core/features", () => ({
  useWorkspacePresentation: () => presentation,
}));

import { Welcome } from "@/components/workspace/welcome";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

function renderWelcome() {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <Welcome />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  cleanup();
  search = "";
  delete presentation.profile;
  delete presentation.starters;
  delete presentation.isLoading;
});

describe("Welcome", () => {
  it("renders no header in a business workspace with configured starters", () => {
    presentation.profile = "business";
    presentation.starters = [
      { id: "review", title: "Monthly business review", prompt: "Build it." },
    ];

    const { container } = renderWelcome();

    expect(container.firstChild).toBeNull();
  });

  it("keeps the instructions on the skill creation route", () => {
    search = "mode=skill";
    presentation.profile = "business";

    renderWelcome();

    expect(
      screen.getByText(`✨ ${enUS.welcome.createYourOwnSkill} ✨`),
    ).toBeTruthy();
    expect(
      screen.getByText(
        enUS.welcome.createYourOwnSkillDescription.replace(/\s+/g, " "),
      ),
    ).toBeTruthy();
  });

  it("keeps the blurb for a deployment that asked for it", () => {
    // A Gateway from before this existed reports no `ui` block, which the
    // client resolves to `developer`, so the blurb is still available.
    presentation.profile = "developer";

    renderWelcome();

    expect(screen.getByText(enUS.welcome.description)).toBeTruthy();
    expect(screen.queryAllByText("Hello, again!")).toHaveLength(0);
    expect(screen.queryByText("👋")).toBeNull();
  });

  it("holds the blurb until the deployment has answered", () => {
    // Otherwise every cold load of a business Home paints the product pitch in
    // the spot the eye lands first and then yanks it away.
    presentation.isLoading = true;

    renderWelcome();

    expect(screen.queryByText(enUS.welcome.description)).toBeNull();
  });

  it("holds the blurb when the deployment could not be reached", () => {
    // A failed fetch leaves the profile unknown with nothing still loading.
    // Copy waits for an answer; it does not assume the flattering one.
    presentation.isLoading = false;

    renderWelcome();

    expect(screen.queryByText(enUS.welcome.description)).toBeNull();
  });
});
