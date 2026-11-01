import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const setInput = rs.fn();
const focusComposer = rs.fn();
let inputValue = "";
const presentation: {
  profile?: "business" | "developer";
  starters?: { id: string; title: string; prompt: string }[];
  isLoading?: boolean;
} = {};

rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(),
}));
rs.mock("@/components/ai-elements/prompt-input", () => ({
  usePromptInputController: () => ({
    textInput: { setInput, value: inputValue },
  }),
}));
rs.mock("@/core/features", () => ({
  useWorkspacePresentation: () => presentation,
}));
rs.mock("@/components/workspace/composer-focus", () => ({
  useComposerFocus: () => focusComposer,
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
  setInput.mockReset();
  focusComposer.mockReset();
  inputValue = "";
  delete presentation.profile;
  delete presentation.starters;
  delete presentation.isLoading;
});

describe("Welcome", () => {
  it("offers what the deployment configured", () => {
    presentation.profile = "business";
    presentation.starters = [
      { id: "review", title: "Monthly business review", prompt: "Build it." },
      { id: "summary", title: "Summarize a document", prompt: "Read it." },
    ];

    renderWelcome();

    expect(
      screen.getByRole("button", { name: "Monthly business review" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Summarize a document" }),
    ).toBeTruthy();
  });

  it("says what choosing one will do", () => {
    // Unexplained pills invite the guess that they send.
    presentation.starters = [
      { id: "review", title: "Monthly business review", prompt: "Build it." },
    ];

    renderWelcome();

    expect(screen.getByText(enUS.welcome.startersHint)).toBeTruthy();
  });

  it("fills the message box, sends nothing, and hands back the cursor", () => {
    // The first moment is "pick the thing, drop the file, say the month", so
    // choosing a starter must leave the person the last word — and the caret,
    // or the next thing typed lands nowhere.
    presentation.starters = [
      { id: "review", title: "Monthly business review", prompt: "Build it." },
    ];

    renderWelcome();
    fireEvent.click(
      screen.getByRole("button", { name: "Monthly business review" }),
    );

    expect(setInput).toHaveBeenCalledWith("Build it.");
    expect(focusComposer).toHaveBeenCalled();
  });

  it("never destroys what a person had already typed", () => {
    inputValue = "need the august numbers for the accountant";
    presentation.starters = [
      { id: "review", title: "Monthly business review", prompt: "Build it." },
    ];

    renderWelcome();
    fireEvent.click(
      screen.getByRole("button", { name: "Monthly business review" }),
    );

    expect(setInput).toHaveBeenCalledWith(
      "need the august numbers for the accountant\n\nBuild it.",
    );
  });

  it("treats swapping between two starters as a swap", () => {
    inputValue = "Build it.";
    presentation.starters = [
      { id: "review", title: "Monthly business review", prompt: "Build it." },
      { id: "summary", title: "Summarize a document", prompt: "Read it." },
    ];

    renderWelcome();
    fireEvent.click(
      screen.getByRole("button", { name: "Summarize a document" }),
    );

    expect(setInput).toHaveBeenCalledWith("Read it.");
  });

  it("drops the product blurb in a business workspace", () => {
    presentation.profile = "business";
    presentation.starters = [];

    renderWelcome();

    expect(screen.queryByText(enUS.welcome.description)).toBeNull();
    expect(screen.queryByTestId("welcome-starters")).toBeNull();
  });

  it("keeps the blurb for a deployment that asked for it", () => {
    // A Gateway from before this existed reports no `ui` block, which the
    // client resolves to `developer` — so its Home is unchanged.
    presentation.profile = "developer";

    renderWelcome();

    expect(screen.getByText(enUS.welcome.description)).toBeTruthy();
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
