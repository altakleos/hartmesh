import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

/**
 * The wire between `ui.profile` and the settings dialog.
 *
 * The pure filter and the decision hook are tested on their own; nothing else
 * checks that the dialog actually asks. Without this, replacing the call at
 * the top of `SettingsDialog` with a literal `true` leaves the suite green.
 *
 * The end-to-end suite cannot cover it: that harness runs with
 * `DEER_FLOW_AUTH_DISABLED=1`, which injects an administrator server-side, and
 * an administrator is offered every screen under either profile.
 */
const presentation = rs.hoisted(() => ({
  developerSurfacesVisible: true,
  isLoading: false,
}));

rs.mock("@/core/features", () => ({
  useDeveloperSurfacesVisible: () => presentation.developerSurfacesVisible,
  useWorkspacePresentation: () => ({ isLoading: presentation.isLoading }),
}));

import { SettingsDialog } from "@/components/workspace/settings/settings-dialog";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

const { sections } = enUS.settings;

function renderDialog(defaultSection?: "skills" | "appearance") {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <SettingsDialog open defaultSection={defaultSection} />
    </I18nContext.Provider>,
  );
}

function tabNames() {
  return screen
    .getAllByRole("button")
    .map((button) => button.textContent?.trim())
    .filter(Boolean);
}

afterEach(() => {
  cleanup();
  presentation.developerSurfacesVisible = true;
  presentation.isLoading = false;
});

describe("SettingsDialog", () => {
  it("offers every screen where the developer surfaces are visible", () => {
    renderDialog();

    const names = tabNames();
    for (const label of [
      sections.skills,
      sections.tools,
      sections.subagents,
      sections.integrations,
      sections.channels,
      sections.memory,
      sections.account,
    ]) {
      expect(names).toContain(label);
    }
  });

  it("keeps the deployment's screens for administrators only", () => {
    presentation.developerSurfacesVisible = false;

    renderDialog();

    const names = tabNames();
    for (const hidden of [
      sections.skills,
      sections.tools,
      sections.subagents,
      sections.integrations,
    ]) {
      expect(names).not.toContain(hidden);
    }
    // What is a person's own stays theirs.
    for (const kept of [sections.channels, sections.memory, sections.account]) {
      expect(names).toContain(kept);
    }
  });

  it("sends a deep link to a hidden screen somewhere it can go", () => {
    presentation.developerSurfacesVisible = false;

    renderDialog("skills");

    expect(tabNames()).not.toContain(sections.skills);
  });

  it("mounts nothing until the deployment has answered", () => {
    // The screens stay offered while the answer is unknown, so without this a
    // deep link to a hidden one would mount it, and fire its fetches, first.
    presentation.isLoading = true;

    renderDialog("skills");

    expect(screen.getByRole("status")).toBeTruthy();
  });
});
