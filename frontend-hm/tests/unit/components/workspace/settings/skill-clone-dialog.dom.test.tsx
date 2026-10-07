import { afterEach, describe, expect, it, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

import { SkillCloneDialog } from "@/components/workspace/settings/skill-clone-dialog";
import { enUS } from "@/core/i18n/locales/en-US";

const state = rs.hoisted(() => ({
  mutateAsync: rs.fn(),
  preview: rs.fn(),
  owner: "owner",
  sourceError: false,
}));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: enUS }) }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: state.owner, system_role: "user" } }),
}));
rs.mock("@/core/skills/hooks", () => ({
  useSkillCloneSources: () => ({
    data: [
      {
        source_id: "integrations:provider/office/helper",
        name: "helper",
        enabled: true,
      },
    ],
    isLoading: false,
    error: state.sourceError ? new Error("Source inventory unavailable") : null,
    isError: state.sourceError,
  }),
  useCloneSkill: () => ({ mutateAsync: state.mutateAsync, isPending: false }),
}));
rs.mock("@/core/skills/api", () => ({ previewSkillClone: state.preview }));

afterEach(() => {
  cleanup();
  state.mutateAsync.mockReset();
  state.preview.mockReset();
  state.owner = "owner";
  state.sourceError = false;
});

describe("private clone selection", () => {
  it("defaults to a distinct private name and requires an observed source revision", async () => {
    state.preview.mockResolvedValue({
      revision: "a".repeat(64),
      can_export: true,
      file_count: 2,
    });
    state.mutateAsync.mockResolvedValue({ success: true });
    render(<SkillCloneDialog open onOpenChange={() => undefined} />);
    fireEvent.change(screen.getByLabelText("Provided skill"), {
      target: { value: "integrations:provider/office/helper" },
    });
    await waitFor(() =>
      expect(
        screen.getByRole<HTMLButtonElement>("button", {
          name: "Create private copy",
        }).disabled,
      ).toBe(false),
    );
    expect(screen.getByLabelText<HTMLInputElement>("Private name").value).toBe(
      "helper-private",
    );
    fireEvent.click(
      screen.getByRole("button", { name: "Create private copy" }),
    );
    await waitFor(() =>
      expect(state.mutateAsync).toHaveBeenCalledWith({
        source_id: "integrations:provider/office/helper",
        name: "helper-private",
        expected_revision: "a".repeat(64),
        allow_baseline_override: false,
      }),
    );
  });

  it("requires a separate override choice for the provided name", async () => {
    state.preview.mockResolvedValue({
      revision: "b".repeat(64),
      can_export: true,
      file_count: 2,
    });
    render(<SkillCloneDialog open onOpenChange={() => undefined} />);
    fireEvent.change(screen.getByLabelText("Provided skill"), {
      target: { value: "integrations:provider/office/helper" },
    });
    fireEvent.change(screen.getByLabelText("Private name"), {
      target: { value: "helper" },
    });
    await waitFor(() => expect(state.preview).toHaveBeenCalled());
    expect(
      screen.getByRole<HTMLButtonElement>("button", {
        name: "Create private copy",
      }).disabled,
    ).toBe(true);
    fireEvent.click(
      screen.getByLabelText("Override the provided name in my workspace"),
    );
    await waitFor(() =>
      expect(
        screen.getByRole<HTMLButtonElement>("button", {
          name: "Create private copy",
        }).disabled,
      ).toBe(false),
    );
  });
  it("retires a late source preview when the account changes", async () => {
    let resolvePreview: ((value: unknown) => void) | undefined;
    state.preview.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolvePreview = resolve;
        }),
    );
    const view = render(
      <SkillCloneDialog open onOpenChange={() => undefined} />,
    );
    fireEvent.change(screen.getByLabelText("Provided skill"), {
      target: { value: "integrations:provider/office/helper" },
    });
    await waitFor(() => expect(state.preview).toHaveBeenCalled());
    state.owner = "another-owner";
    view.rerender(<SkillCloneDialog open onOpenChange={() => undefined} />);
    resolvePreview?.({
      revision: "a".repeat(64),
      can_export: true,
      file_count: 2,
    });
    await waitFor(() =>
      expect(
        screen.getByLabelText<HTMLInputElement>("Private name").value,
      ).toBe(""),
    );
    expect(
      screen.getByRole<HTMLButtonElement>("button", {
        name: "Create private copy",
      }).disabled,
    ).toBe(true);
    expect(state.mutateAsync).not.toHaveBeenCalled();
  });

  it("closes copying on source refetch failure despite previously returned data", async () => {
    state.preview.mockResolvedValue({
      revision: "a".repeat(64),
      can_export: true,
      file_count: 2,
    });
    const view = render(
      <SkillCloneDialog open onOpenChange={() => undefined} />,
    );
    fireEvent.change(screen.getByLabelText("Provided skill"), {
      target: { value: "integrations:provider/office/helper" },
    });
    await waitFor(() =>
      expect(
        screen.getByRole<HTMLButtonElement>("button", {
          name: "Create private copy",
        }).disabled,
      ).toBe(false),
    );
    state.sourceError = true;
    view.rerender(<SkillCloneDialog open onOpenChange={() => undefined} />);
    expect(
      screen.getByRole<HTMLButtonElement>("button", {
        name: "Create private copy",
      }).disabled,
    ).toBe(true);
    expect(screen.getByRole("alert").textContent).toContain(
      "Source inventory unavailable",
    );
  });
});
