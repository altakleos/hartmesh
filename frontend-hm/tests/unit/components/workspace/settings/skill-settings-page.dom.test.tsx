import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, waitFor } from "@testing-library/react";

import { SkillSettingsPage } from "@/components/workspace/settings/skill-settings-page";
import { enUS } from "@/core/i18n/locales/en-US";
import { SkillRequestError } from "@/core/skills/api";

const state = rs.hoisted(() => ({
  owner: "owner-a",
  upload: rs.fn(),
  success: rs.fn(),
  error: rs.fn(),
}));
rs.mock("sonner", () => ({
  toast: { success: state.success, error: state.error },
}));
rs.mock("next/navigation", () => ({ useRouter: () => ({ push: rs.fn() }) }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: state.owner, system_role: "user" } }),
}));
rs.mock("@/core/features/hooks", () => ({
  useCustomerAdministration: () => ({ localSkillManagement: true }),
}));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: enUS }) }));
rs.mock("@/core/skills/hooks", () => ({
  useSkills: () => ({ skills: [], isLoading: false, error: null }),
  useEnableSkill: () => ({ mutate: rs.fn() }),
  useUploadSkillArchive: () => ({
    mutateAsync: state.upload,
    isPending: false,
  }),
}));
rs.mock("@/components/workspace/settings/skill-clone-dialog", () => ({
  SkillCloneDialog: () => null,
}));
rs.mock("@/env", () => ({ env: { NEXT_PUBLIC_STATIC_WEBSITE_ONLY: "false" } }));

afterEach(() => {
  cleanup();
  state.owner = "owner-a";
  state.upload.mockReset();
  state.success.mockReset();
  state.error.mockReset();
});

describe("private archive upload lifetime", () => {
  for (const outcome of ["success", "scanner-error"] as const) {
    it(`retires the previous owner's ${outcome} after an account switch`, async () => {
      let resolve: ((value: unknown) => void) | undefined;
      let reject: ((error: unknown) => void) | undefined;
      state.upload.mockImplementation(
        () =>
          new Promise((done, fail) => {
            resolve = done;
            reject = fail;
          }),
      );
      const view = render(<SkillSettingsPage />);
      const input =
        view.container.querySelector<HTMLInputElement>('input[type="file"]');
      expect(input).not.toBeNull();
      fireEvent.change(input!, {
        target: { files: [new File(["archive"], "private.skill")] },
      });
      await waitFor(() => expect(state.upload).toHaveBeenCalled());
      state.owner = "owner-b";
      view.rerender(<SkillSettingsPage />);
      if (outcome === "success")
        resolve?.({ success: true, message: "Owner A private result" });
      else
        reject?.(
          new SkillRequestError(400, "Owner A scanner finding", {
            findings: [
              {
                rule_id: "fixture",
                severity: "HIGH",
                file: "private/SKILL.md",
                line: 1,
                message: "Owner A finding",
                remediation: null,
              },
            ],
          }),
        );
      await new Promise((done) => setTimeout(done, 0));
      expect(state.success).not.toHaveBeenCalled();
      expect(state.error).not.toHaveBeenCalled();
    });
  }
});
