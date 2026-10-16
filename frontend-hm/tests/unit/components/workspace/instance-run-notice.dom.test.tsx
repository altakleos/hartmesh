import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({
  role: "user",
  supportURL: "https://support.example/help",
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { system_role: state.role } }),
}));
rs.mock("@/core/features", () => ({
  useBranding: () => ({
    providerName: "Provider",
    supportURL: state.supportURL,
  }),
  useStorage: () => ({ reason: null }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  useSupervisorName: () => "Casey",
}));

import { InstanceRunNotice } from "@/components/workspace/chats/instance-run-notice";
import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import type { Refusal } from "@/core/agent-instances/errors";
import { I18nProvider } from "@/core/i18n/context";

const binding = {
  id: "a".repeat(32),
  name: "Researcher",
  requester_id: "reader",
  custody: "personal" as const,
  status: "active" as const,
};
function detail(permissions: number): AgentInstanceDetail {
  return {
    ...binding,
    permissions,
    principal: { kind: "nonhuman", subject_id: "ai" },
    owner_id: "reader",
    creator_id: "reader",
    supervisor: { kind: "human", subject_id: "supervisor" },
    definition_revision: "revision",
    generation: 1,
    home_id: "b".repeat(32),
    activity: {
      running_runs: 0,
      running: [],
      last_active_at: null,
      open_conversations: 1,
    },
    home: { state: "available", workspace: "absent" },
    execution: { execution: "ready", reason: null },
  };
}
function show(refusal: Partial<Refusal>, permissions = 7) {
  return render(
    <I18nProvider initialLocale="en-US">
      <InstanceRunNotice
        binding={binding}
        detail={detail(permissions)}
        refusal={{ detail: "PRIVATE DIAGNOSTIC", extra: {}, ...refusal }}
      />
    </I18nProvider>,
  );
}
beforeEach(() => {
  state.role = "user";
  state.supportURL = "https://support.example/help";
});
afterEach(cleanup);
test.each([
  [
    { name: "ResidentRunLimit" },
    "Researcher is already working in two conversations. Send your message again when one of them finishes.",
  ],
  [
    { name: "ResidentFilesBusy" },
    "Researcher's files are being changed right now and cannot be used. Try again in a few minutes.",
  ],
  [
    { name: "ResidentInterrupted", reason: "lifecycle" },
    "A manager changed Researcher's settings, which restarted Researcher's workspace and stopped this reply. Nothing saved in Researcher's files was lost. Send your message again to continue.",
  ],
  [
    { name: "BackingUnavailable" },
    "Researcher's workspace was restarted, which stopped this reply. Send your message again.",
  ],
])("known frame selects fixed localized copy: %j", (refusal, sentence) => {
  show(refusal);
  expect(screen.getByRole("alert").textContent).toBe(sentence);
  expect(screen.queryByText(/PRIVATE/)).toBeNull();
});
test("full-file notice distinguishes a manager from a colleague and never offers a non-admin support link", () => {
  const view = show({ reason: "files-full" }, 3);
  expect(screen.getByRole("alert").textContent).toContain(
    "Ask Casey to delete files",
  );
  expect(screen.queryByRole("link")).toBeNull();
  view.unmount();
  show({ reason: "files-full" }, 6);
  expect(screen.getByRole("alert").textContent).toContain(
    "Delete files on Researcher's Files tab",
  );
});
test("an administrator receives only the validated configured support link", () => {
  state.role = "admin";
  const view = show({ name: "ResidentStartFailed" });
  const link = screen.getByRole("link");
  expect(link.getAttribute("href")).toBe(state.supportURL);
  expect(link.getAttribute("referrerpolicy")).toBe("no-referrer");
  view.unmount();
  state.supportURL = "http://unsafe.example/help";
  show({ name: "ResidentStartFailed" });
  expect(screen.queryByRole("link")).toBeNull();
});
test("execution refusals use the execution notice, and manager-only refusals remain on the network card", () => {
  const view = show({
    reason: "execution-unavailable",
    executionReason: "operator-hold",
  });
  expect(screen.getByRole("alert").textContent).toContain("paused");
  expect(screen.queryByText(/PRIVATE/)).toBeNull();
  view.unmount();
  show({ reason: "managers-only" });
  expect(screen.queryByRole("alert")).toBeNull();
});
