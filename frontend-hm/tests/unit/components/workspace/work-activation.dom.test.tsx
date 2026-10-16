import { afterEach, expect, test, rs } from "@rstest/core";
import { fireEvent, cleanup, render, screen } from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: "reader", system_role: "user" } }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  useSupervisorName: () => "Casey",
  usePersonName: () => "Jordan",
}));
rs.mock("@/core/features", () => ({
  useBranding: () => ({}),
  useStorage: () => ({ reason: null }),
}));
import {
  WorkActivation,
  WorkActivationRefusal,
  type WorkActivationProps,
} from "@/components/workspace/instances/work-activation";
import { I18nProvider } from "@/core/i18n/context";

import { workInstance, workRecord } from "../../../fixtures/agent-work";

function options(): WorkActivationProps {
  return {
    instance: workInstance(),
    record: { ...workRecord(), attempt: null },
    allowed: true,
    policyEnabled: true,
    pending: false,
    disabled: false,
    canCreateConversation: true,
    activate: rs.fn(),
  };
}
function ui(props: WorkActivationProps) {
  return (
    <I18nProvider initialLocale="en-US">
      <WorkActivation {...props} />
    </I18nProvider>
  );
}
afterEach(cleanup);
test.each([
  [
    "busy",
    "Too much is running at once, so Researcher could not start. Press Work on this again in a minute.",
  ],
  [
    "run-limit",
    "Researcher is already working in two conversations. Press Work on this when one of them finishes.",
  ],
  [
    "restarting",
    "Researcher's workspace is restarting. Press Work on this again in a few minutes.",
  ],
  [
    "storage-busy",
    "Researcher's files are being changed right now and cannot be used. Try again in a few minutes.",
  ],
  ["unknown-code", "That did not work. Refresh the page, then try again."],
])(
  "activation refusal %s selects fixed copy and no raw diagnostics",
  (reason, sentence) => {
    render(
      <I18nProvider initialLocale="en-US">
        <WorkActivationRefusal
          instance={workInstance()}
          refusal={{ reason, detail: "PRIVATE", extra: {} }}
          action="Work on this"
        />
      </I18nProvider>,
    );
    expect(screen.getByRole("alert").textContent).toBe(sentence);
    expect(screen.queryByText("PRIVATE")).toBeNull();
  },
);
test("a full-files refusal names the supervisor to a colleague and does not offer provider support to a non-admin", () => {
  render(
    <I18nProvider initialLocale="en-US">
      <WorkActivationRefusal
        instance={{ ...workInstance(), permissions: 3 }}
        refusal={{ reason: "files-full", extra: {} }}
        action="Resume"
      />
    </I18nProvider>,
  );
  expect(screen.getByRole("alert").textContent).toBe(
    "Researcher's files are full, so Researcher cannot start. Ask Casey to delete files, or ask an administrator for more room.",
  );
  expect(screen.queryByRole("link")).toBeNull();
});
test("paused and policy-disabled assignments keep a disabled activation button with its explanatory relationship", () => {
  const props = options();
  props.instance.status = "suspended";
  const view = render(ui(props));
  const button = screen.getByRole("button", { name: "Work on this" });
  expect(button.hasAttribute("disabled")).toBe(true);
  expect(
    document.getElementById(button.getAttribute("aria-describedby")!)
      ?.textContent,
  ).toBe(
    "Researcher is paused. A manager can resume Researcher on Researcher's Settings tab.",
  );
  view.rerender(
    ui({
      ...props,
      instance: { ...props.instance, status: "active" },
      policyEnabled: false,
    }),
  );
  expect(
    screen
      .getByRole("button", { name: "Work on this" })
      .hasAttribute("disabled"),
  ).toBe(true);
  expect(
    screen.getByText(
      "Assignments are switched off for Researcher. On Researcher's Settings tab a manager can switch them on. Assignments saved earlier stay readable.",
    ),
  ).toBeTruthy();
});
test("capacity names visible running work and announces a freed slot until the person starts", () => {
  const props = options();
  props.capacity = {
    max: 1,
    in_use: 1,
    running: [
      {
        instance_id: "other",
        instance_name: "Reviewer",
        work_id: "work",
        objective: "Check guide",
        started_at: "2026-10-16T07:00:00Z",
        requester_id: "colleague",
      },
    ],
  };
  const view = render(ui(props));
  expect(
    screen.getByText(/Reviewer is working on “Check guide”.*by Jordan/),
  ).toBeTruthy();
  expect(
    screen
      .getByRole("button", { name: "Work on this" })
      .hasAttribute("disabled"),
  ).toBe(true);
  view.rerender(
    ui({ ...props, capacity: { ...props.capacity, in_use: 0, running: [] } }),
  );
  expect(
    screen.getByText("A place is free. Researcher can start now."),
  ).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Work on this" }));
  expect(props.activate).toHaveBeenCalledWith(false);
  expect(
    screen.queryByText("A place is free. Researcher can start now."),
  ).toBeNull();
});
test("only permissions, ceilings, or an unresolved or settled assignment remove the control", () => {
  const props = options();
  const view = render(ui({ ...props, allowed: false }));
  expect(screen.queryByRole("button")).toBeNull();
  view.rerender(ui({ ...props, record: workRecord() }));
  expect(screen.queryByRole("button")).toBeNull();
  view.rerender(
    ui({ ...props, record: { ...props.record, status: "completed" } }),
  );
  expect(screen.queryByRole("button")).toBeNull();
});
