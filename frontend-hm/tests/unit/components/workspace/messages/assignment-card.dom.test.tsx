import { afterEach, describe, expect, it } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

import { AssignmentCard } from "@/components/workspace/messages/assignment-card";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";
import type { AgentTarget } from "@/core/telegram-bots/web-pages";

const draft = {
  objective: "Review <the> guide",
  success_criteria: "Report verified gaps",
  due_text: null,
  due_at: null,
};
const target: AgentTarget = {
  kind: "agent",
  id: "a".repeat(32),
  name: "Analyst",
  permissions: 3,
};
function draw(
  changes = {},
  selected: AgentTarget = target,
  locale: "en-US" | "zh-CN" = "en-US",
) {
  return render(
    <I18nContext.Provider
      value={{
        locale,
        setLocale: () => undefined,
        t: locale === "en-US" ? enUS : zhCN,
      }}
    >
      <AssignmentCard draft={{ ...draft, ...changes }} target={selected} />
    </I18nContext.Provider>,
  );
}
afterEach(cleanup);

describe("read-only assignment proposal", () => {
  it("shows complete plain-text details and a Work link without mutation controls", () => {
    draw();
    expect(screen.getByTestId("assignment-card").textContent).toContain(
      "Assignment proposed by Analyst",
    );
    expect(screen.getByText(draft.objective)).toBeTruthy();
    expect(screen.getByText(draft.success_criteria)).toBeTruthy();
    expect(
      screen.getByRole("link", { name: "Work" }).getAttribute("href"),
    ).toBe(`/workspace/instances?instance=${"a".repeat(32)}&tab=work`);
    expect(screen.queryAllByRole("button")).toHaveLength(0);
    expect(screen.queryByText("Deadline mentioned")).toBeNull();
    expect(document.querySelector("the")).toBeNull();
    expect(screen.getByText(/saved and started nothing/)).toBeTruthy();
  });
  it("uses mentioned words ahead of the optional date and omits missing deadlines", () => {
    draw({ due_text: "Friday afternoon", due_at: "2026-12-04T18:00:00-05:00" });
    expect(screen.getByText("Deadline mentioned")).toBeTruthy();
    expect(screen.getByText("Friday afternoon")).toBeTruthy();
  });
  it("formats an explicit date when words are absent", () => {
    const due = "2026-12-04T18:00:00-05:00";
    draw({ due_at: due });
    expect(
      screen.getByText(new Date(due).toLocaleString("en-US")),
    ).toBeTruthy();
  });
  it("renders the translated proposal without model text as markup", () => {
    draw({}, target, "zh-CN");
    expect(screen.getByTestId("assignment-card").textContent).toContain(
      "Analyst",
    );
    expect(screen.queryByText("Deadline mentioned")).toBeNull();
    expect(screen.getByText(draft.objective)).toBeTruthy();
  });
  it("draws no card outside an assistant or employee conversation", () => {
    draw({}, { kind: "chat" });
    expect(screen.queryByTestId("assignment-card")).toBeNull();
    cleanup();
    draw({}, { kind: "custom_agent", name: "custom" });
    expect(screen.queryByTestId("assignment-card")).toBeNull();
  });
});
