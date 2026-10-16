import { afterEach, expect, test } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

import { AgentAvatar } from "@/components/workspace/instances/agent-avatar";
import { I18nProvider } from "@/core/i18n/context";

afterEach(cleanup);
const instance = {
  id: "a".repeat(32),
  name: "Research analyst",
  custody: "personal" as const,
};
function avatar(value = instance) {
  return (
    <I18nProvider initialLocale="en-US">
      <AgentAvatar instance={value} />
    </I18nProvider>
  );
}

test("marks an AI assistant with initials and an accessible AI badge", () => {
  render(avatar());
  expect(screen.getByText("RA")).toBeDefined();
  expect(screen.getByRole("img", { name: "AI assistant" }).textContent).toBe(
    "AI",
  );
});
test("always qualifies an AI employee's badge", () => {
  render(
    <I18nProvider initialLocale="en-US">
      <AgentAvatar instance={{ ...instance, custody: "company" }} />
    </I18nProvider>,
  );
  expect(screen.getByRole("img", { name: "AI employee" })).toBeDefined();
});
test("keeps the identity colour when the name changes", () => {
  const view = render(avatar());
  const original = screen.getByText("RA").parentElement?.getAttribute("class");
  view.rerender(avatar({ ...instance, name: "New brief" }));
  expect(screen.getByText("NB").parentElement?.getAttribute("class")).toBe(
    original,
  );
});
test("handles empty and single-word names without dropping the AI mark", () => {
  render(
    <I18nProvider initialLocale="en-US">
      <AgentAvatar instance={{ ...instance, name: "" }} />
      <AgentAvatar instance={{ ...instance, name: "Analyst" }} />
    </I18nProvider>,
  );
  expect(screen.getAllByRole("img", { name: "AI assistant" })).toHaveLength(2);
  expect(screen.getByText("A")).toBeDefined();
});
