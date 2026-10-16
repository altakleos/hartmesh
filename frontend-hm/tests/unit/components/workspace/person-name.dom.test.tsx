import { afterEach, expect, test, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({ people: new Map(), locale: "en" }));
rs.mock("@/core/people/hooks", () => ({ usePeople: () => state.people }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: "reader" } }),
}));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: state.locale === "zh" ? zhCN : enUS }),
}));
import {
  PersonName,
  useSupervisorName,
} from "@/components/workspace/person-name";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";

afterEach(() => {
  cleanup();
  state.people.clear();
  state.locale = "en";
});
function Supervisor({ id }: { id: string }) {
  return <p>{useSupervisorName(id)}</p>;
}

test("distinguishes the reader's label from their name inside a sentence", () => {
  render(
    <>
      <PersonName id="reader" />
      <PersonName id="reader" inline />
    </>,
  );
  expect(screen.getByText("You")).toBeDefined();
  expect(screen.getByText("you")).toBeDefined();
});
test.each([
  [null, "lee@example.com"],
  ["Lee", "Lee"],
])("shows a directory name with email as fallback", (name, expected) => {
  state.people.set("lee", {
    status: "ready",
    person: { id: "lee", email: "lee@example.com", name, active: true },
  });
  render(<PersonName id="lee" />);
  expect(screen.getByText(expected)).toBeDefined();
});
test("distinguishes inactive and missing people from a failed lookup", () => {
  state.people.set("inactive", {
    status: "ready",
    person: {
      id: "inactive",
      email: "former@example.com",
      name: "Lee",
      active: false,
    },
  });
  state.people.set("missing", { status: "ready", person: null });
  state.people.set("failed", { status: "error", person: null });
  render(
    <>
      <PersonName id="inactive" />
      <PersonName id="missing" />
      <PersonName id="failed" />
    </>,
  );
  expect(screen.getByText("Lee (no longer active)")).toBeDefined();
  expect(screen.getByText("a former colleague")).toBeDefined();
  expect(screen.getByText("a colleague")).toBeDefined();
});
test.each(["reader", "inactive", "missing", "failed"])(
  "asks a manager rather than the unavailable supervisor %s",
  (id) => {
    state.people.set("inactive", {
      status: "ready",
      person: { name: "Lee", email: "lee@example.com", active: false },
    });
    state.people.set("missing", { status: "ready", person: null });
    state.people.set("failed", { status: "error", person: null });
    render(<Supervisor id={id} />);
    expect(screen.getByText("a manager")).toBeDefined();
  },
);
test("uses an active supervisor's name and follows the active locale", () => {
  state.people.set("lee", {
    status: "ready",
    person: { name: "Lee", email: "lee@example.com", active: true },
  });
  state.locale = "zh";
  render(
    <>
      <PersonName id="reader" />
      <Supervisor id="lee" />
    </>,
  );
  expect(screen.getByText("你")).toBeDefined();
  expect(screen.getByText("Lee")).toBeDefined();
});
