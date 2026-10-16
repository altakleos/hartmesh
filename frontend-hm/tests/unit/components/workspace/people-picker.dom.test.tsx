import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

const state = rs.hoisted(() => ({
  mode: "exact",
  failed: false,
  user: {
    id: "reader",
    email: "reader@example.com",
    system_role: "user",
    permissions: ["agents:read"],
  },
}));
rs.mock("@/core/features", () => ({
  useFeatures: (select: (features: object) => unknown) => ({
    data: select({ people_search: state.mode }),
    isError: state.failed,
  }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: state.user }),
}));
rs.mock("@/core/people/api", () => ({ searchPeople: rs.fn() }));

import { PeoplePicker } from "@/components/workspace/people-picker";
import { I18nProvider } from "@/core/i18n/context";
import { searchPeople } from "@/core/people/api";

const lee = { id: "lee", email: "lee@example.com", name: "Lee", active: true };
const sam = { id: "sam", email: "sam@example.com", name: null, active: true };
const choose = rs.fn();
function tree(props: Partial<React.ComponentProps<typeof PeoplePicker>> = {}) {
  return (
    <I18nProvider initialLocale="en-US">
      <PeoplePicker onChange={choose} {...props} />
    </I18nProvider>
  );
}
beforeEach(() => {
  state.mode = "exact";
  state.failed = false;
  state.user = {
    id: "reader",
    email: "reader@example.com",
    system_role: "user",
    permissions: ["agents:read"],
  };
  choose.mockReset();
  rs.mocked(searchPeople)
    .mockReset()
    .mockResolvedValue({ users: [lee, sam] });
});
afterEach(() => {
  cleanup();
  rs.useRealTimers();
});

test("debounces prefix searches from two characters through the 64-character bound", async () => {
  state.mode = "prefix";
  rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  render(tree());
  const input = screen.getByRole("combobox");
  fireEvent.change(input, { target: { value: "l" } });
  await act(async () => {
    await rs.advanceTimersByTimeAsync(500);
  });
  expect(searchPeople).not.toHaveBeenCalled();
  fireEvent.change(input, { target: { value: "le" } });
  await act(async () => {
    await rs.advanceTimersByTimeAsync(299);
  });
  expect(searchPeople).not.toHaveBeenCalled();
  await act(async () => {
    await rs.advanceTimersByTimeAsync(1);
  });
  expect(searchPeople).toHaveBeenCalledTimes(1);
  expect(rs.mocked(searchPeople).mock.calls[0]![0]).toBe("le");
  fireEvent.change(input, { target: { value: "l".repeat(65) } });
  await act(async () => {
    await rs.advanceTimersByTimeAsync(500);
  });
  expect(searchPeople).toHaveBeenCalledTimes(1);
});

test("sends no exact search while typing and one on Find", async () => {
  render(tree());
  fireEvent.change(screen.getByRole("combobox"), {
    target: { value: "lee@example.com" },
  });
  expect(searchPeople).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Find" }));
  await screen.findByRole("option", { name: /Lee/ });
  expect(searchPeople).toHaveBeenCalledTimes(1);
  expect(rs.mocked(searchPeople).mock.calls[0]![0]).toBe("lee@example.com");
});

test("uses exact search when discovery is missing or failed", () => {
  state.mode = "unknown";
  state.failed = true;
  render(tree());
  expect(
    screen.getByText(
      "Type your colleague's full email address, then press Find.",
    ),
  ).toBeDefined();
  expect(screen.getByRole("button", { name: "Find" })).toBeDefined();
});

test("supports Find, arrow navigation and selection entirely from the keyboard", async () => {
  render(tree());
  const input = screen.getByRole("combobox");
  fireEvent.change(input, { target: { value: "lee@example.com" } });
  fireEvent.keyDown(input, { key: "Enter" });
  await screen.findByRole("option", { name: /Lee/ });
  fireEvent.keyDown(input, { key: "ArrowDown" });
  fireEvent.keyDown(input, { key: "ArrowDown" });
  fireEvent.keyDown(input, { key: "Enter" });
  expect(choose).toHaveBeenCalledWith(sam);
  expect(searchPeople).toHaveBeenCalledTimes(1);
});

test.each(["prefix", "exact"])(
  "shows the appropriate empty sentence for %s",
  async (mode) => {
    state.mode = mode;
    rs.mocked(searchPeople).mockResolvedValue({ users: [] });
    render(tree());
    fireEvent.change(screen.getByRole("combobox"), {
      target: { value: "lee@example.com" },
    });
    if (mode === "exact")
      fireEvent.click(screen.getByRole("button", { name: "Find" }));
    expect(
      await screen.findByText(
        mode === "prefix"
          ? "No colleague matches."
          : "No one at your company has an account with that email address. Check the spelling, or ask an administrator to add the person.",
      ),
    ).toBeDefined();
  },
);

test("keeps the query and shows localized copy after a failed search", async () => {
  rs.mocked(searchPeople).mockRejectedValue(new Error("private diagnostic"));
  render(tree());
  fireEvent.change(screen.getByRole("combobox"), {
    target: { value: "lee@example.com" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Find" }));
  expect(
    await screen.findByText("The search did not work. Try again."),
  ).toBeDefined();
  expect(screen.getByRole<HTMLInputElement>("combobox").value).toBe(
    "lee@example.com",
  );
  expect(screen.queryByText("private diagnostic")).toBeNull();
});

test("excludes the reader and disables a colleague who already has access", async () => {
  rs.mocked(searchPeople).mockResolvedValue({
    users: [{ ...lee, id: "reader" }, lee, sam],
  });
  render(tree({ existingIds: ["lee"] }));
  fireEvent.change(screen.getByRole("combobox"), {
    target: { value: "lee@example.com" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Find" }));
  const result = await screen.findByRole("option", {
    name: /Lee.*already has access/,
  });
  expect(result.getAttribute("aria-disabled")).toBe("true");
  fireEvent.click(result);
  expect(choose).not.toHaveBeenCalled();
  expect(screen.getAllByRole("option")).toHaveLength(2);
});

test("always offers You first in a supervisor picker, including after another person is chosen", async () => {
  const view = render(
    tree({
      includeSelf: true,
      value: {
        id: "reader",
        email: "reader@example.com",
        name: null,
        active: true,
      },
    }),
  );
  expect(
    screen.getByRole("option", { name: "You" }).getAttribute("aria-selected"),
  ).toBe("true");
  view.rerender(tree({ includeSelf: true, value: lee }));
  fireEvent.click(screen.getByRole("option", { name: "You" }));
  expect(choose).toHaveBeenCalledWith({
    id: "reader",
    email: "reader@example.com",
    name: null,
    active: true,
  });
});

test("retires an old search when the account changes and ignores its late result", async () => {
  let finish!: (value: Awaited<ReturnType<typeof searchPeople>>) => void;
  let signal: AbortSignal | undefined;
  rs.mocked(searchPeople).mockImplementationOnce((_query, requestSignal) => {
    signal = requestSignal;
    return new Promise((resolve) => {
      finish = resolve;
    });
  });
  const view = render(tree());
  fireEvent.change(screen.getByRole("combobox"), {
    target: { value: "lee@example.com" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Find" }));
  await waitFor(() => expect(searchPeople).toHaveBeenCalledTimes(1));
  state.user = { ...state.user, id: "next" };
  view.rerender(tree());
  expect(signal?.aborted).toBe(true);
  await act(async () => finish({ users: [lee] }));
  expect(screen.queryByRole("option", { name: /Lee/ })).toBeNull();
  expect(screen.getByRole<HTMLInputElement>("combobox").value).toBe("");
});
