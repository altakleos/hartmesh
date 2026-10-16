import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  grants: [
    { user_id: "starter", permissions: 3 },
    { user_id: "manager", permissions: 6 },
  ],
  purpose: "information",
  people: new Map(),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", system_role: "user", permissions: ["*"] },
  }),
}));
rs.mock("@/core/people/hooks", () => ({ usePeople: () => state.people }));
rs.mock("@/core/agent-instances/api", () => ({
  ...rs.requireActual<typeof api>("@/core/agent-instances/api"),
  getInstanceGrants: rs.fn(),
}));
import { InputRecipientSelect } from "@/components/workspace/attention/recipient-select";
import * as api from "@/core/agent-instances/api";
import { I18nProvider } from "@/core/i18n/context";

const clients: QueryClient[] = [];
const validity = rs.fn();
function show(
  purpose: "information" | "decision" = "information",
  defaultLabel?: string,
  value = "",
) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <InputRecipientSelect
          instance={"a".repeat(32)}
          purpose={purpose}
          enabled
          value={value}
          choose={rs.fn()}
          defaultLabel={defaultLabel}
          validity={validity}
        />
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  validity.mockReset();
  state.people = new Map(
    ["starter", "manager"].map((id) => [
      id,
      {
        status: "ready",
        person: {
          id,
          name: id === "starter" ? "Jordan" : "Casey",
          email: `${id}@example.test`,
          active: true,
        },
      },
    ]),
  );
  rs.mocked(api.getInstanceGrants)
    .mockReset()
    .mockResolvedValue({ grants: state.grants });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("information recipients are named and exclude a manager who lacks Can chat", async () => {
  show();
  await screen.findByRole("option", { name: "Jordan" });
  expect(screen.queryByRole("option", { name: "Casey" })).toBeNull();
  expect(screen.queryByText("starter")).toBeNull();
});
test("decision recipients need both Inspect and Manage, and creating retains its default supervisor choice", async () => {
  show("decision", "Casey (supervisor)");
  await screen.findByRole("option", { name: "Casey" });
  expect(screen.queryByRole("option", { name: "Jordan" })).toBeNull();
  expect(
    screen.getByRole("option", { name: "Casey (supervisor)" }),
  ).toBeTruthy();
  await waitFor(() => expect(validity).toHaveBeenLastCalledWith(true));
});
test("a failed directory lookup changes the label while current grants still decide eligibility", async () => {
  state.people.set("starter", { status: "error", person: null });
  show("information", undefined, "starter");
  await screen.findByRole("option", { name: "a colleague" });
  await waitFor(() => expect(validity).toHaveBeenLastCalledWith(true));
  expect(screen.queryByText("starter")).toBeNull();
});
