import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", system_role: "user", permissions: ["*"] },
  }),
}));
rs.mock("@/core/people/api", () => ({ getPeople: rs.fn() }));
rs.mock("@/core/agent-instances/api", () => ({
  ...rs.requireActual<typeof api>("@/core/agent-instances/api"),
  getInstance: rs.fn(),
}));
import { ActorName } from "@/components/workspace/actor-name";
import * as api from "@/core/agent-instances/api";
import { I18nProvider } from "@/core/i18n/context";
import { getPeople } from "@/core/people/api";

import { workInstance } from "../../../fixtures/agent-work";

const clients: QueryClient[] = [];
function show(id: string, kind?: "human" | "nonhuman" | "host", known = true) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <ActorName
          id={id}
          kind={kind}
          instance={known ? workInstance() : undefined}
        />
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  rs.mocked(getPeople).mockReset().mockResolvedValue({ users: [] });
  rs.mocked(api.getInstance).mockReset().mockResolvedValue(workInstance());
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("the service and current agent show names and the AI badge without a directory request", () => {
  const view = show("private-host-id", "host");
  expect(screen.getByText("The service")).toBeTruthy();
  view.unmount();
  show(`agent:${workInstance().id}`, "nonhuman");
  expect(screen.getByText("Researcher")).toBeTruthy();
  expect(screen.getByRole("img", { name: "AI employee" })).toBeTruthy();
  expect(api.getInstance).not.toHaveBeenCalled();
  expect(getPeople).not.toHaveBeenCalled();
});
test("an inactive human and an id missing from the directory use the people presentation", async () => {
  rs.mocked(getPeople).mockResolvedValueOnce({
    users: [
      {
        id: "former-id",
        name: "Jordan",
        email: "jordan@example.test",
        active: false,
      },
    ],
  });
  const view = show("former-id", "human");
  await screen.findByText("Jordan (no longer active)");
  view.unmount();
  show("missing-id");
  await screen.findByText("a former colleague");
  expect(screen.queryByText("missing-id")).toBeNull();
});
test("an agent omitted from the current detail is read under current access and never prints its identifier", async () => {
  show(`agent:${workInstance().id}`, "nonhuman", false);
  await screen.findByText("Researcher");
  expect(api.getInstance).toHaveBeenCalledWith(
    workInstance().id,
    expect.any(AbortSignal),
  );
  expect(screen.queryByText(`agent:${workInstance().id}`)).toBeNull();
});
