import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", system_role: "user", permissions: ["agents:read"] },
  }),
}));
rs.mock("@/core/attention/api", () => ({
  ...rs.requireActual<typeof api>("@/core/attention/api"),
  listInput: rs.fn(),
}));
import { AttentionInbox } from "@/components/workspace/attention/inbox";
import * as api from "@/core/attention/api";
import { I18nProvider } from "@/core/i18n/context";

import { inputRequest } from "../../../fixtures/attention";

const clients: QueryClient[] = [];
function show(executionUnavailable = false) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <AttentionInbox executionUnavailable={executionUnavailable} />
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  rs.mocked(api.listInput)
    .mockReset()
    .mockResolvedValue({
      requests: [],
      counts: { pending: 0, routing: 0, answered: 0 },
      has_more: false,
    });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("empty input and another empty view use different generic platform copy", async () => {
  show();
  await screen.findByText(
    "Nothing needs your input. When an AI assistant or AI employee needs a file, a fact or a decision from you, the request appears here.",
  );
  expect(
    screen.getByText(
      "A reply is saved and starts nothing. To let the AI assistant or AI employee continue, open the assignment and press Resume.",
    ),
  ).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Shared requests" }));
  await screen.findByText("No requests in this view.");
});
test("the list identifies both agents with AI badges without fetching their private details", async () => {
  rs.mocked(api.listInput).mockResolvedValue({
    requests: [
      inputRequest(),
      inputRequest({ id: "e".repeat(32), instance_name: "Helper" }),
    ],
    counts: { pending: 2, routing: 0, answered: 0 },
    has_more: false,
  });
  show();
  await screen.findByRole("button", { name: /Researcher · Review the guide/ });
  expect(screen.getAllByRole("img", { name: "AI" })).toHaveLength(2);
  expect(
    screen.getByRole("button", { name: /Helper · Review the guide/ }),
  ).toBeTruthy();
});
test("list errors use fixed load copy without raw diagnostics", async () => {
  rs.mocked(api.listInput).mockRejectedValue(new Error("PRIVATE DIAGNOSTIC"));
  show();
  await screen.findByText(
    "Requests could not be loaded. Check your connection and refresh.",
  );
  expect(screen.queryByText(/PRIVATE/)).toBeNull();
});
test("execution holds keep the list readable and explain that replies wait for either agent kind", async () => {
  show(true);
  await screen.findByText(
    "You can still read requests and reply. Replies are kept until AI assistants and AI employees can work again.",
  );
});
