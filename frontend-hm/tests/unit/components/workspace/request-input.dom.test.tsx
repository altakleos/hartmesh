import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: "reader", system_role: "user", permissions: ["*"] },
  }),
}));
rs.mock("@/core/people/api", () => ({ getPeople: rs.fn() }));
rs.mock("@/core/agent-instances/api", () => ({
  ...rs.requireActual<typeof api>("@/core/agent-instances/api"),
  getInstanceGrants: rs.fn(),
}));
rs.mock("@/core/attention/api", () => ({
  ...rs.requireActual<typeof attention>("@/core/attention/api"),
  createInput: rs.fn(),
}));

import { RequestInput } from "@/components/workspace/attention/create";
import * as api from "@/core/agent-instances/api";
import * as attention from "@/core/attention/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
import { getPeople } from "@/core/people/api";

import { workInstance, workRecord } from "../../../fixtures/agent-work";

const clients: QueryClient[] = [];
function show() {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <RequestInput
            instance={workInstance()}
            record={{ ...workRecord(), attempt: null }}
            manager
            disabled={false}
          />
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  rs.mocked(api.getInstanceGrants)
    .mockReset()
    .mockResolvedValue({
      grants: [
        { user_id: "supervisor", permissions: 7 },
        { user_id: "colleague", permissions: 3 },
      ],
    });
  rs.mocked(getPeople)
    .mockReset()
    .mockImplementation(async (ids) => ({
      users: ids.map((id) => ({
        id,
        name: id === "supervisor" ? "Casey" : "Jordan",
        email: `${id}@example.test`,
        active: true,
      })),
    }));
  rs.mocked(attention.createInput)
    .mockReset()
    .mockResolvedValue({} as attention.InputRequest);
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
test("creating a request defaults to its named supervisor and omits a recipient override", async () => {
  show();
  expect(api.getInstanceGrants).not.toHaveBeenCalled();
  fireEvent.click(
    screen.getByText("Request human input", { selector: "summary" }),
  );
  await screen.findByRole("option", { name: "Casey (supervisor)" });
  fireEvent.change(screen.getByLabelText("Question"), {
    target: { value: "Which guide?" },
  });
  fireEvent.change(screen.getByLabelText("Why it is needed"), {
    target: { value: "To check the source" },
  });
  fireEvent.change(screen.getByLabelText("Expected response"), {
    target: { value: "A filename" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Request human input" }));
  await waitFor(() => expect(attention.createInput).toHaveBeenCalledTimes(1));
  const body = rs.mocked(attention.createInput).mock.calls[0]![2];
  expect(body).not.toHaveProperty("recipient_id");
  expect(body).toMatchObject({
    purpose: "information",
    expected_work_revision: 5,
    expected_assignment_revision: 2,
  });
});
test("an explicitly selected eligible colleague is sent by ID while the UI shows a name", async () => {
  show();
  fireEvent.click(
    screen.getByText("Request human input", { selector: "summary" }),
  );
  await screen.findByRole("option", { name: "Jordan" });
  fireEvent.change(screen.getByLabelText("Who should answer"), {
    target: { value: "colleague" },
  });
  fireEvent.change(screen.getByLabelText("Question"), {
    target: { value: "Which guide?" },
  });
  fireEvent.change(screen.getByLabelText("Why it is needed"), {
    target: { value: "To check the source" },
  });
  fireEvent.change(screen.getByLabelText("Expected response"), {
    target: { value: "A filename" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Request human input" }));
  await waitFor(() => expect(attention.createInput).toHaveBeenCalledTimes(1));
  expect(rs.mocked(attention.createInput).mock.calls[0]![2].recipient_id).toBe(
    "colleague",
  );
  expect(screen.queryByText("colleague")).toBeNull();
});
