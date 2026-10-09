import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "alice",
      system_role: "user",
      permissions: ["agents:read", "agents:write"],
    },
  }),
}));
rs.mock("@/core/attention/api", { spy: true });
rs.mock("@/core/agent-instances/api", { spy: true });
rs.mock("@/core/spaces/api", () => ({
  listSpaces: rs.fn().mockResolvedValue({ spaces: [] }),
}));
import { RequestDetail } from "@/components/workspace/attention/inbox";
import * as workAPI from "@/core/agent-instances/api";
import * as api from "@/core/attention/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";

const id = "a".repeat(32);
let current: api.InputRequest;
let responses: api.InputResponse[];
function show() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  const mounted = render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <RequestDetail id={id} />
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
  return { client, ...mounted };
}
beforeEach(() => {
  current = {
    id,
    instance_id: "b".repeat(32),
    work_id: "c".repeat(32),
    instance_name: "Documentation AI employee",
    work_objective: "Review guide",
    assignment_revision: 1,
    basis_id: "d".repeat(32),
    basis_revision: 1,
    revision: 1,
    request_revision: 1,
    purpose: "information",
    question: "Which guide?",
    reason: "Missing reference",
    expected_response: "A reference or explanation",
    choices: [],
    sources: [],
    state: "pending",
    closed_reason: null,
    recipient_id: "alice",
    creator_id: "bob",
    needs_routing: false,
    read_revision: 0,
    can_respond: true,
    can_recover_response: true,
    can_recover_management: true,
    can_manage: true,
    updated_at: "2026-10-09T00:00:00Z",
  };
  responses = [];
  rs.mocked(api.getInput)
    .mockReset()
    .mockImplementation(async () => ({ ...current }));
  rs.mocked(api.getResponses)
    .mockReset()
    .mockImplementation(async (_id, offset) => ({
      responses: offset === 0 ? [...responses] : [],
      has_more: false,
    }));
  rs.mocked(api.respond)
    .mockReset()
    .mockImplementation(async () => ({
      ...current,
      state: "answered",
      receipt: {
        operation_id: "e".repeat(32),
        response_id: "f".repeat(32),
        actor_id: "alice",
      },
    }));
  rs.mocked(api.command).mockReset().mockResolvedValue(current);
  rs.mocked(api.markRead).mockReset().mockResolvedValue({});
  rs.mocked(workAPI.getWork)
    .mockReset()
    .mockImplementation(
      async () =>
        ({
          id: current.work_id,
          instance_id: current.instance_id,
          revision: current.revision,
          assignment_revision: 1,
          outcome:
            current.purpose === "review"
              ? { statement: "Checked the guide", sources: [] }
              : null,
        }) as workAPI.WorkRecord,
    );
  rs.mocked(workAPI.commandWork)
    .mockReset()
    .mockResolvedValue({} as workAPI.WorkRecord);
});
afterEach(cleanup);
it("persists information without creating a Work command and retries exactly after uncertain acknowledgement", async () => {
  rs.mocked(api.respond).mockRejectedValueOnce(
    new Error("lost acknowledgement"),
  );
  show();
  fireEvent.change(await screen.findByLabelText("Response", { exact: true }), {
    target: { value: "Use the current guide" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Supply response" }));
  fireEvent.click(
    await screen.findByRole("button", { name: "Retry exact submission" }),
  );
  await waitFor(() => expect(api.respond).toHaveBeenCalledTimes(2));
  expect(rs.mocked(api.respond).mock.calls[0]?.[1]).toEqual(
    rs.mocked(api.respond).mock.calls[1]?.[1],
  );
  expect(workAPI.commandWork).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: /^(Run|Resume)$/ })).toBeNull();
});
it("keeps same-question concurrent replies eligible but retires rerouted drafts", async () => {
  const { client } = show();
  fireEvent.change(await screen.findByLabelText("Response", { exact: true }), {
    target: { value: "A fact" },
  });
  current = { ...current, revision: 2, state: "answered" };
  await act(() => client.invalidateQueries({ queryKey: ["attention-detail"] }));
  expect(
    screen
      .getByRole("button", {
        name: "Supply response",
      })
      .hasAttribute("disabled"),
  ).toBe(false);
  current = { ...current, revision: 3, request_revision: 2 };
  await act(() => client.invalidateQueries({ queryKey: ["attention-detail"] }));
  await screen.findByText(/The request changed/);
  expect(
    screen
      .getByLabelText("Response", { exact: true })
      .closest("fieldset")
      ?.hasAttribute("disabled"),
  ).toBe(true);
  expect(api.respond).not.toHaveBeenCalled();
});
it("never silently rebases a routing draft onto a concurrent reroute", async () => {
  const { client } = show();
  fireEvent.change(await screen.findByLabelText("Human recipient ID"), {
    target: { value: "bob" },
  });
  current = {
    ...current,
    revision: 2,
    request_revision: 2,
    recipient_id: "someone-else",
  };
  await act(() => client.invalidateQueries({ queryKey: ["attention-detail"] }));
  const button = screen.getByRole("button", {
    name: "Route request",
  });
  expect(button.hasAttribute("disabled")).toBe(true);
  fireEvent.click(button);
  expect(api.command).not.toHaveBeenCalled();
});
it("new responses invalidate review acknowledgement and exact response IDs bind acceptance", async () => {
  current = { ...current, purpose: "review" };
  const { client } = show();
  fireEvent.click(await screen.findByRole("checkbox"));
  current = { ...current, revision: 2 };
  responses = [
    {
      id: "e".repeat(32),
      actor_id: "bob",
      request_revision: 1,
      disposition: "supplied",
      text: "Additional context",
      sources: [],
      created_at: current.updated_at,
    },
  ];
  await act(() => client.invalidateQueries({ queryKey: ["attention-detail"] }));
  expect(
    screen
      .getByRole("button", {
        name: "Accept outcome statement",
      })
      .hasAttribute("disabled"),
  ).toBe(true);
  fireEvent.click(screen.getByRole("checkbox"));
  fireEvent.click(
    screen.getByRole("button", { name: "Accept outcome statement" }),
  );
  await waitFor(() => expect(workAPI.commandWork).toHaveBeenCalledTimes(1));
  expect(
    rs.mocked(workAPI.commandWork).mock.calls[0]?.[2].request_basis,
  ).toEqual({
    id,
    revision: 2,
    request_revision: 1,
    response_ids: ["e".repeat(32)],
  });
});
it("hides revoked title and response controls after revalidation fails", async () => {
  const { client } = show();
  await screen.findByRole("heading", { name: "Which guide?" });
  rs.mocked(api.getInput).mockRejectedValue(
    new workAPI.InstanceApiError(404, "Unavailable"),
  );
  await act(() => client.invalidateQueries({ queryKey: ["attention-detail"] }));
  expect(screen.queryByRole("heading", { name: "Which guide?" })).toBeNull();
  expect(screen.queryByLabelText("Response", { exact: true })).toBeNull();
});
it("read state is a separate mutation and cannot approve or respond", async () => {
  show();
  fireEvent.click(await screen.findByRole("button", { name: "Mark read" }));
  await waitFor(() => expect(api.markRead).toHaveBeenCalledTimes(1));
  expect(api.respond).not.toHaveBeenCalled();
  expect(workAPI.commandWork).not.toHaveBeenCalled();
});
it("unmount aborts a pending response and ignores its late receipt", async () => {
  let resolve!: (value: api.InputRequest) => void;
  rs.mocked(api.respond).mockImplementation(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const { unmount } = show();
  fireEvent.change(await screen.findByLabelText("Response", { exact: true }), {
    target: { value: "A fact" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Supply response" }));
  await waitFor(() => expect(api.respond).toHaveBeenCalledTimes(1));
  const signal = rs.mocked(api.respond).mock.calls[0]?.[2];
  unmount();
  expect(signal?.aborted).toBe(true);
  await act(async () => resolve(current));
});

it("recovers the exact acceptance receipt after a lost acknowledgement and closed-state poll", async () => {
  current = { ...current, purpose: "review" };
  rs.mocked(workAPI.commandWork).mockRejectedValueOnce(new Error("ack lost"));
  const { client } = show();
  fireEvent.click(await screen.findByRole("checkbox"));
  fireEvent.click(
    screen.getByRole("button", { name: "Accept outcome statement" }),
  );
  await screen.findByRole("button", { name: "Retry exact submission" });
  current = {
    ...current,
    revision: 2,
    state: "closed",
    closed_reason: "resolved",
    can_respond: false,
    can_manage: false,
  };
  await act(() => client.invalidateQueries({ queryKey: ["attention-detail"] }));
  await screen.findByText("Request state: Closed (resolved)");
  expect(
    screen.queryByRole("button", { name: "Accept outcome statement" }),
  ).toBeNull();
  fireEvent.click(
    screen.getByRole("button", { name: "Retry exact submission" }),
  );
  await waitFor(() => expect(workAPI.commandWork).toHaveBeenCalledTimes(2));
  expect(rs.mocked(workAPI.commandWork).mock.calls[0]?.[2]).toEqual(
    rs.mocked(workAPI.commandWork).mock.calls[1]?.[2],
  );
});
