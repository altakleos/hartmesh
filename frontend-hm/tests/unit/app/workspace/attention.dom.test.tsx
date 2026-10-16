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
rs.mock("@/core/people/api", { spy: true });
rs.mock("@/core/spaces/api", () => ({
  listSpaces: rs.fn().mockResolvedValue({ spaces: [] }),
}));
import { RequestDetail } from "@/components/workspace/attention/inbox";
import * as workAPI from "@/core/agent-instances/api";
import * as api from "@/core/attention/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
import { getPeople } from "@/core/people/api";

const id = "a".repeat(32);
let current: api.InputRequest;
let responses: api.InputResponse[];
function show(executionUnavailable = false) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  const mounted = render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <RequestDetail id={id} executionUnavailable={executionUnavailable} />
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>,
  );
  return { client, ...mounted };
}
beforeEach(() => {
  rs.mocked(workAPI.getInstanceGrants)
    .mockReset()
    .mockResolvedValue({
      grants: [
        { user_id: "alice", permissions: 7 },
        { user_id: "bob", permissions: 3 },
        { user_id: "manager", permissions: 6 },
      ],
    });
  rs.mocked(getPeople)
    .mockReset()
    .mockImplementation(async (ids) => ({
      users: ids.map((id) => ({
        id,
        name: id === "bob" ? "Jordan" : "Casey",
        email: `${id}@example.test`,
        active: true,
      })),
    }));
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
  rs.mocked(api.getHistory)
    .mockReset()
    .mockResolvedValue({ events: [], has_more: false });
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
  await screen.findByRole("option", { name: "Jordan" });
  fireEvent.change(screen.getByLabelText("Who should answer"), {
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
  await waitFor(() => expect(button.hasAttribute("disabled")).toBe(true));
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
  const command = rs.mocked(workAPI.commandWork).mock.calls[0]?.[2];
  if (!command) throw new Error("Missing exact outcome acceptance command");
  expect(Reflect.get(command, "request_basis")).toEqual({
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
it("continues saving replies during an execution hold and shows a saved time without operation IDs", async () => {
  show(true);
  await screen.findByText(
    "You can still read requests and reply. Replies are kept until Documentation AI employee can work again.",
  );
  fireEvent.change(screen.getByLabelText("Response", { exact: true }), {
    target: { value: "Use this guide" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Supply response" }));
  await screen.findByText(/^Saved at .+\.$/);
  expect(api.respond).toHaveBeenCalledTimes(1);
  expect(workAPI.commandWork).not.toHaveBeenCalled();
  expect(screen.queryByText(/eeeeeeeeeeeeeeee/)).toBeNull();
});
it("read status appears only after reading and distinguishes a newer request revision", async () => {
  const view = show();
  await screen.findByRole("heading", { name: "Which guide?" });
  expect(screen.queryByText("You have read the latest version.")).toBeNull();
  current = { ...current, read_revision: current.revision };
  await act(() =>
    view.client.invalidateQueries({ queryKey: ["attention-detail"] }),
  );
  await screen.findByText("You have read the latest version.");
  current = { ...current, revision: current.revision + 1 };
  await act(() =>
    view.client.invalidateQueries({ queryKey: ["attention-detail"] }),
  );
  await screen.findByText("Changed since you read it.");
});
it("routes to nobody through the same named list and sends null without a separate clear action", async () => {
  show();
  await screen.findByLabelText("Who should answer");
  fireEvent.change(screen.getByLabelText("Who should answer"), {
    target: { value: "__none" },
  });
  const route = screen.getByRole("button", { name: "Route request" });
  await waitFor(() => expect(route.hasAttribute("disabled")).toBe(false));
  fireEvent.click(route);
  await waitFor(() => expect(api.command).toHaveBeenCalledTimes(1));
  expect(rs.mocked(api.command).mock.calls[0]![1]).toMatchObject({
    action: "route",
    recipient_id: null,
  });
  expect(screen.queryByRole("button", { name: "Clear recipient" })).toBeNull();
});
it("history uses the service name and event labels, while unavailable sources use their own sentence", async () => {
  current = {
    ...current,
    can_manage: false,
    sources: [{ kind: "unavailable" }],
  };
  rs.mocked(api.getHistory).mockResolvedValue({
    events: [
      {
        id: "event",
        actor_id: "PRIVATE-HOST-ID",
        actor_kind: "host",
        action: "respond",
        revision: 4,
        created_at: "2026-10-16T07:00:00Z",
      },
    ],
    has_more: false,
  });
  show();
  await screen.findByText("This file is no longer available to you.");
  fireEvent.click(screen.getByRole("button", { name: "Request history" }));
  await screen.findByText(/The service · Reply given/);
  expect(screen.queryByText(/PRIVATE-HOST-ID/)).toBeNull();
});
it("a deactivated response author is named without exposing the stored account ID", async () => {
  current = { ...current, can_manage: false };
  responses = [
    {
      id: "response",
      actor_id: "former",
      request_revision: 1,
      disposition: "supplied",
      text: "A fact",
      sources: [],
      created_at: "2026-10-16T07:00:00Z",
    },
  ];
  rs.mocked(getPeople).mockResolvedValue({
    users: [
      {
        id: "former",
        name: "Jordan",
        email: "jordan@example.test",
        active: false,
      },
    ],
  });
  show();
  await screen.findByText("Jordan (no longer active)");
  expect(screen.queryByText("former")).toBeNull();
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
  await screen.findByText("Request state: Closed (answered and checked)");
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
