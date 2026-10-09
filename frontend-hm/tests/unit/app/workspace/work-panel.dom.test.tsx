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

const state = rs.hoisted(() => ({ user: "alice" }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: state.user,
      system_role: "user",
      permissions: ["agents:read", "agents:write"],
    },
  }),
}));
rs.mock("@/core/agent-instances/api", { spy: true });
rs.mock("@/core/spaces/api", () => ({
  listSpaces: rs.fn().mockResolvedValue({ spaces: [] }),
}));
import { WorkPanel } from "@/components/workspace/instances/work-panel";
import * as api from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";

const instance: api.AgentInstance = {
  id: "a".repeat(32),
  name: "Documentation AI employee",
  principal: { kind: "nonhuman", subject_id: "agent:" + "a".repeat(32) },
  custody: "company",
  owner_id: null,
  creator_id: "alice",
  supervisor: { kind: "human", subject_id: "alice" },
  definition_revision: "d".repeat(64),
  home_id: "b".repeat(32),
  status: "active",
  generation: 1,
  permissions: 7,
};
function record(): api.WorkRecord {
  return {
    id: "c".repeat(32),
    instance_id: instance.id,
    creator_id: "alice",
    definition_revision: instance.definition_revision,
    objective: "Review the guide",
    success_criteria: "Explain the gaps",
    priority: "normal",
    review_required: true,
    assignment_revision: 1,
    revision: 1,
    status: "open",
    progress: "",
    next_action: "",
    sources: [],
    review_state: "none",
    outcome: null,
    review: null,
    blocker: null,
    attempt: null,
    execution_available: false,
    availability: "records_only",
    work_enabled: true,
    needs_mandate_reconciliation: false,
    current_contents: "not_checked",
    created_at: "2026-10-09T00:00:00Z",
    updated_at: "2026-10-09T00:00:00Z",
  };
}
function show(permissions = 7, canWrite = true, enabled = true) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const ui = () => (
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <WorkPanel
            instance={{ ...instance, permissions }}
            policy={{ enabled, review_required: true }}
            canRead
            canWrite={canWrite}
            lifecyclePending={false}
          />
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>
  );
  return { ...render(ui()), ui, client };
}
beforeEach(() => {
  state.user = "alice";
  rs.mocked(api.listWork).mockReset().mockResolvedValue({ work: [] });
  rs.mocked(api.getWork)
    .mockReset()
    .mockImplementation(async () => record());
  rs.mocked(api.workHistory).mockReset().mockResolvedValue({ events: [] });
  rs.mocked(api.delegateWork).mockReset().mockResolvedValue(record());
  rs.mocked(api.commandWork).mockReset().mockResolvedValue(record());
});
afterEach(cleanup);
it("explains shared visibility before Use-only humans can compose or read Work", () => {
  show(1);
  expect(
    screen.getByText(/Tracked delegation requires Use and Inspect/),
  ).toBeTruthy();
  expect(screen.queryByLabelText("Objective")).toBeNull();
  expect(api.listWork).not.toHaveBeenCalled();
});
it("delegates records without claiming execution and retries an uncertain response exactly", async () => {
  rs.mocked(api.delegateWork).mockRejectedValueOnce(new Error("response lost"));
  show(3);
  expect(
    screen.getByText(/AI employee execution is not available yet/),
  ).toBeTruthy();
  expect(screen.queryByRole("button", { name: /^(Run|Resume)$/ })).toBeNull();
  expect(screen.queryByLabelText("Priority")).toBeNull();
  fireEvent.change(screen.getByLabelText("Objective"), {
    target: { value: "Review the guide" },
  });
  fireEvent.change(screen.getByLabelText("Success criteria"), {
    target: { value: "Explain the gaps" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Delegate Work" }));
  fireEvent.click(
    await screen.findByRole("button", { name: "Retry same request" }),
  );
  await waitFor(() => expect(api.delegateWork).toHaveBeenCalledTimes(2));
  expect(rs.mocked(api.delegateWork).mock.calls[0]?.[1]).toEqual(
    rs.mocked(api.delegateWork).mock.calls[1]?.[1],
  );
  expect(rs.mocked(api.delegateWork).mock.calls[0]?.[1]).not.toHaveProperty(
    "priority",
  );
  expect(api.commandWork).not.toHaveBeenCalled();
});
it("retains history when policy is disabled and withholds mutations at the provider ceiling", async () => {
  rs.mocked(api.listWork).mockResolvedValue({ work: [record()] });
  show(7, false, false);
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Review the guide · Open · Normal",
    }),
  );
  expect(await screen.findByText("Explain the gaps")).toBeTruthy();
  expect(screen.queryByRole("button", { name: "Delegate Work" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Edit assignment" })).toBeNull();
  fireEvent.click(screen.getByText("History"));
  await waitFor(() => expect(api.workHistory).toHaveBeenCalled());
});
it("sends the displayed row and assignment revisions for manager commands", async () => {
  rs.mocked(api.listWork).mockResolvedValue({ work: [record()] });
  show();
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Review the guide · Open · Normal",
    }),
  );
  fireEvent.click(await screen.findByRole("button", { name: "Cancel Work" }));
  await waitFor(() => expect(api.commandWork).toHaveBeenCalledTimes(1));
  expect(rs.mocked(api.commandWork).mock.calls[0]?.[2]).toMatchObject({
    action: "cancel",
    expected_revision: 1,
    expected_assignment_revision: 1,
  });
});
it("retires a pending delegation when the account changes", async () => {
  let resolve!: (value: api.WorkRecord) => void;
  rs.mocked(api.delegateWork).mockImplementationOnce(
    () =>
      new Promise((r) => {
        resolve = r;
      }),
  );
  const view = show();
  fireEvent.change(screen.getByLabelText("Objective"), {
    target: { value: "Private draft" },
  });
  fireEvent.change(screen.getByLabelText("Success criteria"), {
    target: { value: "Current account" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Delegate Work" }));
  await waitFor(() => expect(api.delegateWork).toHaveBeenCalledTimes(1));
  const signal = rs.mocked(api.delegateWork).mock.calls[0]?.[2];
  state.user = "bob";
  view.rerender(view.ui());
  await act(async () => resolve(record()));
  expect(signal?.aborted).toBe(true);
  expect(screen.queryByDisplayValue("Private draft")).toBeNull();
  expect(api.getWork).not.toHaveBeenCalled();
});
it("requires unchecked-content acknowledgement and invalidates it when the outcome changes", async () => {
  let current: api.WorkRecord = {
    ...record(),
    status: "submitted",
    review_state: "pending",
    outcome: {
      id: "e".repeat(32),
      statement: "Review completed",
      evidence_revision: 1,
      sources: [],
    },
  };
  rs.mocked(api.listWork).mockResolvedValue({ work: [current] });
  rs.mocked(api.getWork).mockImplementation(async () => current);
  const view = show();
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Review the guide · Ready for review · Normal",
    }),
  );
  const accept = await screen.findByRole("button", {
    name: "Accept outcome statement",
  });
  expect((accept as HTMLButtonElement).disabled).toBe(true);
  fireEvent.click(
    screen.getByLabelText(/I accept the recorded outcome statement/),
  );
  expect((accept as HTMLButtonElement).disabled).toBe(false);
  current = {
    ...current,
    revision: 2,
    outcome: { ...current.outcome!, id: "f".repeat(32), evidence_revision: 2 },
  };
  await act(async () => {
    await view.client.invalidateQueries({ queryKey: ["agent-work"] });
  });
  await waitFor(() =>
    expect((accept as HTMLButtonElement).disabled).toBe(true),
  );
  expect(api.commandWork).not.toHaveBeenCalled();
});

it("requires consent to replace unavailable sources and strips projection fields from retained references", async () => {
  const current: api.WorkRecord = {
    ...record(),
    sources: [
      {
        kind: "space_file",
        space_id: "b".repeat(32),
        path: "first.txt",
        current_contents: "not_checked",
      } as api.WorkSource,
      {
        kind: "space_file",
        space_id: "b".repeat(32),
        path: "second.txt",
        current_contents: "not_checked",
      } as api.WorkSource,
      { kind: "unavailable" },
    ],
  };
  rs.mocked(api.listWork).mockResolvedValue({ work: [current] });
  rs.mocked(api.getWork).mockResolvedValue(current);
  show();
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Review the guide · Open · Normal",
    }),
  );
  fireEvent.click(
    await screen.findByRole("button", { name: "Edit assignment" }),
  );
  const buttons = screen.getAllByRole("button", {
    name: "Remove reference",
    hidden: true,
  });
  expect((buttons[0] as HTMLButtonElement).disabled).toBe(true);
  fireEvent.click(screen.getByLabelText(/Replace the reference list/));
  fireEvent.click(buttons[0]!);
  fireEvent.click(screen.getByRole("button", { name: "Save assignment" }));
  await waitFor(() => expect(api.commandWork).toHaveBeenCalledTimes(1));
  expect(rs.mocked(api.commandWork).mock.calls[0]?.[2].sources).toEqual([
    { kind: "space_file", space_id: "b".repeat(32), path: "second.txt" },
  ]);
});

it("lets a collaborator discard a stale note and answer the current blocker", async () => {
  let current: api.WorkRecord = {
    ...record(),
    status: "blocked",
    blocker: {
      id: "e".repeat(32),
      revision: 1,
      kind: "information",
      question: "Which guide?",
    },
  };
  rs.mocked(api.listWork).mockResolvedValue({ work: [current] });
  rs.mocked(api.getWork).mockImplementation(async () => current);
  const view = show(3);
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Review the guide · Blocked · Normal",
    }),
  );
  fireEvent.change(await screen.findByLabelText("Note or response"), {
    target: { value: "An old response" },
  });
  current = {
    ...current,
    revision: 2,
    blocker: {
      ...current.blocker!,
      revision: 2,
      question: "Which newer guide?",
    },
  };
  await act(async () => {
    await view.client.invalidateQueries({ queryKey: ["agent-work"] });
  });
  fireEvent.click(
    await screen.findByRole("button", {
      name: "Reset response for current record",
    }),
  );
  fireEvent.change(screen.getByLabelText("Note or response"), {
    target: { value: "Use the newer guide" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Supply factual input" }));
  await waitFor(() => expect(api.commandWork).toHaveBeenCalledTimes(1));
  expect(rs.mocked(api.commandWork).mock.calls[0]?.[2]).toMatchObject({
    expected_revision: 2,
    blocker_revision: 2,
    note: "Use the newer guide",
    action: "input",
  });
});
