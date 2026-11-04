import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
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
    user: { id: "reader", system_role: "admin", permissions: ["*"] },
  }),
}));
rs.mock("@/core/features", () => ({
  useBranding: () => ({
    providerName: "Provider",
    supportURL: "https://support.example/help",
  }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  useSupervisorName: () => "Casey",
  usePersonName: () => "Jordan",
}));
rs.mock("@/core/agent-instances/api", () => ({
  ...rs.requireActual<typeof api>("@/core/agent-instances/api"),
  getWork: rs.fn(),
  commandWork: rs.fn(),
  workHistory: rs.fn(),
}));
rs.mock("@/core/agent-instances/work-run", () => ({ readWorkRun: rs.fn() }));

import {
  WorkAttemptBox,
  type WorkAttemptBoxProps,
} from "@/components/workspace/instances/work-attempt-box";
import * as api from "@/core/agent-instances/api";
import { readWorkRun } from "@/core/agent-instances/work-run";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";

import { workInstance, workRecord } from "../../../fixtures/agent-work";

const changed = rs.fn();
const clients: QueryClient[] = [];
function options(): WorkAttemptBoxProps {
  return {
    instance: workInstance(),
    record: workRecord(),
    manager: true,
    collaborate: true,
    canStop: true,
    canResume: true,
    disabled: false,
    queryKey: ["work-test"],
    changed,
  };
}
function show(props = options()) {
  const client = new QueryClient();
  clients.push(client);
  const ui = (value: WorkAttemptBoxProps) => (
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <WorkAttemptBox {...value} />
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>
  );
  return { ...render(ui(props)), ui, client };
}
beforeEach(() => {
  changed.mockReset();
  rs.mocked(api.workHistory).mockReset().mockResolvedValue({ events: [] });
  rs.mocked(api.getWork)
    .mockReset()
    .mockImplementation(async () => workRecord());
  rs.mocked(api.commandWork)
    .mockReset()
    .mockImplementation(async () => {
      const value = workRecord();
      return { ...value, attempt: { ...value.attempt!, status: "stopping" } };
    });
  rs.mocked(readWorkRun).mockReset().mockResolvedValue({ stop_reason: "busy" });
});
afterEach(() => {
  cleanup();
  rs.useRealTimers();
  clients.splice(0).forEach((client) => client.clear());
});
test("Stop asks explicitly, preserves the captured revision and uses the shared Work command", async () => {
  const props = options();
  const view = show(props);
  expect(screen.getByText("Researcher's work: Working")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Stop" }));
  expect(screen.getByRole("dialog").textContent).toContain(
    "Stop Researcher working on this?",
  );
  view.rerender(
    view.ui({ ...props, record: { ...props.record, revision: 99 } }),
  );
  fireEvent.click(screen.getByRole("button", { name: "Stop now" }));
  await waitFor(() => expect(api.commandWork).toHaveBeenCalledTimes(1));
  expect(rs.mocked(api.commandWork).mock.calls[0]![2]).toMatchObject({
    action: "stop",
    expected_revision: 5,
    expected_assignment_revision: 2,
    attempt_id: props.record.attempt!.id,
  });
  await waitFor(() => expect(changed).toHaveBeenCalledTimes(1));
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(
    screen.getByRole("button", { name: "Stop" }).hasAttribute("disabled"),
  ).toBe(true);
  view.rerender(
    view.ui({
      ...props,
      record: {
        ...props.record,
        attempt: { ...props.record.attempt!, status: "cancelled" },
      },
    }),
  );
  expect(
    screen.queryByText("Stopping Researcher. This can take up to a minute."),
  ).toBeNull();
});
test("a colleague who did not start the assignment has no Stop, while its starter does", () => {
  const props = options();
  props.manager = false;
  props.record.attempt!.requester_id = "colleague";
  const view = show(props);
  expect(screen.queryByRole("button", { name: "Stop" })).toBeNull();
  props.record = {
    ...props.record,
    attempt: { ...props.record.attempt!, requester_id: "reader" },
  };
  view.rerender(view.ui(props));
  expect(screen.getByRole("button", { name: "Stop" })).toBeTruthy();
});
test("a failed start reads the exact run and offers a named fixed refusal without diagnostics", async () => {
  const props = options();
  props.record.attempt!.status = "failed";
  show(props);
  expect(
    await screen.findByText("Researcher's work: Did not start"),
  ).toBeTruthy();
  expect(readWorkRun).toHaveBeenCalledWith(
    "work-chat",
    "run",
    expect.any(AbortSignal),
  );
  expect(
    screen.getByText(
      "Too much is running at once, so Researcher could not start. Press Resume again in a minute.",
    ),
  ).toBeTruthy();
});
test("a contained completion report is labelled interrupted and keeps its candidate separate", () => {
  const props = options();
  props.record.attempt!.status = "failed";
  props.record.attempt!.contained = { reason: "outcome-uncertain", at: "now" };
  props.record.attempt!.candidate = {
    id: "outcome",
    statement: "Draft complete",
    evidence_revision: 1,
    sources: [],
  };
  show(props);
  expect(screen.getByText("Researcher's work: Interrupted")).toBeTruthy();
  expect(screen.getByText("Draft complete")).toBeTruthy();
  expect(screen.getByRole("link", { name: "Open files" })).toBeTruthy();
  expect(screen.queryByText("outcome-uncertain")).toBeNull();
  expect(readWorkRun).not.toHaveBeenCalled();
});
test("work ending without a result explains where to answer only on an open assignment without a request", () => {
  const props = options();
  props.record.attempt!.status = "succeeded";
  const view = show(props);
  expect(
    screen.getByText("Researcher's work: Ended without a result"),
  ).toBeTruthy();
  expect(
    screen.getByText(/Researcher stopped without reporting a result/),
  ).toBeTruthy();
  view.rerender(
    view.ui({
      ...props,
      record: {
        ...props.record,
        status: "blocked",
        human_input_request_id: "request",
      },
    }),
  );
  expect(
    screen.queryByText(/Researcher stopped without reporting a result/),
  ).toBeNull();
});
test("unknown stop outcomes offer only a manual exact retry", async () => {
  rs.mocked(api.commandWork).mockRejectedValueOnce(new TypeError("PRIVATE"));
  show();
  fireEvent.click(screen.getByRole("button", { name: "Stop" }));
  fireEvent.click(screen.getByRole("button", { name: "Stop now" }));
  await screen.findByText("The work could not be stopped. Try again.");
  expect(api.commandWork).toHaveBeenCalledTimes(1);
  const body = rs.mocked(api.commandWork).mock.calls[0]![2];
  fireEvent.click(screen.getByRole("button", { name: "Retry same request" }));
  await waitFor(() => expect(api.commandWork).toHaveBeenCalledTimes(2));
  expect(rs.mocked(api.commandWork).mock.calls[1]![2]).toEqual(body);
});
test.each([4, 5])(
  "the Stop control still requires Inspect when a caller passes manager with mask %s",
  (permissions) => {
    const props = options();
    props.instance.permissions = permissions;
    props.record.attempt!.requester_id = "colleague";
    show(props);
    expect(screen.queryByRole("button", { name: "Stop" })).toBeNull();
  },
);
test("the startup delay comes from the assignment update and retires when work begins", () => {
  rs.useFakeTimers();
  rs.setSystemTime(new Date("2026-10-16T08:00:00Z"));
  const props = options();
  props.record.updated_at = "2026-10-16T08:00:00Z";
  props.record.attempt!.workspace = "starting";
  const view = show(props);
  expect(screen.getByText("Starting Researcher's workspace…")).toBeTruthy();
  act(() => {
    rs.advanceTimersByTime(30_000);
  });
  expect(
    screen.getByText(
      "Still starting Researcher's workspace. After a quiet period this can take up to two minutes.",
    ),
  ).toBeTruthy();
  view.rerender(
    view.ui({
      ...props,
      record: {
        ...props.record,
        attempt: { ...props.record.attempt!, workspace: "ready" },
      },
    }),
  );
  expect(screen.queryByText(/Still starting/)).toBeNull();
  expect(screen.getByText("Researcher's work: Working")).toBeTruthy();
});
test("a cancelled assignment names the earliest stop in the current history bracket", async () => {
  const props = options();
  props.record.attempt!.status = "cancelled";
  function event(
    action: string,
    minute: number,
    report?: api.WorkEvent["report"],
  ): api.WorkEvent {
    return {
      id: action,
      actor_id: "person",
      actor_kind: "human",
      action,
      revision: minute,
      assignment_revision: 2,
      created_at: `2026-10-16T07:${minute}:00Z`,
      note: null,
      record: props.record,
      report,
    };
  }
  rs.mocked(api.workHistory).mockResolvedValue({
    events: [
      event("attempt_settled", 59, { attempt_id: props.record.attempt!.id }),
      event("cancel", 58),
      event("stop", 57),
      event("activate", 56),
    ],
  });
  show(props);
  await screen.findByText(/Stopped by Jordan at/);
  expect(screen.queryByText(/withdrew the assignment/)).toBeNull();
});
test("a retired conflict read cannot issue a follow-up Stop or update its old view", async () => {
  let resolve!: (value: api.WorkRecord) => void;
  const reading = new Promise<api.WorkRecord>((done) => {
    resolve = done;
  });
  rs.mocked(api.commandWork).mockRejectedValueOnce(
    new api.InstanceApiError(409, "PRIVATE", "revision-conflict"),
  );
  rs.mocked(api.getWork).mockReturnValue(reading);
  const view = show();
  fireEvent.click(screen.getByRole("button", { name: "Stop" }));
  fireEvent.click(screen.getByRole("button", { name: "Stop now" }));
  await waitFor(() => expect(api.getWork).toHaveBeenCalledTimes(1));
  const signal = rs.mocked(api.getWork).mock.calls[0]![2]!;
  view.unmount();
  expect(signal.aborted).toBe(true);
  await act(async () => {
    resolve({ ...workRecord(), revision: 6 });
  });
  expect(api.commandWork).toHaveBeenCalledTimes(1);
  expect(changed).not.toHaveBeenCalled();
});
