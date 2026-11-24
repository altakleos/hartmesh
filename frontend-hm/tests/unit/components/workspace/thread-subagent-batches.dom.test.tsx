import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const featureState = rs.hoisted(() => ({
  repositoryAvailable: true,
  workerRunning: false,
}));
const batchState = rs.hoisted(() => ({
  batches: [] as Array<Record<string, unknown>>,
  control: rs.fn(),
  itemPages: [] as Array<Array<Record<string, unknown>>>,
  fetchNextPage: rs.fn(),
  hasNextPage: false,
}));

rs.mock("@/core/features", () => ({
  useSubagentBatchesCapability: () => ({
    ...featureState,
    maxRunning: 3,
    isLoading: false,
  }),
}));

rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({
    t: {
      common: { loading: "Loading", loadMore: "Load more" },
      subagentBatches: {
        jobState: { waiting: "Waiting", working: "Working", ended: "Ended" },
        jobReasons: {
          unavailable: "Unavailable",
          stopped_by_requester: "Stopped by you.",
          stopped_by_manager: "Stopped by a manager.",
        },
        conversationBusy: "This conversation is busy until the large job ends.",
        deadlineSummary: (count: number) =>
          `Time limit reached for ${count} checks.`,
        itemDeadline: "This check did not finish before the time limit.",
        unavailableNotice: (finished: number, total: number) =>
          `${finished} of ${total} checks finished`,
        finishedChecks: (finished: number, total: number) =>
          `${finished} of ${total} finished`,
        askAgain: "Ask again",
        switchedOff: "Off",
        openPanel: "Open Batches",
        dismiss: "Dismiss",
        label: "Batches",
        title: "Subagent batches",
        description: "Durable batch work",
        workerUnavailable:
          "The batch worker is not running. Historical batches are read-only.",
        empty: "No batches",
        emptyHint: "Submit a batch",
        loadFailed: "Load failed",
        pause: "Pause",
        resume: "Resume",
        cancel: "Cancel",
        retryItem: "Retry",
        exportResults: "Export JSONL",
        viewItems: "View items",
        hideItems: "Hide items",
        itemsFailed: "Items failed",
        progress: (completed: number, total: number) =>
          `${completed} of ${total}`,
        limits: (live: number, running: number) =>
          `Live ${live} running ${running}`,
        status: {
          queued: "Queued",
          running: "Running",
          paused: "Paused",
          completed: "Completed",
          failed: "Failed",
          cancelled: "Cancelled",
        },
      },
    },
  }),
}));

rs.mock("@/core/subagent-batches", () => ({
  completedSubagentBatchItems: () => 1,
  isActiveSubagentBatch: (batch: { status: string }) =>
    ["queued", "running", "paused"].includes(batch.status),
  subagentBatchProgress: () => 50,
  subagentBatchResultsUrl: () => "/results.jsonl",
  useControlSubagentBatch: () => ({
    isPending: false,
    variables: undefined,
    mutate: batchState.control,
  }),
  useRetrySubagentBatchItem: () => ({
    isPending: false,
    variables: undefined,
    mutate: rs.fn(),
  }),
  useSubagentBatchItems: () => ({
    data: { pages: batchState.itemPages },
    isLoading: false,
    isError: false,
    hasNextPage: batchState.hasNextPage,
    isFetchingNextPage: false,
    fetchNextPage: batchState.fetchNextPage,
  }),
  useSubagentBatches: () => ({
    data: batchState.batches,
    isLoading: false,
    isError: false,
  }),
}));

import { ThreadSubagentBatches } from "@/components/workspace/thread-subagent-batches";

const HISTORICAL_BATCH = {
  id: "batch-1",
  title: "Historical records",
  subagent_type: "general-purpose",
  status: "running",
  total_items: 2,
  max_live_items: 2,
  max_running_items: 1,
  max_attempts: 3,
  counts: {
    pending: 0,
    queued: 0,
    leased: 0,
    running: 1,
    succeeded: 1,
    failed: 0,
    cancelled: 0,
  },
  created_at: "2026-08-24T00:00:00Z",
  updated_at: "2026-08-24T00:01:00Z",
  completed_at: null,
};

afterEach(() => {
  cleanup();
  featureState.repositoryAvailable = true;
  featureState.workerRunning = false;
  batchState.batches = [];
  batchState.control.mockReset();
  batchState.itemPages = [];
  batchState.fetchNextPage.mockReset();
  batchState.hasNextPage = false;
});

describe("ThreadSubagentBatches capability gating", () => {
  it("keeps a read-only batch readable when the ordinary worker is running", async () => {
    featureState.workerRunning = true;
    batchState.batches = [{ ...HISTORICAL_BATCH, read_only: true }];
    batchState.itemPages = [
      [{ id: "one", item_key: "part", status: "failed" }],
    ];
    render(<ThreadSubagentBatches threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Batches" }));
    fireEvent.click(await screen.findByRole("button", { name: "View items" }));

    for (const name of ["Pause", "Cancel", "Retry"]) {
      expect(await screen.findByRole("button", { name })).toHaveProperty(
        "disabled",
        true,
      );
    }
    expect(
      screen.getByRole("link", { name: "Export JSONL" }).getAttribute("href"),
    ).toBe("/results.jsonl");
  });

  it("keeps historical batches readable when the worker is stopped", async () => {
    batchState.batches = [HISTORICAL_BATCH];
    render(<ThreadSubagentBatches threadId="thread-1" />);

    fireEvent.click(screen.getByRole("button", { name: "Batches" }));

    expect(
      await screen.findByText(
        "The batch worker is not running. Historical batches are read-only.",
      ),
    ).toBeDefined();
    expect(screen.getByRole("button", { name: "Pause" })).toHaveProperty(
      "disabled",
      true,
    );
    expect(screen.getByRole("button", { name: "Cancel" })).toHaveProperty(
      "disabled",
      true,
    );
    expect(
      screen.getByRole("link", { name: "Export JSONL" }).getAttribute("href"),
    ).toBe("/results.jsonl");
  });

  it("hides an unused batch surface when neither worker nor history exists", () => {
    render(<ThreadSubagentBatches threadId="thread-1" />);
    expect(screen.queryByRole("button", { name: "Batches" })).toBeNull();
  });

  it("loads the next page of batch items", async () => {
    batchState.batches = [HISTORICAL_BATCH];
    batchState.itemPages = [
      [
        {
          id: "item-1",
          item_key: "record-1",
          status: "succeeded",
          result_preview: "done",
        },
      ],
    ];
    batchState.hasNextPage = true;
    render(<ThreadSubagentBatches threadId="thread-1" />);

    fireEvent.click(screen.getByRole("button", { name: "Batches" }));
    fireEvent.click(screen.getByRole("button", { name: "View items" }));
    fireEvent.click(await screen.findByRole("button", { name: "Load more" }));

    expect(batchState.fetchNextPage).toHaveBeenCalledTimes(1);
  });
});

describe("agent conversation jobs", () => {
  it("keeps a colleague's empty drawer private when the worker is active", () => {
    featureState.workerRunning = true;
    render(<ThreadSubagentBatches threadId="other-chat" hideWhenEmpty />);
    expect(screen.queryByRole("button", { name: "Batches" })).toBeNull();
  });

  it("shows only the advertised Cancel and keeps item failures readable", async () => {
    featureState.workerRunning = true;
    batchState.batches = [
      {
        ...HISTORICAL_BATCH,
        read_only: true,
        follow_up: { state: "working", finished: 1, total: 2 },
        controls: ["cancel"],
      },
    ];
    batchState.itemPages = [
      [{ id: "one", item_key: "part", status: "failed" }],
    ];
    render(<ThreadSubagentBatches threadId="thread-1" />);
    fireEvent.click(screen.getByRole("button", { name: "Batches" }));
    fireEvent.click(await screen.findByRole("button", { name: "View items" }));
    expect(screen.queryByRole("button", { name: "Pause" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Retry" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(batchState.control).toHaveBeenCalledWith({
      batchId: "batch-1",
      action: "cancel",
    });
  });

  it.each([
    ["stopped_by_requester", "Stopped by you."],
    ["stopped_by_manager", "Stopped by a manager."],
  ])(
    "shows the deadline and stop actor when a large job ends (%s)",
    async (reason, copy) => {
      const mount = document.createElement("div");
      document.body.appendChild(mount);
      featureState.workerRunning = true;
      const working = {
        ...HISTORICAL_BATCH,
        follow_up: { state: "waiting", finished: 0, total: 2 },
      };
      batchState.batches = [working];
      const component = (
        <ThreadSubagentBatches
          accountId="starter"
          threadId="thread-1"
          noticeContainer={mount}
        />
      );
      const view = render(component);
      expect(mount.textContent).toContain("This conversation is busy");
      batchState.batches = [
        {
          ...working,
          status: "cancelled",
          follow_up: {
            state: "ended",
            reason,
            finished: 1,
            total: 2,
            timed_out: 1,
          },
        },
      ];
      view.rerender(
        <ThreadSubagentBatches
          accountId="starter"
          threadId="thread-1"
          noticeContainer={mount}
        />,
      );
      expect(mount.textContent).toBe("");
      fireEvent.click(screen.getByRole("button", { name: "Batches" }));
      expect(
        await screen.findByText(/Time limit reached for 1 checks/),
      ).toBeDefined();
      expect(screen.getByText(copy, { exact: false })).toBeDefined();
      mount.remove();
    },
  );

  it("shows only the newest unavailable job above the composer and remembers dismissal per account", async () => {
    const mount = document.createElement("div");
    document.body.appendChild(mount);
    const job = {
      ...HISTORICAL_BATCH,
      read_only: true,
      status: "cancelled",
      controls: [],
      follow_up: {
        state: "ended",
        reason: "unavailable",
        finished: 1,
        total: 2,
      },
    };
    batchState.batches = [job];
    const view = render(
      <ThreadSubagentBatches
        accountId="starter"
        threadId="thread-1"
        noticeContainer={mount}
      />,
    );
    expect(mount.textContent).toContain("1 of 2 checks finished");
    fireEvent.click(screen.getByRole("button", { name: "Open Batches" }));
    expect(await screen.findByRole("dialog")).toBeDefined();
    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
    fireEvent.click(await screen.findByRole("button", { name: "Dismiss" }));
    view.unmount();
    const again = render(
      <ThreadSubagentBatches
        accountId="starter"
        threadId="thread-1"
        noticeContainer={mount}
      />,
    );
    expect(mount.textContent).toBe("");
    again.rerender(
      <ThreadSubagentBatches
        accountId="other"
        threadId="thread-1"
        noticeContainer={mount}
      />,
    );
    expect(mount.textContent).toContain("1 of 2 checks finished");
    for (const reason of ["cancelled", "interrupted", "future"]) {
      batchState.batches = [
        { ...job, follow_up: { ...job.follow_up, reason } },
        job,
      ];
      again.rerender(
        <ThreadSubagentBatches
          accountId="other"
          threadId="thread-1"
          noticeContainer={mount}
        />,
      );
      expect(mount.textContent).toBe("");
    }
    mount.remove();
  });
});
