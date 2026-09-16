import { afterEach, expect, test, rs } from "@rstest/core";

import { parseArtifactDeliveryFailure } from "@/core/artifact-delivery";

const deliveryState = rs.hoisted(() => ({
  recordFailure: rs.fn(),
}));

async function captureThreadStreamOptions() {
  let capturedOptions: Record<string, unknown> | undefined;

  rs.resetModules();
  rs.doMock("react", () => ({
    useCallback: <T extends (...args: never[]) => unknown>(callback: T) =>
      callback,
    useEffect: () => undefined,
    useMemo: <T>(factory: () => T) => factory(),
    useRef: <T>(initialValue: T) => ({ current: initialValue }),
    useState: <T>(initialValue: T | (() => T)) => [
      typeof initialValue === "function"
        ? (initialValue as () => T)()
        : initialValue,
      rs.fn(),
    ],
  }));
  rs.doMock("@tanstack/react-query", () => ({
    useInfiniteQuery: () => ({
      data: { pages: [] },
      error: null,
      fetchNextPage: rs.fn(),
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: false,
    }),
    useMutation: rs.fn(),
    useQuery: rs.fn(),
    useQueryClient: () => ({
      invalidateQueries: rs.fn(),
      setQueriesData: rs.fn(),
    }),
  }));
  rs.doMock("@langchain/langgraph-sdk/react", () => ({
    useStream: (options: Record<string, unknown>) => {
      capturedOptions = options;
      return {
        isLoading: false,
        messages: [],
        stop: rs.fn(),
        submit: rs.fn(),
        values: { title: "", messages: [] },
      };
    },
  }));
  rs.doMock("@/core/api", () => ({
    getAPIClient: () => ({}),
  }));
  rs.doMock("@/core/i18n/hooks", () => ({
    useI18n: () => ({
      t: {
        conversation: { streamReplayGap: "Reloading this conversation" },
        pages: { newChat: "New chat" },
        uploads: { uploadingFiles: "Uploading files" },
      },
    }),
  }));
  rs.doMock("@/core/tasks/context", () => ({
    useSubtaskContext: () => ({
      tasksRef: { current: {} },
      setTasks: rs.fn(),
    }),
    useUpdateSubtask: () => rs.fn(),
  }));
  rs.doMock("@/core/artifact-delivery", () => ({
    parseArtifactDeliveryFailure,
    useArtifactDeliveryContext: () => deliveryState,
  }));
  rs.doMock("sonner", () => ({
    toast: Object.assign(rs.fn(), {
      error: rs.fn(),
      success: rs.fn(),
      warning: rs.fn(),
    }),
  }));

  const { useThreadStream } = await import("@/core/threads/hooks");
  function ThreadStreamCapture() {
    useThreadStream({
      context: { mode: "flash" },
      isMock: true,
    } as never);
    return null;
  }
  ThreadStreamCapture();

  return capturedOptions;
}

afterEach(() => {
  rs.doUnmock("react");
  rs.doUnmock("@tanstack/react-query");
  rs.doUnmock("@langchain/langgraph-sdk/react");
  rs.doUnmock("@/core/api");
  rs.doUnmock("@/core/i18n/hooks");
  rs.doUnmock("@/core/tasks/context");
  rs.doUnmock("@/core/artifact-delivery");
  rs.doUnmock("sonner");
  rs.resetModules();
  deliveryState.recordFailure.mockClear();
});

test("a delivery failure frame is kept for the run that produced it", async () => {
  const options = await captureThreadStreamOptions();

  (options?.onCustomEvent as (event: unknown) => void)({
    type: "artifact_delivery_incomplete",
    run_id: "run-1",
    message:
      "Artifact delivery incomplete: no produced output artifact was presented",
    undelivered_paths: ["/mnt/user-data/outputs/report.md"],
    undelivered_count: 1,
  });

  expect(deliveryState.recordFailure).toHaveBeenCalledWith({
    runId: "run-1",
    message:
      "Artifact delivery incomplete: no produced output artifact was presented",
    undeliveredPaths: ["/mnt/user-data/outputs/report.md"],
    undeliveredCount: 1,
  });
});

test("an unrelated custom event is not mistaken for a delivery failure", async () => {
  const options = await captureThreadStreamOptions();

  (options?.onCustomEvent as (event: unknown) => void)({
    type: "llm_retry",
    message: "Retrying",
  });

  expect(deliveryState.recordFailure).not.toHaveBeenCalled();
});

test("a replay gap keeps the verdicts it cannot re-read from durable state", async () => {
  const options = await captureThreadStreamOptions();

  (options?.onCustomEvent as (event: unknown) => void)({
    type: "artifact_delivery_incomplete",
    run_id: "run-1",
    message:
      "Artifact delivery incomplete: no produced output artifact was presented",
    undelivered_paths: ["/mnt/user-data/outputs/report.md"],
    undelivered_count: 1,
  });
  (options?.onCustomEvent as (event: unknown) => void)({
    type: "stream_replay_gap",
    code: "stream_replay_gap",
    run_id: "run-1",
    requested_event_id: null,
    earliest_available_event_id: null,
    latest_available_event_id: null,
    recovery: "reload_durable_state",
  });

  // Nothing in this hook can withdraw a recorded verdict: no durable source can
  // hand it back, so the gap's wholesale reset deliberately leaves it alone.
  expect(deliveryState).not.toHaveProperty("clearFailures");
  expect(deliveryState.recordFailure).toHaveBeenCalledTimes(1);
});
