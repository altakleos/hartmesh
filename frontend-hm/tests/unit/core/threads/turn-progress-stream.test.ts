import { afterEach, expect, test, rs } from "@rstest/core";

import { parseTurnProgress } from "@/core/turn-progress";

const progressState = rs.hoisted(() => ({
  record: rs.fn(),
  clear: rs.fn(),
}));

const toastState = rs.hoisted(() => {
  const error = rs.fn();
  return {
    error,
    toast: Object.assign(rs.fn(), {
      error,
      success: rs.fn(),
      warning: rs.fn(),
    }),
  };
});

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
        artifactDelivery: { receiptToast: "We couldn't confirm the save." },
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
    parseArtifactDeliveryFailure: () => null,
    parseArtifactDeliveryUnverified: () => null,
    useArtifactDeliveryContext: () => ({ recordFailure: rs.fn() }),
  }));
  rs.doMock("@/core/turn-progress", () => ({
    parseTurnProgress,
    useTurnProgressContext: () => progressState,
  }));
  rs.doMock("sonner", () => ({ toast: toastState.toast }));

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
  rs.doUnmock("@/core/turn-progress");
  rs.doUnmock("sonner");
  rs.resetModules();
  progressState.record.mockClear();
  progressState.clear.mockClear();
  toastState.error.mockClear();
});

test("a progress frame on the stream is recorded for the thread that is streaming", async () => {
  const options = await captureThreadStreamOptions();
  const onCreated = options?.onCreated as (meta: {
    thread_id: string;
    run_id: string;
  }) => void;
  const onCustomEvent = options?.onCustomEvent as (event: unknown) => void;

  onCreated({ thread_id: "thread-1", run_id: "run-1" });
  onCustomEvent({
    type: "turn_progress",
    run_id: "run-1",
    stage: "preparing",
    at_ms: 3,
  });

  expect(progressState.record).toHaveBeenCalledWith("thread-1", {
    runId: "run-1",
    stage: "preparing",
    atMs: 3,
  });
  expect(toastState.error).not.toHaveBeenCalled();
});

test("a frame that arrives before the stream named its thread is dropped", async () => {
  const options = await captureThreadStreamOptions();
  const onCustomEvent = options?.onCustomEvent as (event: unknown) => void;

  onCustomEvent({
    type: "turn_progress",
    run_id: "run-1",
    stage: "preparing",
    at_ms: 3,
  });

  expect(progressState.record).not.toHaveBeenCalled();
});

test("a frame with a stage this client cannot name is ignored, not shown", async () => {
  const options = await captureThreadStreamOptions();
  const onCustomEvent = options?.onCustomEvent as (event: unknown) => void;

  onCustomEvent({
    type: "turn_progress",
    run_id: "run-1",
    stage: "rendering",
    at_ms: 3,
  });

  expect(progressState.record).not.toHaveBeenCalled();
});

test("the thread's label is cleared when its run finishes, fails or is replayed", async () => {
  const options = await captureThreadStreamOptions();
  const onCreated = options?.onCreated as (meta: {
    thread_id: string;
    run_id: string;
  }) => void;
  const onFinish = options?.onFinish as (state: { values: unknown }) => void;
  const onError = options?.onError as (error: unknown) => void;
  const onCustomEvent = options?.onCustomEvent as (event: unknown) => void;

  onCreated({ thread_id: "thread-1", run_id: "run-1" });
  onFinish({ values: { messages: [] } });
  onError(new Error("stream failed"));
  onCustomEvent({ type: "stream_replay_gap" });

  expect(progressState.clear).toHaveBeenCalledTimes(3);
  expect(progressState.clear).toHaveBeenCalledWith("thread-1");
});
