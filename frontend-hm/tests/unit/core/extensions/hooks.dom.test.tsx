import { afterEach, expect, rs, test } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  render,
  screen,
  waitFor,
  act,
  fireEvent,
} from "@testing-library/react";

import {
  useFrontendExtensions,
  useFrontendServices,
} from "@/core/extensions/hooks";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";

const state = rs.hoisted(() => ({ user: "viewer-1", request: rs.fn() }));
const notifications = rs.hoisted(() => ({
  message: rs.fn(() => "plugin-message-id"),
  dismiss: rs.fn(),
}));
rs.mock("sonner", () => ({ toast: notifications }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: state.user } }),
}));
rs.mock("@/core/api/fetcher", () => ({ fetch: state.request }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));

function descriptor(viewer: string) {
  return {
    viewer_id: viewer,
    namespace: `example.${viewer}`,
    module: null,
    entry: null,
    title: viewer,
    description: "",
    settings: { enabled: true },
    backend_actions: [],
    artifact_presentations: [],
  };
}
function Host() {
  const query = useFrontendExtensions();
  return <p>{query.data?.[0]?.title ?? "Loading"}</p>;
}
afterEach(() => {
  cleanup();
  rs.clearAllMocks();
  state.user = "viewer-1";
});

test("workspace retirement dismisses an already displayed plugin message and rejects late messages", () => {
  let late!: (message: string) => void;
  function Messages() {
    const { showMessage } = useFrontendServices();
    late = showMessage;
    return (
      <button onClick={() => showMessage("Private plugin message")}>
        Show message
      </button>
    );
  }
  const { unmount } = render(
    <FileActionLifetimeProvider>
      <Messages />
    </FileActionLifetimeProvider>,
  );
  fireEvent.click(screen.getByRole("button", { name: "Show message" }));
  expect(notifications.message).toHaveBeenCalledWith("Private plugin message");
  unmount();
  expect(notifications.dismiss).toHaveBeenCalledWith("plugin-message-id");
  expect(() => late("Late private message")).toThrow();
  expect(notifications.message).toHaveBeenCalledTimes(1);
});

test("account discovery cancellation cannot populate the new viewer and inactive data is evicted", async () => {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  let finish!: (response: Response) => void;
  let oldSignal!: AbortSignal;
  state.request
    .mockImplementationOnce((_url, options: { signal: AbortSignal }) => {
      oldSignal = options.signal;
      return new Promise<Response>((resolve) => {
        finish = resolve;
      });
    })
    .mockResolvedValueOnce(
      new Response(JSON.stringify([descriptor("viewer-2")])),
    );
  const { rerender, unmount } = render(
    <QueryClientProvider client={client}>
      <Host />
    </QueryClientProvider>,
  );
  await waitFor(() => expect(state.request).toHaveBeenCalledTimes(1));
  state.user = "viewer-2";
  rerender(
    <QueryClientProvider client={client}>
      <Host />
    </QueryClientProvider>,
  );
  await waitFor(() => expect(screen.getByText("viewer-2")).toBeDefined());
  expect(oldSignal.aborted).toBe(true);
  await act(async () => {
    finish(new Response(JSON.stringify([descriptor("viewer-1")])));
    await Promise.resolve();
  });
  expect(screen.queryByText("viewer-1")).toBeNull();
  unmount();
  await waitFor(() => expect(client.getQueryCache().getAll()).toHaveLength(0));
});
