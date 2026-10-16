import { afterEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({ query: "deleted=Researcher&clearing=1" }));
rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(state.query),
}));

import { DeletedAgentNotice } from "@/components/workspace/instances/deleted-agent-notice";
import { I18nProvider } from "@/core/i18n/context";

const clients: QueryClient[] = [];
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  rs.useRealTimers();
});
function show(kind: "personal" | "company" = "personal") {
  const client = new QueryClient();
  clients.push(client);
  const invalidations = rs.spyOn(client, "invalidateQueries");
  return {
    ...render(
      <QueryClientProvider client={client}>
        <I18nProvider initialLocale="en-US">
          <DeletedAgentNotice kind={kind} />
        </I18nProvider>
      </QueryClientProvider>,
    ),
    invalidations,
  };
}
test("a clearing notice refreshes actual feature counts every fifteen seconds for at most three minutes", async () => {
  state.query = "deleted=Researcher&clearing=1";
  rs.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
  const view = show();
  expect(
    screen.getByText(
      "Researcher was deleted. The files are still being cleared; the place will be free for a new AI assistant in a few minutes.",
    ),
  ).toBeDefined();
  expect(view.invalidations).toHaveBeenCalledTimes(1);
  await act(async () => {
    await rs.advanceTimersByTimeAsync(180_000);
  });
  expect(view.invalidations).toHaveBeenCalledTimes(13);
  await act(async () => {
    await rs.advanceTimersByTimeAsync(180_000);
  });
  expect(view.invalidations).toHaveBeenCalledTimes(13);
});
test("leaving the list retires feature polling", async () => {
  state.query = "deleted=Researcher&clearing=1";
  rs.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
  const view = show("company");
  view.unmount();
  await act(async () => {
    await rs.advanceTimersByTimeAsync(30_000);
  });
  expect(view.invalidations).toHaveBeenCalledTimes(1);
});
test("a completed deletion has final wording and no interval", async () => {
  state.query = "deleted=Researcher";
  rs.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
  const view = show();
  expect(screen.getByText("Researcher was deleted.")).toBeDefined();
  await act(async () => {
    await rs.advanceTimersByTimeAsync(30_000);
  });
  expect(view.invalidations).toHaveBeenCalledTimes(1);
});
