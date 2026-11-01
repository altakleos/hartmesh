import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({ user: { id: "alice", system_role: "user" } }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({
    t: { agentInstances: { providerFallback: "your service provider" } },
  }),
}));

import { fetch } from "@/core/api/fetcher";
import { useTelegram } from "@/core/features/hooks";
import {
  telegramBotsQueryPrefix,
  useTelegramBots,
  useTelegramReader,
} from "@/core/telegram-bots/hooks";

const clients: QueryClient[] = [];
const list = { bots: [], limits: { per_person: 10, used: 0 } };

function Probe() {
  const telegram = useTelegram();
  const bots = useTelegramBots();
  const reader = useTelegramReader();
  return (
    <output data-testid="probe">
      {JSON.stringify({
        known: telegram.known,
        loading: telegram.isLoading,
        bots: bots.data?.limits.per_person ?? null,
        reader,
      })}
    </output>
  );
}

function serve(features: () => Response) {
  rs.mocked(fetch).mockImplementation(async (url: string | URL | Request) =>
    (url as string).endsWith("/api/features")
      ? features()
      : new Response(JSON.stringify(list), { status: 200 }),
  );
}

function start() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, retryDelay: 0 } },
  });
  clients.push(client);
  render(
    <QueryClientProvider client={client}>
      <Probe />
    </QueryClientProvider>,
  );
  return client;
}

const state = () =>
  JSON.parse(screen.getByTestId("probe").textContent ?? "{}") as {
    known: boolean;
    loading: boolean;
    bots: number | null;
    reader: { admin: boolean; provider: string };
  };
const listRequests = () =>
  rs
    .mocked(fetch)
    .mock.calls.filter(([url]) =>
      (url as string).endsWith("/api/telegram-bots"),
    );
const features = (telegram: unknown) =>
  new Response(JSON.stringify({ agents_api: { enabled: true }, telegram }), {
    status: 200,
  });

afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  rs.mocked(fetch).mockReset();
  auth.user = { id: "alice", system_role: "user" };
});

describe("the telegram hooks", () => {
  it("stays closed while loading and reads no list", () => {
    rs.mocked(fetch).mockImplementation(() => new Promise(() => undefined));
    start();
    expect(state()).toMatchObject({ known: false, loading: true, bots: null });
    expect(listRequests()).toHaveLength(0);
  });

  it("stays closed after the features request failed", async () => {
    serve(() => new Response("{}", { status: 500 }));
    start();
    await waitFor(() => expect(state().loading).toBe(false));
    expect(state().known).toBe(false);
    expect(listRequests()).toHaveLength(0);
  });

  it("reads no list for a null key", async () => {
    serve(() => features(null));
    start();
    await waitFor(() => expect(state().loading).toBe(false));
    expect(state().known).toBe(false);
    expect(listRequests()).toHaveLength(0);
  });

  it("reads the list while Telegram is switched off, so bots are still seen", async () => {
    serve(() =>
      features({ enabled: false, bots: "unavailable", reason: "off" }),
    );
    start();
    await waitFor(() => expect(state().bots).toBe(10));
    expect(state().known).toBe(true);
  });

  it("keeps each account's list apart and refreshes by the shared prefix", async () => {
    serve(() => features({ enabled: true, bots: "ready", reason: null }));
    const client = start();
    await waitFor(() => expect(state().bots).toBe(10));
    expect(
      client.getQueryData([...telegramBotsQueryPrefix, "alice"]),
    ).toMatchObject(list);
    expect(
      client.getQueriesData({ queryKey: telegramBotsQueryPrefix }),
    ).toHaveLength(1);
  });

  it("says who reads a text", async () => {
    serve(
      () =>
        new Response(
          JSON.stringify({
            agents_api: { enabled: true },
            branding: { provider: { name: "Acme" } },
          }),
          { status: 200 },
        ),
    );
    auth.user = { id: "alice", system_role: "admin" };
    start();
    await waitFor(() => expect(state().loading).toBe(false));
    expect(state().reader.admin).toBe(true);
    expect(state().reader.provider).toBeTruthy();
  });
});
