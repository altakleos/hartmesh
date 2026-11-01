import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactNode } from "react";

import type { Bot, BotList } from "@/core/telegram-bots/api";

export const ago = (seconds: number) =>
  new Date(Date.now() - seconds * 1000).toISOString();

export function bot(overrides: Partial<Bot> = {}): Bot {
  return {
    id: "link-a",
    bot_id: 1,
    agent_kind: "lead_agent",
    agent_ref: "lead_agent",
    agent_name: "HartMesh",
    bot_username: "chat_abc_bot",
    bot_name: "HartMesh",
    url: "https://t.me/chat_abc_bot",
    origin: "pasted",
    managed_by: null,
    can_manage_bots: false,
    bind_mode: "code",
    state: "connected",
    health: "ok",
    connected_as: "Rosa",
    connect_expires_at: null,
    profile_synced: true,
    acts_paused_since: null,
    probing_ended: false,
    limited_since: null,
    delivery_pending: 0,
    delivery_degraded: false,
    delivery_failed: 0,
    webhook_host: null,
    topics_enabled: false,
    policy: null,
    last_update_at: null,
    created_at: ago(3600),
    ...overrides,
  };
}

export function listOf(bots: Bot[], overrides: Partial<BotList> = {}): BotList {
  return {
    bots,
    limits: { per_person: 10, used: bots.length },
    settings: null,
    quick_setup: {
      available: false,
      manager_link_id: null,
      manager_username: null,
    },
    pending_creation: null,
    removed_bots: [],
    policies: [],
    read_by_others: [],
    ...overrides,
  };
}

export interface Call {
  url: string;
  method: string;
  body: unknown;
}

/** Answers by `METHOD path`; every call is recorded for the test to read. */
export function server(routes: Record<string, () => Response>): {
  calls: Call[];
  handle: typeof fetch;
} {
  const calls: Call[] = [];
  const handle = (async (url: string, init?: RequestInit) => {
    const method = init?.method ?? "GET";
    calls.push({
      url,
      method,
      body: init?.body ? JSON.parse(init.body as string) : undefined,
    });
    const route = routes[`${method} ${url}`];
    return route ? route() : new Response("{}", { status: 200 });
  }) as unknown as typeof fetch;
  return { calls, handle };
}

export const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status });

const clients: QueryClient[] = [];

export function renderWithClient(tree: ReactNode) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, retryDelay: 0 } },
  });
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>{tree}</QueryClientProvider>,
  );
}

/** The client of the tree rendered last, for a test that makes it read again. */
export function queries(): QueryClient {
  return clients[clients.length - 1]!;
}

export function clearClients() {
  clients.splice(0).forEach((client) => client.clear());
}
