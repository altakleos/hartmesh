import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({ user: { id: "alice", system_role: "user" } }));
const i18n = rs.hoisted(() => ({ t: {} as unknown }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: i18n.t }) }));
rs.mock("sonner", () => ({ toast: { success: rs.fn(), error: rs.fn() } }));

import { resetTelegramPageState } from "@/components/workspace/telegram/setup-dialog-store";
import {
  useTelegramConnect,
  useTelegramDisconnect,
} from "@/components/workspace/telegram/use-telegram-failure";
import { fetch } from "@/core/api/fetcher";
import { createEnUS } from "@/core/i18n/locales/en-US";

import { bot, json, listOf } from "./support";

i18n.t = createEnUS("HartMesh");

const CONNECT = {
  url: "https://t.me/x?start=c",
  expires_at: "2099-01-01T00:00:00Z",
};

/** A POST stays unanswered until `answer`, as on a slow network. */
function slowServer(answer: unknown) {
  const posts: string[] = [];
  let finish = () => undefined as void;
  rs.mocked(fetch).mockImplementation(((url: string, init?: RequestInit) => {
    if (init?.method !== "POST") {
      return Promise.resolve(json(listOf([bot()])));
    }
    posts.push(url);
    return new Promise((resolve) => {
      finish = () => resolve(json(answer));
    });
  }) as unknown as typeof fetch);
  return { posts, answer: () => finish() };
}

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

afterEach(() => {
  cleanup();
  resetTelegramPageState();
  rs.mocked(fetch).mockReset();
});

describe("one request at a time", () => {
  it("a second press while a link is on its way mints nothing, and the next one can", async () => {
    const network = slowServer(CONNECT);
    const { result } = renderHook(() => useTelegramConnect(bot(), rs.fn()), {
      wrapper,
    });

    act(() => void result.current.request({ open: false }));
    await waitFor(() => expect(network.posts).toHaveLength(1));
    expect(result.current.busy).toBe(true);
    act(() => void result.current.request({ open: false }));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(network.posts).toHaveLength(1);

    network.answer();
    await waitFor(() => expect(result.current.busy).toBe(false));
    act(() => void result.current.request({ open: false }));
    await waitFor(() => expect(network.posts).toHaveLength(2));
  });

  it("a second press while a disconnect is on its way sends nothing", async () => {
    const network = slowServer({ bot: bot(), connect: CONNECT });
    const { result } = renderHook(() => useTelegramDisconnect(), { wrapper });

    act(() => void result.current.disconnect(bot(), "HartMesh"));
    await waitFor(() => expect(network.posts).toHaveLength(1));
    expect(result.current.busy).toBe(true);
    act(() => void result.current.disconnect(bot(), "HartMesh"));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(network.posts).toHaveLength(1);

    network.answer();
    await waitFor(() => expect(result.current.busy).toBe(false));
  });
});
