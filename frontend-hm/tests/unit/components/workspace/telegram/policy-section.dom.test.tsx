import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  screen,
  waitFor,
} from "@testing-library/react";
import { toast } from "sonner";

const state = rs.hoisted(() => ({
  user: {
    id: "person-a",
    system_role: "user",
    permissions: ["agents:read", "agents:write"],
  },
  known: true,
  t: {} as unknown,
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: state.user }),
}));
rs.mock("@/core/features/hooks", () => ({
  useTelegram: () => ({ known: state.known, enabled: false }),
}));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: state.t }) }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("sonner", () => ({
  toast: { success: rs.fn(), error: rs.fn(), dismiss: rs.fn() },
}));

import { TelegramPolicySection } from "@/components/workspace/telegram/policy-section";
import type { AgentInstance } from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { createEnUS } from "@/core/i18n/locales/en-US";

import { clearClients, json, queries, renderWithClient } from "./support";

const instance: AgentInstance = {
  id: "employee-a",
  name: "Mira",
  custody: "company",
  permissions: 7,
  owner_id: null,
  creator_id: "person-a",
  supervisor: { kind: "human", subject_id: "person-a" },
  principal: { kind: "nonhuman", subject_id: "employee-a" },
  definition_revision: "revision",
  home_id: "home",
  status: "active",
  generation: 1,
};
const path = "/api/telegram-bots/instances/employee-a/policy";
let stored = { policy: "allowed", people: 3, connected: 2 };
let calls: { url: string; init?: RequestInit }[];
let answer: (init?: RequestInit) => Promise<Response>;
const view = (entry = instance) => (
  <FileActionLifetimeProvider>
    <TelegramPolicySection instance={entry} />
  </FileActionLifetimeProvider>
);
const choose = (label: string) =>
  fireEvent.click(screen.getByRole<HTMLInputElement>("radio", { name: label }));
const save = () =>
  fireEvent.click(
    screen.getByRole<HTMLButtonElement>("button", { name: "Save" }),
  );
const puts = () => calls.filter(({ init }) => init?.method === "PUT");

beforeEach(() => {
  state.t = createEnUS("HartMesh");
  state.user = {
    id: "person-a",
    system_role: "user",
    permissions: ["agents:read", "agents:write"],
  };
  state.known = true;
  stored = { policy: "allowed", people: 3, connected: 2 };
  calls = [];
  answer = async (init) => {
    if (init?.method === "PUT")
      stored = { ...stored, ...JSON.parse(init.body as string) };
    return json(stored);
  };
  rs.stubGlobal(
    "fetch",
    rs.fn(async (url: string, init?: RequestInit) => {
      calls.push({ url, init });
      return answer(init);
    }),
  );
});
afterEach(() => {
  cleanup();
  clearClients();
  rs.unstubAllGlobals();
  rs.clearAllMocks();
});

describe("employee Telegram policy", () => {
  it("shows the count and three choices with the stored one chosen, even when Telegram is off", async () => {
    renderWithClient(view());
    await screen.findByText(
      "2 of 3 people who can chat with Mira have connected a Telegram bot.",
    );
    expect(screen.getAllByRole("radio")).toHaveLength(3);
    expect(
      screen.getByRole<HTMLInputElement>("radio", { name: "Yes" }).checked,
    ).toBe(true);
    expect(screen.getByText(/A screenshot is still possible/)).toBeDefined();
    expect(
      screen.getByRole<HTMLButtonElement>("button", { name: "Save" }).disabled,
    ).toBe(true);
    expect(calls[0]?.url).toBe(path);
  });
  it.each(["allowed", "protected"])(
    "saves %s without confirmation and shows its toast",
    async (policy) => {
      stored.policy = "off";
      renderWithClient(view());
      await screen.findByRole("radio", { name: "No" });
      choose(
        policy === "allowed" ? "Yes" : "Yes, without forwarding or saving",
      );
      save();
      await waitFor(() => expect(puts()).toHaveLength(1));
      expect(JSON.parse(puts()[0]!.init!.body as string)).toEqual({ policy });
      await waitFor(() => expect(toast.success).toHaveBeenCalledTimes(1));
      expect(screen.queryByRole("dialog")).toBeNull();
    },
  );
  it.each([0, 1, 2])(
    "confirms No with %s connected bots before saving",
    async (connected) => {
      stored.connected = connected;
      renderWithClient(view());
      await screen.findByRole("radio", { name: "No" });
      choose("No");
      save();
      await screen.findByRole("dialog", {
        name: "Turn Telegram off for Mira?",
      });
      expect(puts()).toHaveLength(0);
      expect(
        screen.getByText(
          connected === 0
            ? /Nobody has connected a bot yet/
            : connected === 1
              ? /1 bot stops answering now/
              : /2 bots stop answering now/,
        ),
      ).toBeDefined();
      fireEvent.click(
        screen.getByRole<HTMLButtonElement>("button", { name: "Cancel" }),
      );
      expect(puts()).toHaveLength(0);
      save();
      fireEvent.click(
        screen.getByRole<HTMLButtonElement>("button", { name: "Turn off" }),
      );
      await waitFor(() => expect(puts()).toHaveLength(1));
      await waitFor(() =>
        expect(toast.success).toHaveBeenCalledWith(
          "Saved. Mira's Telegram bots have stopped.",
        ),
      );
    },
  );
  it("a refusal rereads stored policy and refreshes instance authority on manage-required", async () => {
    renderWithClient(view());
    await screen.findByRole("radio", { name: "No" });
    const invalidate = rs.spyOn(queries(), "invalidateQueries");
    answer = async (init) =>
      init?.method === "PUT"
        ? json({ reason: "manage-required", detail: "private detail" }, 403)
        : json({ ...stored, policy: "protected" });
    choose("No");
    save();
    fireEvent.click(
      screen.getByRole<HTMLButtonElement>("button", { name: "Turn off" }),
    );
    await waitFor(() =>
      expect(toast.error).toHaveBeenCalledWith(
        "The setting could not be saved. Try again.",
      ),
    );
    await waitFor(() =>
      expect(
        screen.getByRole<HTMLInputElement>("radio", {
          name: "Yes, without forwarding or saving",
        }).checked,
      ).toBe(true),
    );
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["agent-instances"] });
    expect(screen.queryByText("private detail")).toBeNull();
  });
  it("does not read or render for a personal assistant, a non-manager or unknown Telegram", async () => {
    const tree = renderWithClient(view({ ...instance, custody: "personal" }));
    expect(screen.queryByRole("radio")).toBeNull();
    expect(calls).toHaveLength(0);
    tree.rerender(
      <QueryClientProvider client={queries()}>
        {view({ ...instance, permissions: 3 })}
      </QueryClientProvider>,
    );
    expect(calls).toHaveLength(0);
    state.known = false;
    tree.rerender(
      <QueryClientProvider client={queries()}>{view()}</QueryClientProvider>,
    );
    expect(calls).toHaveLength(0);
  });
  it("disables changes without agents:write", async () => {
    state.user.permissions = ["agents:read"];
    renderWithClient(view());
    await screen.findByRole("radio", { name: "No" });
    expect(
      screen
        .getAllByRole("radio")
        .every((r) => (r as HTMLInputElement).disabled),
    ).toBe(true);
    expect(puts()).toHaveLength(0);
  });
  it.each(["account", "grant", "unmount"])(
    "retires an in-flight save on %s change without late UI or follow-up reads",
    async (retirement) => {
      const tree = renderWithClient(view());
      await screen.findByRole("radio", { name: "No" });
      let finish!: (response: Response) => void;
      answer = async (init) =>
        init?.method === "PUT"
          ? new Promise<Response>((resolve) => {
              finish = resolve;
            })
          : json(stored);
      choose("Yes, without forwarding or saving");
      save();
      await waitFor(() => expect(puts()).toHaveLength(1));
      if (retirement === "account") {
        state.user = { ...state.user, id: "person-b" };
        tree.rerender(
          <QueryClientProvider client={queries()}>
            {view()}
          </QueryClientProvider>,
        );
      } else if (retirement === "grant")
        tree.rerender(
          <QueryClientProvider client={queries()}>
            {view({ ...instance, permissions: 3 })}
          </QueryClientProvider>,
        );
      else tree.unmount();
      expect(puts()[0]!.init!.signal!.aborted).toBe(true);
      const reads = calls.length;
      await act(async () => {
        finish(json({ reason: "manage-required" }, 403));
      });
      expect(toast.error).not.toHaveBeenCalled();
      expect(toast.success).not.toHaveBeenCalled();
      expect(calls).toHaveLength(reads);
    },
  );
});
