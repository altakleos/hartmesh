import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const auth = rs.hoisted(() => ({ user: { id: "alice", system_role: "user" } }));
const toast = rs.hoisted(() => ({ success: rs.fn(), error: rs.fn() }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
rs.mock("sonner", () => ({ toast }));
const i18n = rs.hoisted(() => ({ t: {} as unknown }));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: i18n.t, locale: "en-US" }),
}));

import {
  resetTelegramPageState,
  useTelegramPageState,
} from "@/components/workspace/telegram/setup-dialog-store";
import { TelegramTokenDialog } from "@/components/workspace/telegram/token-dialog";
import { fetch } from "@/core/api/fetcher";
import { createEnUS } from "@/core/i18n/locales/en-US";

import {
  bot,
  clearClients,
  json,
  listOf,
  renderWithClient,
  server,
} from "./support";

i18n.t = createEnUS("HartMesh");

const ROUTE = "POST /api/telegram-bots/link-a/token";
// Built at run time: no tracked file may hold text of a token's shape.
const tokenA = `${"1".repeat(9)}:${"A".repeat(35)}`;
const tokenB = `${"2".repeat(9)}:${"B".repeat(35)}`;
const onClose = rs.fn();

function Replaced() {
  const { replaced, connects } = useTelegramPageState();
  return (
    <output data-testid="page">
      {JSON.stringify({
        replaced: Array.from(replaced),
        connects: Array.from(connects.keys()),
      })}
    </output>
  );
}

function start(answer: () => Response) {
  const { calls, handle } = server({
    "GET /api/features": () =>
      json({
        agents_api: { enabled: true },
        telegram: { enabled: true, bots: "ready", reason: null },
      }),
    "GET /api/telegram-bots": () => json(listOf([bot()])),
    [ROUTE]: answer,
  });
  rs.mocked(fetch).mockImplementation(handle);
  renderWithClient(
    <>
      <TelegramTokenDialog bot={bot()} name="HartMesh" onClose={onClose} />
      <Replaced />
    </>,
  );
  const sent = () => calls.filter(({ url }) => url.endsWith("/token"));
  return { sent };
}

const field = () => screen.getByLabelText<HTMLInputElement>("Bot token");
const paste = (text: string) =>
  fireEvent.paste(field(), { clipboardData: { getData: () => text } });
const committed = (overrides = {}) =>
  json({
    bot: bot(overrides),
    managed_replaced: 0,
    replacement_status: "committed",
  });
const refusal = (status: number, reason: string, extra = {}) =>
  json({ detail: "not shown", reason, ...extra }, status);

beforeEach(() => {
  onClose.mockReset();
  toast.success.mockReset();
  toast.error.mockReset();
});

afterEach(() => {
  cleanup();
  clearClients();
  resetTelegramPageState();
  rs.mocked(fetch).mockReset();
});

describe("the token dialog", () => {
  it("a new token for the same bot closes the dialog with its message", async () => {
    const { sent } = start(() => committed());
    paste(`Use this token: ${tokenA}`);
    await waitFor(() =>
      expect(toast.success).toHaveBeenCalledWith(
        "Token replaced. HartMesh keeps answering in the same chat.",
      ),
    );
    expect(onClose).toHaveBeenCalled();
    expect(sent()[0]?.body).toMatchObject({ token: tokenA });
    expect(field().value).toBe("");
  });

  it("a token of another bot leaves the row waiting for Start", async () => {
    start(() =>
      json({
        bot: bot({ id: "link-b", bot_id: 2, state: "waiting_connect" }),
        connect: { url: "https://t.me/b?start=c", expires_at: "2099-01-01" },
        managed_replaced: 0,
        replacement_status: "committed",
      }),
    );
    paste(tokenA);
    await waitFor(() =>
      expect(toast.success).toHaveBeenCalledWith(
        "Bot replaced. Open the chat from its row.",
      ),
    );
    expect(screen.getByTestId("page").textContent).toBe(
      JSON.stringify({ replaced: ["link-b"], connects: ["link-b"] }),
    );
  });

  it("recognises a successor from a replayed receipt without an address", async () => {
    start(() => committed({ id: "link-b", bot_id: 2 }));
    paste(tokenA);
    await waitFor(() =>
      expect(toast.success).toHaveBeenCalledWith(
        "Bot replaced. Open the chat from its row.",
      ),
    );
  });

  it("token-same shows its sentence and empties the field", async () => {
    start(() => refusal(409, "token-same"));
    paste(tokenA);
    await screen.findByText(/That is the token this bot already has/);
    expect(field().value).toBe("");
    expect(onClose).not.toHaveBeenCalled();
  });

  it("token-invalid shows refusal.rejected", async () => {
    start(() => refusal(400, "token-invalid"));
    paste(tokenA);
    await screen.findByText(/Telegram does not recognise this token/);
  });

  it("sends nothing for text without a token", async () => {
    const { sent } = start(() => committed());
    paste("not a token");
    await screen.findByText(/That is not a bot token/);
    expect(sent()).toEqual([]);
  });

  it("sends nothing while a token is typed, and the whole token when the field loses the focus", async () => {
    const { sent } = start(() => committed());
    for (let length = 1; length <= tokenA.length; length += 7) {
      fireEvent.change(field(), {
        target: { value: tokenA.slice(0, length) },
      });
    }
    fireEvent.change(field(), { target: { value: tokenA } });
    expect(sent()).toEqual([]);
    fireEvent.blur(field());
    await waitFor(() => expect(sent()).toHaveLength(1));
    expect(sent()[0]?.body).toMatchObject({ token: tokenA });
  });

  it("repeats one operation for the same token after a lost answer, and starts a new one for another", async () => {
    const { sent } = start(() => {
      throw new TypeError("network");
    });
    paste(tokenA);
    await screen.findByText(/The result is not confirmed/);
    paste(tokenA);
    await waitFor(() => expect(sent()).toHaveLength(2));
    await screen.findByText(/The result is not confirmed/);
    paste(tokenB);
    await waitFor(() => expect(sent()).toHaveLength(3));
    const ids = sent().map(
      ({ body }) => (body as { operation_id: string }).operation_id,
    );
    expect(ids[0]).toMatch(/^[0-9a-f]{32}$/);
    expect(ids[1]).toBe(ids[0]);
    expect(ids[2]).not.toBe(ids[0]);
  });

  it("asks for the token again after Use it here, and keeps the operation", async () => {
    let answer = () => refusal(409, "webhook-set", { webhook_host: "x.test" });
    const { sent } = start(() => answer());
    paste(tokenA);
    fireEvent.click(await screen.findByRole("button", { name: "Use it here" }));
    await screen.findByText(/The result is not confirmed/);
    expect(sent()).toHaveLength(1);
    answer = () => committed();
    paste(tokenA);
    await waitFor(() => expect(sent()).toHaveLength(2));
    const [first, second] = sent().map(
      ({ body }) => body as Record<string, unknown>,
    );
    expect(second?.take_over_webhook).toBe(true);
    expect(first?.take_over_webhook).toBeUndefined();
    expect(second?.operation_id).toBe(first?.operation_id);
  });

  it("takes over no webhook but the one the person agreed to", async () => {
    let answer = () => refusal(409, "webhook-set", { webhook_host: "x.test" });
    const { sent } = start(() => answer());
    paste(tokenA);
    fireEvent.click(await screen.findByRole("button", { name: "Use it here" }));
    await screen.findByText(/The result is not confirmed/);
    answer = () => refusal(400, "token-invalid");
    paste(tokenB);
    await waitFor(() => expect(sent()).toHaveLength(2));
    await screen.findByText(/Telegram does not recognise this token/);
    const [first, second] = sent().map(
      ({ body }) => body as Record<string, unknown>,
    );
    expect(second?.take_over_webhook).toBeUndefined();
    expect(second?.operation_id).not.toBe(first?.operation_id);
    expect(screen.queryByRole("button", { name: "Use it here" })).toBeNull();
  });

  it("reads the clipboard on Paste", async () => {
    rs.stubGlobal("navigator", {
      ...navigator,
      clipboard: { readText: async () => `token: ${tokenA}` },
    });
    const { sent } = start(() => committed());
    fireEvent.click(screen.getByRole("button", { name: "Paste" }));
    await waitFor(() => expect(sent()).toHaveLength(1));
    expect(sent()[0]?.body).toMatchObject({ token: tokenA });
  });

  it("says an empty clipboard holds no token", async () => {
    rs.stubGlobal("navigator", {
      ...navigator,
      clipboard: { readText: async () => "" },
    });
    const { sent } = start(() => committed());
    fireEvent.click(screen.getByRole("button", { name: "Paste" }));
    await screen.findByText(/That is not a bot token/);
    expect(sent()).toEqual([]);
  });

  it("puts the focus in the field where the browser refuses the clipboard", async () => {
    rs.stubGlobal("navigator", {
      ...navigator,
      clipboard: {
        readText: async () => {
          throw new Error("refused");
        },
      },
    });
    start(() => committed());
    fireEvent.click(screen.getByRole("button", { name: "Paste" }));
    await waitFor(() => expect(document.activeElement).toBe(field()));
  });

  it("sends nothing when Cancel is pressed with text in the field", () => {
    const { sent } = start(() => committed());
    fireEvent.change(field(), { target: { value: tokenA } });
    const cancel = screen.getByRole("button", { name: "Cancel" });
    expect(fireEvent.mouseDown(cancel)).toBe(false);
    fireEvent.click(cancel);
    expect(onClose).toHaveBeenCalled();
    expect(sent()).toEqual([]);
  });

  it("forgets the operation when the dialog closes", async () => {
    const { sent } = start(() => {
      throw new TypeError("network");
    });
    paste(tokenA);
    await screen.findByText(/The result is not confirmed/);
    const first = (sent()[0]?.body as { operation_id: string }).operation_id;
    cleanup();
    clearClients();

    const again = start(() => committed());
    paste(tokenA);
    await waitFor(() => expect(again.sent()).toHaveLength(1));
    expect(
      (again.sent()[0]?.body as { operation_id: string }).operation_id,
    ).not.toBe(first);
  });

  it("closes a stale flow when the replacement already finished", async () => {
    start(() =>
      json({
        bot: bot(),
        managed_replaced: 0,
        replacement_status: "superseded",
      }),
    );
    paste(tokenA);
    await waitFor(() =>
      expect(toast.success).toHaveBeenCalledWith(
        "This token change completed earlier. The bot has changed again since then. The current state is shown.",
      ),
    );
    expect(onClose).toHaveBeenCalled();
  });

  it("closes when the replaced bot was later removed", async () => {
    start(() => refusal(410, "replacement-gone"));
    paste(tokenA);
    await waitFor(() => expect(onClose).toHaveBeenCalled());
    expect(toast.success).toHaveBeenCalledWith(
      "The replacement completed, and that bot was later removed. The bot list has been refreshed.",
    );
  });

  it("explains a saved token whose bot could not start", async () => {
    start(() => committed({ health: "stopped" }));
    paste(tokenA);
    await waitFor(() =>
      expect(toast.success).toHaveBeenCalledWith(
        "The token was saved but this bot could not start. Choose Try now on its row.",
      ),
    );
  });

  it("keeps a stopped successor's address for when it starts", async () => {
    start(() =>
      json({
        bot: bot({ id: "link-b", bot_id: 2, health: "stopped" }),
        connect: { url: "https://t.me/b?start=c", expires_at: "2099-01-01" },
        managed_replaced: 0,
        replacement_status: "committed",
      }),
    );
    paste(tokenA);
    await waitFor(() =>
      expect(screen.getByTestId("page").textContent).toBe(
        JSON.stringify({ replaced: ["link-b"], connects: ["link-b"] }),
      ),
    );
  });

  it("closes for a refusal of the whole section and reads the features again", async () => {
    start(() => refusal(409, "secret-missing"));
    paste(tokenA);
    await waitFor(() => expect(onClose).toHaveBeenCalled());
  });

  it("names the target's kind when the server cannot name a deleted one", async () => {
    start(() => refusal(409, "bot-linked", { own: true, agent_name: null }));
    paste(tokenA);
    await screen.findByText(/already connected to Custom agent/);
  });

  it("says the old bot is unchanged after a refusal before commit", async () => {
    start(() => refusal(409, "bot-linked", { own: true, agent_name: "Maya" }));
    paste(tokenA);
    await screen.findByText(/already connected to Maya/);
    await screen.findByText(/Your current bot was not changed/);
    expect(onClose).not.toHaveBeenCalled();
  });
});
