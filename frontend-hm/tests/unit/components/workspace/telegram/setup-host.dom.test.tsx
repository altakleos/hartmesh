import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { act, cleanup, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";

// The chunk never arrives here, so what a slow phone sees is what renders.
rs.mock("next/dynamic", () => ({
  default: (
    _load: unknown,
    { loading }: { loading: (props: object) => ReactNode },
  ) => loading,
}));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: { common: { loading: "Loading..." } } }),
}));

import {
  holdTelegramConnect,
  openTelegramSetup,
  resetTelegramPageState,
  telegramConnectEpoch,
  useTelegramPageState,
} from "@/components/workspace/telegram/setup-dialog-store";
import { TelegramSetupHost } from "@/components/workspace/telegram/setup-host";

function Store() {
  const { setup, connects } = useTelegramPageState();
  return (
    <output data-testid="store">
      {JSON.stringify({ open: setup !== null, held: connects.size })}
    </output>
  );
}

afterEach(() => {
  cleanup();
  resetTelegramPageState();
});

describe("the set-up host", () => {
  it("shows a press was taken over Settings, which covers the page it is rendered in", () => {
    const page = document.createElement("main");
    document.body.append(page);
    const settings = document.createElement("div");
    settings.dataset.testid = "settings";
    document.body.append(settings);
    render(<TelegramSetupHost />, { container: page });

    act(() => openTelegramSetup({ kind: "chat" }));

    const overlay = screen.getByRole("status").parentElement!;
    expect(screen.getByRole("status").textContent).toBe("Loading...");
    expect(overlay.parentElement).toBe(document.body);
    expect(
      settings.compareDocumentPosition(overlay) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    settings.remove();
    page.remove();
  });

  it("leaves nothing of one account for the next on a shared tab", () => {
    const { unmount } = render(<TelegramSetupHost />);
    render(<Store />);
    const epoch = telegramConnectEpoch();
    act(() => {
      openTelegramSetup({ kind: "custom_agent", name: "someone-elses-agent" });
      holdTelegramConnect(
        "link-a",
        { url: "https://t.me/x?start=c", expires_at: "2099-01-01T00:00:00Z" },
        epoch,
      );
    });
    expect(screen.getByTestId("store").textContent).toBe(
      JSON.stringify({ open: true, held: 1 }),
    );
    act(() => unmount());
    expect(screen.getByTestId("store").textContent).toBe(
      JSON.stringify({ open: false, held: 0 }),
    );
    expect(telegramConnectEpoch()).toBeGreaterThan(epoch);
  });
});
