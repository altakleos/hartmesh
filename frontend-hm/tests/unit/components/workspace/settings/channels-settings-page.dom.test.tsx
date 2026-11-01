import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({
  known: true,
  providers: [] as { provider: string; enabled: boolean }[],
}));

rs.mock("@/core/features/hooks", () => ({
  useTelegram: () => ({ known: state.known }),
}));
rs.mock("@/core/channels/hooks", () => ({
  useChannelProviders: () => ({
    enabled: true,
    providers: state.providers,
    isLoading: false,
    error: null,
  }),
  useChannelConnections: () => ({
    connections: [],
    isLoading: false,
    error: null,
  }),
  useConfigureChannelProvider: () => ({ mutate: rs.fn(), isPending: false }),
  useConnectChannelProvider: () => ({ mutate: rs.fn(), isPending: false }),
  useDisconnectChannelConnection: () => ({ mutate: rs.fn(), isPending: false }),
  useDisconnectChannelProvider: () => ({ mutate: rs.fn(), isPending: false }),
}));
rs.mock("@/components/workspace/telegram/bots-section", () => ({
  TelegramBotsSection: () => <div>the Telegram section</div>,
}));

import { ChannelsSettingsPage } from "@/components/workspace/settings/channels-settings-page";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

function renderPage() {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <ChannelsSettingsPage />
    </I18nContext.Provider>,
  );
}

afterEach(() => {
  cleanup();
  state.known = true;
  state.providers = [];
});

describe("ChannelsSettingsPage", () => {
  it("shows the Telegram section and not the sentence that channels are off", () => {
    renderPage();
    screen.getByText("the Telegram section");
    expect(screen.queryByText(enUS.settings.channels.disabled)).toBeNull();
  });

  it("keeps the old page without an answer about Telegram", () => {
    state.known = false;
    renderPage();
    expect(screen.queryByText("the Telegram section")).toBeNull();
    screen.getByText(enUS.settings.channels.disabled);
  });
});
