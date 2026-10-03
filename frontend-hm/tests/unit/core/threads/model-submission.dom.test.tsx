import { expect, rs, test } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import { createElement, type ReactNode } from "react";

import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { DEFAULT_LOCAL_SETTINGS } from "@/core/settings/local";

const sdk = rs.hoisted(() => ({
  submit: rs.fn(async (_input: unknown, _options: unknown) => undefined),
}));
rs.mock("@langchain/langgraph-sdk/react", () => ({
  useStream: () => ({
    isLoading: false,
    messages: [],
    values: {},
    submit: sdk.submit,
    stop: () => undefined,
  }),
}));

test("sends the composer's resolved model and mode even before stored selection updates", async () => {
  const { useThreadStream } = await import("@/core/threads/hooks");
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const wrapper = ({ children }: { children: ReactNode }) =>
    createElement(
      QueryClientProvider,
      { client },
      createElement(
        I18nContext.Provider,
        { value: { locale: "en-US", setLocale: () => undefined, t: enUS } },
        children,
      ),
    );
  const { result, unmount } = renderHook(
    () =>
      useThreadStream({
        context: {
          ...DEFAULT_LOCAL_SETTINGS.context,
          model_name: "removed-provider-model",
          mode: "ultra",
        },
        isMock: true,
        threadId: "model-refresh",
      }),
    { wrapper },
  );
  try {
    await act(async () => {
      await result.current.sendMessage(
        "model-refresh",
        { text: "Keep my draft", files: [] },
        undefined,
        {
          modelContext: {
            model_name: "available-model",
            mode: "flash",
            reasoning_effort: "minimal",
          },
        },
      );
    });
    expect(sdk.submit.mock.calls[0]?.[1]).toMatchObject({
      context: {
        model_name: "available-model",
        mode: "flash",
        thinking_enabled: false,
        subagent_enabled: false,
        is_plan_mode: false,
        reasoning_effort: "minimal",
      },
    });
  } finally {
    unmount();
    client.clear();
  }
});
