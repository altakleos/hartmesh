import { expect, rs, test } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import { createElement, type ReactNode } from "react";

import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { DEFAULT_LOCAL_SETTINGS } from "@/core/settings/local";

const sdk = rs.hoisted(() => ({
  isLoading: true,
  finishStop: undefined as (() => void) | undefined,
  submit: rs.fn(),
}));

rs.mock("@langchain/langgraph-sdk/react", () => ({
  useStream: () => ({
    isLoading: sdk.isLoading,
    messages: [],
    values: {},
    submit: sdk.submit,
    stop: () => {
      sdk.isLoading = false;
      return new Promise<void>((resolve) => {
        sdk.finishStop = resolve;
      });
    },
  }),
}));

test("keeps the composer busy and preserves the next draft until Stop drains", async () => {
  const { useThreadStream } = await import("@/core/threads/hooks");
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const wrapper = ({ children }: { children: ReactNode }) =>
    createElement(
      QueryClientProvider,
      { client: queryClient },
      createElement(
        I18nContext.Provider,
        { value: { locale: "en-US", setLocale: () => undefined, t: enUS } },
        children,
      ),
    );
  const { result, rerender, unmount } = renderHook(
    () =>
      useThreadStream({
        context: DEFAULT_LOCAL_SETTINGS.context,
        isMock: true,
        threadId: "thread-stop",
      }),
    { wrapper },
  );
  let stopping!: Promise<void>;
  act(() => {
    stopping = Promise.resolve(result.current.thread.stop());
  });
  await act(async () => {
    await Promise.resolve();
    rerender();
  });
  expect(sdk.isLoading).toBe(false);
  expect(result.current.thread.isLoading).toBe(true);
  const onSent = rs.fn();
  await act(async () => {
    await result.current.sendMessage(
      "thread-stop",
      { text: "My next message", files: [] },
      undefined,
      { onSent },
    );
  });
  expect(onSent).not.toHaveBeenCalled();
  expect(sdk.submit).not.toHaveBeenCalled();
  await act(async () => {
    sdk.finishStop?.();
    await stopping;
  });
  expect(result.current.thread.isLoading).toBe(false);
  unmount();
  queryClient.clear();
});
