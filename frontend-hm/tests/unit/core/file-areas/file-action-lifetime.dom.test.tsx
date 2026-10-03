import { afterEach, expect, test } from "@rstest/core";
import { cleanup, renderHook } from "@testing-library/react";
import { StrictMode, type PropsWithChildren } from "react";

import {
  FileActionLifetimeProvider,
  useFileActionLifetime,
} from "@/core/file-areas/file-action-lifetime";

afterEach(cleanup);

test("StrictMode setup uses a live signal and retiring the account aborts only that account", () => {
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <StrictMode>
        <FileActionLifetimeProvider>{children}</FileActionLifetimeProvider>
      </StrictMode>
    );
  }
  const first = renderHook(() => useFileActionLifetime(), { wrapper: Wrapper });
  const firstSignal = first.result.current.signal;
  expect(first.result.current.active).toBe(true);
  expect(firstSignal.aborted).toBe(false);
  first.unmount();
  expect(firstSignal.aborted).toBe(true);
  const second = renderHook(() => useFileActionLifetime(), {
    wrapper: Wrapper,
  });
  expect(second.result.current.signal).not.toBe(firstSignal);
  expect(second.result.current.signal.aborted).toBe(false);
});
