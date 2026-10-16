import { afterEach, expect, test, rs } from "@rstest/core";

import { retryFileRead } from "@/core/agent-instances/file-reads";
import { StorageRequestError } from "@/core/spaces/api";
afterEach(() => {
  rs.useRealTimers();
});
test("retries a conflicting read once after exactly three seconds", async () => {
  rs.useFakeTimers();
  const read = rs
    .fn<() => Promise<string>>()
    .mockRejectedValueOnce(new StorageRequestError(409, "conflict"))
    .mockResolvedValue("ready");
  const pending = retryFileRead(read, new AbortController().signal);
  await rs.advanceTimersByTimeAsync(2999);
  expect(read).toHaveBeenCalledTimes(1);
  await rs.advanceTimersByTimeAsync(1);
  expect(await pending).toBe("ready");
  expect(read).toHaveBeenCalledTimes(2);
});
test("a second conflict fails without another automatic read", async () => {
  rs.useFakeTimers();
  const error = new StorageRequestError(409, "conflict");
  const read = rs.fn<() => Promise<string>>().mockRejectedValue(error);
  const answer = retryFileRead(read, new AbortController().signal).catch(
    (failure: unknown) => failure,
  );
  await rs.advanceTimersByTimeAsync(3000);
  expect(await answer).toBe(error);
  await rs.advanceTimersByTimeAsync(3000);
  expect(read).toHaveBeenCalledTimes(2);
});
test("retirement cancels the delayed retry", async () => {
  rs.useFakeTimers();
  const controller = new AbortController();
  const read = rs
    .fn<() => Promise<string>>()
    .mockRejectedValue(new StorageRequestError(409, "conflict"));
  const answer = retryFileRead(read, controller.signal).catch(
    (failure: unknown) => failure,
  );
  await rs.advanceTimersByTimeAsync(1000);
  controller.abort();
  expect(await answer).toBeInstanceOf(DOMException);
  await rs.advanceTimersByTimeAsync(5000);
  expect(read).toHaveBeenCalledTimes(1);
});
test.each([403, 404, 500])("a %s read is not retried", async (status) => {
  const error = new StorageRequestError(status, "unavailable");
  const read = rs.fn<() => Promise<string>>().mockRejectedValue(error);
  await expect(retryFileRead(read, new AbortController().signal)).rejects.toBe(
    error,
  );
  expect(read).toHaveBeenCalledTimes(1);
});
