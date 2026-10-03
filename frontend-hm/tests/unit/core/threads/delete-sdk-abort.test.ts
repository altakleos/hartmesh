import { createServer } from "node:http";

import { Client } from "@langchain/langgraph-sdk/client";
import { expect, test } from "@rstest/core";
test("installed SDK retries cannot send another request with an aborted account signal", async () => {
  const controller = new AbortController();
  let requests = 0;
  const server = createServer((_request, response) => {
    requests += 1;
    response.writeHead(500, { "content-type": "application/json" });
    response.end(JSON.stringify({ detail: "Retryable failure" }));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("Missing test port");
  const client = new Client({
    apiUrl: `http://127.0.0.1:${address.port}`,
    callerOptions: {
      maxRetries: 1,
      onFailedResponseHook: async () => {
        controller.abort();
        return true;
      },
    },
  });
  try {
    await expect(
      client.threads.delete("thread", { signal: controller.signal }),
    ).rejects.toThrow();
    expect(controller.signal.aborted).toBe(true);
    expect(requests).toBe(1);
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
