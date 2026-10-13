/** Exercise the production download observer with real streamed HTTP responses. */
import { readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";

import { expect, test } from "@playwright/test";
import ts from "typescript";

let server: Server;
let origin: string;
let script: string;
let requests: number;
test.beforeAll(async () => {
  script = ts.transpileModule(
    await readFile("src/core/file-areas/native-download.ts", "utf8"),
    {
      compilerOptions: {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ES2022,
      },
    },
  ).outputText;
  server = createServer((req, res) => {
    if (req.url === "/observer.js") {
      res.writeHead(200, { "Content-Type": "text/javascript" });
      res.end(script);
      return;
    }
    if (req.url?.split("?")[0] === "/") {
      res.writeHead(200, { "Content-Type": "text/html" });
      res.end(`<button id="download">Download</button><script type="module">
        import { observeNativeDownload } from '/observer.js';
        window.refusals = []; window.inconclusive = 0;
        window.lifetime = new AbortController();
        document.querySelector('button').onclick = () => observeNativeDownload({
          url: location.origin + new URLSearchParams(location.search).get('file'),
          signal: window.lifetime.signal, timeoutMs: 100,
          onRefusal: value => window.refusals.push(value),
          onInconclusive: () => window.inconclusive++,
        });
        window.ready = true;
      </script>`);
      return;
    }
    requests++;
    if (req.url === "/redirect") {
      res.writeHead(302, { Location: "/busy" });
      res.end();
    } else if (req.url === "/busy") {
      res.writeHead(409, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({ detail: { code: "storage-busy", message: "busy" } }),
      );
    } else if (req.url === "/unknown") {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ detail: "unexpected" }));
    } else if (req.url === "/script") {
      res.writeHead(200, { "Content-Type": "text/html" });
      res.end("<script>parent.compromised = true</script>");
    } else {
      const start = () => {
        res.writeHead(200, {
          "Content-Type": "application/octet-stream",
          "Content-Disposition": 'attachment; filename="streamed.txt"',
        });
        res.write("first\n");
        setTimeout(() => res.end("last\n"), 300);
      };
      if (req.url === "/slow-headers") setTimeout(start, 300);
      else start();
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("Missing test port");
  origin = `http://127.0.0.1:${address.port}`;
});
test.afterAll(async () => {
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
});
test.beforeEach(() => {
  requests = 0;
});

test("actual GET busy refusal is observed without a second request", async ({
  page,
}) => {
  await page.goto(`${origin}/?file=/busy`);
  await page.waitForFunction(
    () => (window as unknown as { ready: boolean }).ready,
  );
  await page.getByRole("button", { name: "Download" }).click();
  await expect
    .poll(() =>
      page.evaluate(
        () => (window as unknown as { refusals: unknown[] }).refusals,
      ),
    )
    .toEqual([
      { status: 409, detail: { code: "storage-busy", message: "busy" } },
    ]);
  expect(requests).toBe(1);
});

for (const file of ["/streamed", "/slow-headers"]) {
  test(`native attachment ${file} finishes beyond the observer deadline`, async ({
    page,
  }) => {
    await page.goto(`${origin}/?file=${file}`);
    await page.waitForFunction(
      () => (window as unknown as { ready: boolean }).ready,
    );
    const pending = page.waitForEvent("download");
    await page.getByRole("button", { name: "Download" }).click();
    const download = await pending;
    expect(await download.failure()).toBeNull();
    const path = await download.path();
    expect(await readFile(path, "utf8")).toBe("first\nlast\n");
    expect(requests).toBe(1);
    expect(
      await page.evaluate(
        () => (window as unknown as { refusals: unknown[] }).refusals,
      ),
    ).toEqual([]);
  });
}

for (const file of ["/redirect", "/unknown", "/script"]) {
  test(`${file} is inconclusive and executes no response script`, async ({
    page,
  }) => {
    await page.goto(`${origin}/?file=${file}`);
    await page.waitForFunction(
      () => (window as unknown as { ready: boolean }).ready,
    );
    await page.getByRole("button", { name: "Download" }).click();
    await expect
      .poll(() =>
        page.evaluate(
          () => (window as unknown as { inconclusive: number }).inconclusive,
        ),
      )
      .toBe(1);
    expect(
      await page.evaluate(
        () => (window as unknown as { refusals: unknown[] }).refusals,
      ),
    ).toEqual([]);
    expect(
      await page.evaluate(
        () => (window as unknown as { compromised?: boolean }).compromised,
      ),
    ).toBeUndefined();
    expect(requests).toBe(file === "/redirect" ? 2 : 1);
  });
}

test("retired account cancels the old observer without reviving it", async ({
  page,
}) => {
  await page.goto(`${origin}/?file=/slow-headers`);
  await page.waitForFunction(
    () => (window as unknown as { ready: boolean }).ready,
  );
  await page.getByRole("button", { name: "Download" }).click();
  await page.evaluate(() => {
    const owner = window as unknown as { lifetime: AbortController };
    owner.lifetime.abort();
    owner.lifetime = new AbortController();
  });
  await expect(page.locator("iframe")).toHaveCount(0);
  expect(
    await page.evaluate(
      () => (window as unknown as { refusals: unknown[] }).refusals,
    ),
  ).toEqual([]);
});
