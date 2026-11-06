/** Product identity and explicit legacy selection for browser-mocked suites. */
import { createServer } from "node:http";

const port = Number(process.env.E2E_PRODUCT_GATEWAY_PORT ?? "3019");
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("E2E_PRODUCT_GATEWAY_PORT must be a TCP port");
}
const server = createServer((request, response) => {
  response.setHeader("Content-Type", "application/json");
  response.setHeader("Cache-Control", "no-store");
  if (request.method === "GET" && request.url === "/api/branding/presentation") {
    response.end(JSON.stringify({ schema_version: 1, mode: "legacy" }));
    return;
  }
  if (
    request.method === "GET" &&
    request.url === "/api/v1/auth/product-identity"
  ) {
    response.end(
      JSON.stringify({
        schema_version: 1,
        mode: "default",
        name: "HartMesh",
        product: { name: "HartMesh", logo: null, browser_icon: null },
        provider: null,
      }),
    );
    return;
  }
  response.writeHead(503);
  response.end(
    JSON.stringify({ detail: "Gateway unavailable in browser-mocked suite" }),
  );
});
server.listen(port, "127.0.0.1");
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    server.closeAllConnections();
    server.close();
  });
}
