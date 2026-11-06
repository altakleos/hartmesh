import {
  afterEach,
  beforeEach,
  describe,
  expect,
  rs,
  test,
} from "@rstest/core";

const ENV_KEYS = [
  "DEER_FLOW_INTERNAL_GATEWAY_BASE_URL",
  "NEXT_PUBLIC_STATIC_WEBSITE_ONLY",
] as const;

type EnvSnapshot = Partial<
  Record<(typeof ENV_KEYS)[number], string | undefined>
>;

function setEnv(key: (typeof ENV_KEYS)[number], value: string | undefined) {
  const env = process.env as Record<string, string | undefined>;
  if (value === undefined) {
    delete env[key];
  } else {
    env[key] = value;
  }
}

async function loadFreshProductServer() {
  rs.resetModules();
  return await import("@/core/product/server");
}

describe("getServerSideProductName", () => {
  let saved: EnvSnapshot;

  beforeEach(() => {
    saved = {};
    for (const key of ENV_KEYS) {
      saved[key] = process.env[key];
    }
    setEnv("DEER_FLOW_INTERNAL_GATEWAY_BASE_URL", "http://gateway.test:8001");
    setEnv("NEXT_PUBLIC_STATIC_WEBSITE_ONLY", undefined);
  });

  afterEach(() => {
    for (const key of ENV_KEYS) {
      setEnv(key, saved[key]);
    }
    rs.unstubAllGlobals();
  });

  test("reads the deployment's name from the Gateway's public route", async () => {
    const fetchSpy = rs.fn(() =>
      Promise.resolve(
        Response.json({
          schema_version: 1,
          mode: "default",
          name: "Acme Assist",
          product: { name: "Acme Assist", logo: null, browser_icon: null },
          provider: null,
        }),
      ),
    );
    rs.stubGlobal("fetch", fetchSpy);

    const { getServerSideProductName } = await loadFreshProductServer();

    await expect(getServerSideProductName()).resolves.toBe("Acme Assist");
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0] as unknown as [
      string,
      RequestInit,
    ];
    expect(url).toBe("http://gateway.test:8001/api/v1/auth/product-identity");
    // An operator's edit reaches the next page load, not a cached one.
    expect(init.cache).toBe("no-store");
    expect(init.signal).toBeInstanceOf(AbortSignal);
  });

  test("a Gateway error reports identity unavailable", async () => {
    rs.stubGlobal(
      "fetch",
      rs.fn(() =>
        Promise.resolve(Response.json({ name: "Error Page" }, { status: 503 })),
      ),
    );

    const { getServerSideProductName } = await loadFreshProductServer();

    await expect(getServerSideProductName()).resolves.toBe(
      "Service identity unavailable",
    );
  });

  test("an unreachable Gateway reports identity unavailable", async () => {
    rs.stubGlobal(
      "fetch",
      rs.fn(() => Promise.reject(new DOMException("Aborted", "AbortError"))),
    );

    const { getServerSideProductName } = await loadFreshProductServer();

    await expect(getServerSideProductName()).resolves.toBe(
      "Service identity unavailable",
    );
  });

  test("a body that is not JSON reports identity unavailable", async () => {
    rs.stubGlobal(
      "fetch",
      rs.fn(() => Promise.resolve(new Response("<html>proxy</html>"))),
    );

    const { getServerSideProductName } = await loadFreshProductServer();

    await expect(getServerSideProductName()).resolves.toBe(
      "Service identity unavailable",
    );
  });

  test("the static website never asks a Gateway", async () => {
    setEnv("NEXT_PUBLIC_STATIC_WEBSITE_ONLY", "true");
    const fetchSpy = rs.fn(() => {
      throw new Error("fetch should not be called in static website mode");
    });
    rs.stubGlobal("fetch", fetchSpy);

    const { getServerSideProductName } = await loadFreshProductServer();

    await expect(getServerSideProductName()).resolves.toBe("HartMesh");
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
