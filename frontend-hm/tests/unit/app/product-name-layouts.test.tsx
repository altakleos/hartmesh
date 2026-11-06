import { describe, expect, rs, test } from "@rstest/core";
import { isValidElement, type ReactElement } from "react";

import { RuntimeProductProvider } from "@/core/product/context";

rs.mock("@/core/product/server", () => ({
  getServerSideProductIdentity: rs.fn(async () => ({ name: "Acme Assist" })),
  productMetadata: rs.fn(async () => ({
    title: "Acme Assist",
    icons: { icon: "/favicon.ico" },
  })),
}));
rs.mock("@/core/i18n/server", () => ({
  detectLocaleServer: rs.fn(async () => "en-US"),
}));
rs.mock("@/core/auth/server", () => ({
  getServerSideUser: rs.fn(async () => ({
    tag: "authenticated",
    user: { id: "u1", email: "owner@example.com" },
  })),
}));
rs.mock("next/navigation", () => ({
  redirect: rs.fn(() => {
    throw new Error("NEXT_REDIRECT");
  }),
}));

type ProviderProps = { initialLocale: string; snapshot: { name: string } };

function providerProps(tree: unknown): ProviderProps {
  expect(isValidElement(tree)).toBe(true);
  const element = tree as ReactElement<ProviderProps>;
  expect(element.type).toBe(RuntimeProductProvider);
  return element.props;
}

// Each layout is where a page learns the deployment's name: the tab title
// comes from generateMetadata, every visible string from the provider.
describe("the deployment's product name reaches every layout", () => {
  test("sign-in and setup", async () => {
    const { getServerSideUser } = await import("@/core/auth/server");
    rs.mocked(getServerSideUser).mockResolvedValueOnce({
      tag: "unauthenticated",
    });
    const layout = await import("@/app/(auth)/layout");

    await expect(layout.generateMetadata()).resolves.toEqual({
      title: "Acme Assist",
      icons: { icon: "/favicon.ico" },
    });
    const props = providerProps(await layout.default({ children: null }));
    expect(props.snapshot.name).toBe("Acme Assist");
  });

  test("the workspace", async () => {
    const layout = await import("@/app/workspace/layout");

    await expect(layout.generateMetadata()).resolves.toEqual({
      title: "Acme Assist",
      icons: { icon: "/favicon.ico" },
    });
    const props = providerProps(await layout.default({ children: null }));
    expect(props.snapshot.name).toBe("Acme Assist");
  });

  test("the artifact window", async () => {
    const layout = await import("@/app/artifacts/view/layout");

    await expect(layout.generateMetadata()).resolves.toEqual({
      title: "Acme Assist",
      icons: { icon: "/favicon.ico" },
    });
    const props = providerProps(await layout.default({ children: null }));
    expect(props.snapshot.name).toBe("Acme Assist");
  });
});
