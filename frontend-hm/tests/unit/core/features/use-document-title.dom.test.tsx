import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, waitFor } from "@testing-library/react";

const featureState = rs.hoisted(() => ({
  answered: true,
  companyName: "Example Customer",
}));
rs.mock("@tanstack/react-query", () => ({
  useQuery: () => ({
    data: featureState.answered
      ? { companyName: featureState.companyName, hasLogo: true }
      : undefined,
  }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => ({}) }));

import { useDocumentTitle } from "@/core/features/hooks";
import { useProductName } from "@/core/i18n/context";
import { RuntimeProductProvider } from "@/core/product/context";
import {
  DEFAULT_IDENTITY,
  type ProductIdentity,
} from "@/core/product/identity";

const logo = "data:image/png;base64,iVBORw0KGgo=";
const configured: ProductIdentity = {
  mode: "configured",
  schema_version: 1,
  name: "Example Product",
  product: { name: "Example Product", logo, browser_icon: logo },
  provider: {
    name: "Example Provider",
    logo,
    support_url: "https://support.example.test/",
  },
};

function Page({ name }: { name: string }) {
  useDocumentTitle(name, useProductName());
  return null;
}

afterEach(() => {
  cleanup();
  featureState.answered = true;
  featureState.companyName = "Example Customer";
  document.title = "";
});

describe("useDocumentTitle", () => {
  it("keeps the mounted page title when streamed route metadata arrives late", async () => {
    const view = render(
      <RuntimeProductProvider initialLocale="en-US" snapshot={DEFAULT_IDENTITY}>
        <Page name="Chats" />
      </RuntimeProductProvider>,
    );
    const metadata = document.createElement("title");
    metadata.textContent = "HartMesh";
    document.head.prepend(metadata);
    await waitFor(() => expect(document.title).toBe("Chats - HartMesh"));

    view.unmount();
    metadata.textContent = "Sign in";
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(document.title).toBe("Sign in");
    metadata.remove();
  });

  it.each([
    DEFAULT_IDENTITY,
    {
      schema_version: 1,
      mode: "default",
      name: "Custom UI name",
      product: { name: "Custom UI name", logo: null, browser_icon: null },
      provider: null,
    } satisfies ProductIdentity,
    configured,
  ])(
    "uses $name as the suffix across navigation and company changes",
    (identity) => {
      const tree = (name: string) => (
        <RuntimeProductProvider initialLocale="en-US" snapshot={identity}>
          <Page name={name} />
        </RuntimeProductProvider>
      );
      featureState.answered = false;
      const view = render(tree("Chats"));
      expect(document.title).toBe("Chats");
      featureState.answered = true;
      view.rerender(tree("Chats"));
      expect(document.title).toBe(`Chats - ${identity.name}`);
      featureState.companyName = "Another Customer";
      view.rerender(tree("My files"));
      expect(document.title).toBe(`My files - ${identity.name}`);
    },
  );
});
