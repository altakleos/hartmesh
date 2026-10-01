import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render } from "@testing-library/react";

const branding: { companyName: string | null; isLoading: boolean } = {
  companyName: null,
  isLoading: false,
};

rs.mock("@tanstack/react-query", () => ({
  useQuery: () => ({
    data: branding.isLoading
      ? undefined
      : { companyName: branding.companyName, hasLogo: false },
  }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => ({}) }));

import { useDocumentTitle } from "@/core/features/hooks";

function Page({ page }: { page: string }) {
  useDocumentTitle(page, "DeerFlow");
  return null;
}

afterEach(() => {
  cleanup();
  branding.companyName = null;
  branding.isLoading = false;
  document.title = "";
});

/**
 * The tab is read as often as the header: it carries the company's name where
 * one is named, the product's where none is, and only the page's own name
 * until the deployment has answered.
 */
describe("useDocumentTitle", () => {
  it("names the company after the page", () => {
    branding.companyName = "Example Services Co.";
    render(<Page page="Chats" />);
    expect(document.title).toBe("Chats - Example Services Co.");
  });

  it("names the product where no company is named", () => {
    render(<Page page="Chats" />);
    expect(document.title).toBe("Chats - DeerFlow");
  });

  it("carries only the page until the deployment has answered", () => {
    branding.isLoading = true;
    render(<Page page="Chats" />);
    expect(document.title).toBe("Chats");
  });
});
