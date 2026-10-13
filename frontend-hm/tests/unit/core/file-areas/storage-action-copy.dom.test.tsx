import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, renderHook, screen } from "@testing-library/react";
import type { PropsWithChildren, ReactNode } from "react";

const viewer = rs.hoisted(() => ({ role: "admin" }));
const errorToast = rs.hoisted(() => rs.fn(() => 1));
rs.mock("sonner", () => ({ toast: { error: errorToast } }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: "viewer", system_role: viewer.role } }),
}));
rs.mock("@/core/features", () => ({
  useBranding: () => ({
    providerName: "Acme",
    supportURL: "https://support.example.com",
  }),
}));

import { FilesStateNotice } from "@/components/workspace/chats/files-state-notice";
import { useStorageCopy } from "@/core/file-areas/storage-copy";
import { readStorageRefusal } from "@/core/file-areas/storage-state";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

function Wrapper({ children }: PropsWithChildren) {
  return (
    <I18nContext.Provider
      value={{ t: enUS, locale: "en-US", setLocale: () => undefined }}
    >
      {children}
    </I18nContext.Provider>
  );
}
afterEach(() => {
  cleanup();
  errorToast.mockClear();
  viewer.role = "admin";
});
describe("action and chip refusal support", () => {
  it.each(["files", "shared"] as const)(
    "keeps partial %s success and exposes quota remedies with the administrator support link",
    (area) => {
      const { result } = renderHook(() => useStorageCopy(area), {
        wrapper: Wrapper,
      });
      result.current.error(
        { status: 507, code: "files-full" },
        "fallback",
        "One file was kept",
      );
      const args = errorToast.mock.calls[0] as unknown as [
        string,
        { description: ReactNode },
      ];
      expect(args[0]).toBe("One file was kept");
      render(<Wrapper>{args[1].description}</Wrapper>);
      expect(
        screen.getByText(
          area === "files"
            ? enUS.files.saveFull("Acme")
            : enUS.shared.full("Acme"),
        ),
      ).toBeTruthy();
      expect(screen.getByRole("link").getAttribute("href")).toBe(
        "https://support.example.com/",
      );
    },
  );
  it("shows support on a refused upload chip without any internal storage geometry", () => {
    render(
      <Wrapper>
        <FilesStateNotice
          area="upload"
          filename="statement.txt"
          refusal={readStorageRefusal({ status: 507, code: "files-full" })!}
        />
      </Wrapper>,
    );
    expect(
      screen.getByText(enUS.uploads.filesFull("statement.txt", "Acme")),
    ).toBeTruthy();
    expect(screen.getByRole("link")).toBeTruthy();
    expect(screen.getByRole("alert").textContent).not.toMatch(
      /\b(?:Space|slot|pool|GiB)\b/,
    );
  });
  it("keeps unverified action endings for a user without exposing provider support", () => {
    viewer.role = "user";
    const { result } = renderHook(() => useStorageCopy("files"), {
      wrapper: Wrapper,
    });
    result.current.error(
      { status: 503, code: "storage-unavailable", reason: "space-unverified" },
      "fallback",
    );
    const args = errorToast.mock.calls[0] as unknown as [
      string,
      { description: ReactNode },
    ];
    expect(args[0]).toBe(enUS.chat.myFilesUnchecked);
    render(<Wrapper>{args[1].description}</Wrapper>);
    expect(
      screen.getByText(enUS.agentInstances.unavailable.askAdmin),
    ).toBeTruthy();
    expect(screen.queryByRole("link")).toBeNull();
  });
});
