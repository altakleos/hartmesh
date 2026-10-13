import { afterEach, describe, expect, it } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

import { ExecutionNotice } from "@/components/workspace/instances/execution-notice";
import { STORAGE_REASONS } from "@/core/features/api";
import { STORAGE_REFERENCES } from "@/core/file-areas/storage-state";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";

afterEach(cleanup);
function notice(
  props: React.ComponentProps<typeof ExecutionNotice>,
  chinese = false,
) {
  return render(
    <I18nContext.Provider
      value={{
        locale: chinese ? "zh-CN" : "en-US",
        setLocale: () => undefined,
        t: chinese ? zhCN : enUS,
      }}
    >
      <ExecutionNotice {...props} />
    </I18nContext.Provider>,
  );
}

describe("file execution notices", () => {
  it.each(STORAGE_REASONS)(
    "shows a user file-oriented copy without deployment details for %s",
    (reason) => {
      notice({
        reason,
        administrator: false,
        provider: "Acme",
        supportURL: "https://support.example.com",
      });
      expect(screen.getByRole("alert").textContent).toBe(
        enUS.chat.filesUnavailable + enUS.agentInstances.unavailable.askAdmin,
      );
      expect(screen.queryByRole("link")).toBeNull();
      expect(screen.getByRole("alert").textContent).not.toMatch(
        /Space|slot|pool|GiB|AI employee/i,
      );
    },
  );

  it.each(STORAGE_REASONS)(
    "gives an administrator the safe support reference for %s",
    (reason) => {
      notice({
        reason,
        administrator: true,
        provider: "Acme",
        supportURL: "https://support.example.com",
      });
      expect(
        screen.getByText(enUS.agentInstances.unavailable.next("Acme")),
      ).toBeTruthy();
      expect(
        screen.getByText(
          enUS.agentInstances.unavailable.reference(STORAGE_REFERENCES[reason]),
        ),
      ).toBeTruthy();
      const link = screen.getByRole("link");
      expect(link.getAttribute("href")).toBe("https://support.example.com/");
      expect(link.getAttribute("rel")).toContain("noreferrer");
      expect(screen.getByRole("alert").textContent).not.toMatch(
        /Space|slot|pool|GiB|AI employee/i,
      );
    },
  );

  it("uses Chinese, preserves unchecked files and rejects an unsafe support link", () => {
    notice(
      {
        reason: "space-unverified",
        message: zhCN.shared.unchecked,
        administrator: true,
        provider: "Acme",
        supportURL: "javascript:alert(1)",
      },
      true,
    );
    expect(screen.getByText(zhCN.shared.unchecked)).toBeTruthy();
    expect(
      screen.getByText(
        zhCN.agentInstances.unavailable.reference("files-unchecked"),
      ),
    ).toBeTruthy();
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("keeps unknown discovery file-oriented with no invented reference", () => {
    notice({ reason: null, administrator: false, provider: "Acme" });
    expect(screen.getByRole("alert").textContent).toContain(
      enUS.chat.filesUnavailable,
    );
    expect(screen.getByRole("alert").textContent).not.toMatch(
      /Reference|AI employee|AI assistant/,
    );
  });
});
