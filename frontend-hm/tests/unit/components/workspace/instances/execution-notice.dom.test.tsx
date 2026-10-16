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

describe("agent execution notices", () => {
  it("gives a deployment hold precedence over busy files", () => {
    notice({
      area: "agent",
      reason: "operator-hold",
      busy: true,
      name: "Researcher",
      administrator: false,
      provider: "Acme",
    });
    expect(screen.getByRole("alert").textContent).toBe(
      "AI assistants and AI employees are paused by Acme.",
    );
  });
  it("explains an operator hold without asking the reader to fix it", () => {
    notice({
      area: "agent",
      reason: "operator-hold",
      administrator: true,
      provider: "Acme",
      supportURL: "https://support.example.com",
    });
    expect(screen.getByRole("alert").textContent).toBe(
      "AI assistants and AI employees are paused by Acme.",
    );
    expect(screen.queryByRole("link")).toBeNull();
  });
  it("uses the provider fallback for a hold", () => {
    notice({ area: "agent", reason: "operator-hold", administrator: false });
    expect(screen.getByRole("alert").textContent).toBe(
      "AI assistants and AI employees are paused by your service provider.",
    );
  });
  it.each(["personal", "company"] as const)(
    "shows only the not-offered sentence for %s custody",
    (kind) => {
      notice({
        area: "agent",
        kind,
        reason: "agents-none",
        administrator: true,
      });
      expect(screen.getByRole("alert").textContent).toBe(
        kind === "personal"
          ? "AI assistants are not offered here."
          : "AI employees are not offered here.",
      );
    },
  );
  it("keeps an unknown agent reason generic without inventing a reference", () => {
    notice({ area: "agent", reason: null, administrator: true });
    expect(screen.getByRole("alert").textContent).toBe(
      enUS.sidebar.agentsUnavailable,
    );
  });
  it.each([
    ["host-replaced", "server-moved"],
    ["space-unverified", "files-unchecked"],
  ] as const)(
    "gives an administrator the named agent and safe reference for %s",
    (reason, reference) => {
      notice({
        area: "agent",
        name: "Researcher",
        reason,
        administrator: true,
        provider: "Acme",
        supportURL: "https://support.example.com",
      });
      expect(screen.getByRole("alert").textContent).toContain("Researcher");
      if (reason === "host-replaced") {
        expect(screen.getByRole("alert").textContent).toContain(
          "Acme has to confirm the move",
        );
        expect(screen.getByRole("alert").textContent).not.toContain(
          "{provider}",
        );
      }
      expect(
        screen.getByText(enUS.agentInstances.unavailable.reference(reference)),
      ).toBeDefined();
      expect(screen.getByRole("link")).toBeDefined();
    },
  );
  it.each([
    "disk-too-small",
    "storage-pool-missing",
    "inventory-unreadable",
  ] as const)(
    "adds the agent-work explanation for underlying storage %s",
    (storageReason) => {
      notice({
        area: "agent",
        reason: "storage-unavailable",
        storageReason,
        administrator: false,
        provider: "Acme",
      });
      expect(screen.getByRole("alert").textContent).toBe(
        enUS.chat.filesUnavailable +
          "AI assistants and AI employees cannot work until then." +
          enUS.agentInstances.unavailable.askAdmin,
      );
      expect(screen.queryByRole("link")).toBeNull();
    },
  );
  it.each([
    [
      "host-unqualified",
      "The server is not yet set up to run AI assistants and AI employees safely.",
    ],
    [
      "admission-failed",
      "AI assistants and AI employees cannot start right now: a safety check on the server failed. Their files are kept.",
    ],
  ] as const)("uses agent-oriented copy for %s", (storageReason, text) => {
    notice({
      area: "agent",
      reason: "storage-unavailable",
      storageReason,
      administrator: false,
    });
    expect(screen.getByText(text)).toBeDefined();
    expect(
      screen.getByText(enUS.agentInstances.unavailable.askAdmin),
    ).toBeDefined();
  });
  it("uses the busy-files sentence with no support instructions", () => {
    notice({
      area: "agent",
      reason: null,
      name: "Researcher",
      busy: true,
      administrator: true,
    });
    expect(screen.getByRole("alert").textContent).toBe(
      "Researcher's files are being changed right now and cannot be used. Try again in a few minutes.",
    );
  });
});
