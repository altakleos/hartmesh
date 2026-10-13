import { describe, expect, it } from "@rstest/core";

import {
  readStorageRefusal,
  storageRefusalSentence,
} from "@/core/file-areas/storage-state";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";

describe("storage refusal presentation", () => {
  it("reads HTTP and stream envelopes and SDK HTTP errors", () => {
    const detail = {
      code: "storage-unavailable",
      reason: "space-not-assigned",
      detail_code: "no-seat",
      message: "raw server text",
    };
    const expected = {
      status: 503,
      code: "storage-unavailable",
      reason: "space-not-assigned",
      detailCode: "no-seat",
      message: "raw server text",
    };
    expect(readStorageRefusal({ status: 503, detail })).toEqual(expected);
    expect(readStorageRefusal({ status: 503, ...detail })).toEqual(expected);
    expect(
      readStorageRefusal({
        status: 503,
        message: `HTTP 503: ${JSON.stringify({ detail })}`,
      }),
    ).toEqual(expected);
    expect(readStorageRefusal({ error: { status: 503, detail } })).toEqual(
      expected,
    );
  });

  it("rejects mismatched status/code and unrelated messages", () => {
    for (const error of [
      null,
      "files-full",
      new Error("HTTP 507: broken JSON"),
      { status: 400, code: "files-full" },
      { status: 507, code: "future-code" },
    ])
      expect(readStorageRefusal(error)).toBeNull();
  });

  it("maps personal, shared and upload capacity refusals to active-locale text", () => {
    const refusal = readStorageRefusal({
      status: 507,
      code: "files-full",
      message: "raw",
    })!;
    expect(
      storageRefusalSentence(refusal, enUS, {
        area: "files",
        support: "an administrator",
      }),
    ).toBe(enUS.files.saveFull("an administrator"));
    expect(
      storageRefusalSentence(refusal, enUS, {
        area: "shared",
        support: "Acme",
      }),
    ).toBe(enUS.shared.full("Acme"));
    expect(
      storageRefusalSentence(refusal, zhCN, {
        area: "upload",
        filename: "statement.pdf",
        support: "管理员",
      }),
    ).toBe(zhCN.uploads.filesFull("statement.pdf", "管理员"));
  });

  it("distinguishes preparing, no seat and unchecked personal and shared files", () => {
    const missing = readStorageRefusal({
      status: 503,
      code: "storage-unavailable",
      reason: "space-not-assigned",
    })!;
    expect(
      storageRefusalSentence(missing, enUS, {
        area: "files",
        support: "an administrator",
      }),
    ).toBe(enUS.chat.filesPreparing("an administrator"));
    expect(
      storageRefusalSentence({ ...missing, detailCode: "no-seat" }, enUS, {
        area: "files",
        support: "Acme",
        administrator: true,
      }),
    ).toBe(enUS.chat.filesNoSeat(true, "Acme"));
    expect(
      storageRefusalSentence({ ...missing, reason: "space-unverified" }, enUS, {
        area: "shared",
        support: "Acme",
      }),
    ).toBe(enUS.shared.unchecked);
  });

  it.each([
    "disk-too-small",
    "storage-pool-missing",
    "inventory-unreadable",
    "host-unqualified",
    "admission-failed",
  ])("hides deployment details from a user for %s", (reason) => {
    const refusal = readStorageRefusal({
      status: 503,
      code: "storage-unavailable",
      reason,
      message: "raw internal geometry",
    })!;
    expect(
      storageRefusalSentence(refusal, enUS, {
        area: "files",
        support: "an administrator",
      }),
    ).toBe(enUS.chat.filesUnavailable);
    expect(
      storageRefusalSentence(refusal, enUS, {
        area: "files",
        support: "Acme",
        administrator: true,
      }),
    ).not.toContain("raw internal geometry");
  });

  it("keeps unknown-reason server messages and chooses busy copy per area", () => {
    const unknown = readStorageRefusal({
      status: 503,
      code: "storage-unavailable",
      reason: "future-reason",
      message: "specific server explanation",
    })!;
    expect(
      storageRefusalSentence(unknown, enUS, { area: "files", support: "Acme" }),
    ).toBe("specific server explanation");
    const busy = readStorageRefusal({ status: 409, code: "storage-busy" })!;
    expect(
      storageRefusalSentence(busy, enUS, { area: "files", support: "Acme" }),
    ).toBe(enUS.files.busy);
    expect(
      storageRefusalSentence(busy, enUS, { area: "shared", support: "Acme" }),
    ).toBe(enUS.shared.busy);
  });
});
