import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "@rstest/core";

import { enUS, zhCN } from "@/core/i18n/locales";
import { loadTranslations } from "@/core/i18n/translations";

describe("core copy loading", () => {
  it("loads only the requested overseas and domestic copy, in the deployment's name", async () => {
    const [english, chinese] = await Promise.all([
      loadTranslations("en-US", "Acme Assist"),
      loadTranslations("zh-CN", "Acme Assist"),
    ]);
    expect(english.channels.descriptions.buzz).toBe(
      "Buzz channels and direct messages through your Acme Assist agent.",
    );
    expect(chinese.channels.descriptions.buzz).toBe(
      "通过 Acme Assist 智能体接收 Buzz 频道消息和私聊。",
    );
    expect(english.pages.appName).toBe("Acme Assist");
    expect(english.workspace.about).toBe("About Acme Assist");
  });

  it("names the product HartMesh when the deployment names nothing", () => {
    expect(enUS.pages.appName).toBe("HartMesh");
    expect(zhCN.pages.appName).toBe("HartMesh");
  });

  it("never names the framework the product is built on", () => {
    // The source, so formatters are covered too: every name in the copy is
    // the one the deployment configured.
    for (const file of ["en-US.ts", "zh-CN.ts"]) {
      const source = readFileSync(
        join(import.meta.dirname, "../../../../src/core/i18n/locales", file),
        "utf8",
      );
      expect(source).not.toMatch(/deer ?flow|🦌/i);
    }
  });
});
