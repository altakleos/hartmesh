import { afterEach, describe, expect, it } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

import {
  I18nProvider,
  useI18nContext,
  useProductName,
} from "@/core/i18n/context";

afterEach(cleanup);

function Consumer() {
  const { locale, setLocale, t } = useI18nContext();
  return (
    <>
      <output>{`${locale}:${t.locale.localName}`}</output>
      <button type="button" onClick={() => setLocale("zh-CN")}>
        switch
      </button>
    </>
  );
}

describe("I18nProvider", () => {
  it("keeps locale and its dictionary in sync", async () => {
    render(
      <I18nProvider initialLocale="en-US">
        <Consumer />
      </I18nProvider>,
    );

    expect(screen.getByText("en-US:English")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "switch" }));

    expect(await screen.findByText("zh-CN:中文")).toBeTruthy();
    expect(document.documentElement.lang).toBe("zh-CN");
  });

  it("selects the initial dictionary inside the client boundary", () => {
    render(
      <I18nProvider initialLocale="zh-CN">
        <Consumer />
      </I18nProvider>,
    );

    expect(screen.getByText("zh-CN:中文")).toBeTruthy();
  });

  it("says the deployment's product name in every dictionary it selects", async () => {
    function Named() {
      const { setLocale, t } = useI18nContext();
      return (
        <>
          <output>{`${useProductName()}|${t.workspace.about}`}</output>
          <button type="button" onClick={() => setLocale("zh-CN")}>
            switch
          </button>
        </>
      );
    }
    render(
      <I18nProvider initialLocale="en-US" productName="Acme Assist">
        <Named />
      </I18nProvider>,
    );

    expect(screen.getByText("Acme Assist|About Acme Assist")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "switch" }));
    expect(
      await screen.findByText("Acme Assist|关于 Acme Assist"),
    ).toBeTruthy();
  });

  it("is HartMesh when the layout names nothing", () => {
    function Named() {
      return <output>{useProductName()}</output>;
    }
    render(
      <I18nProvider initialLocale="en-US">
        <Named />
      </I18nProvider>,
    );
    expect(screen.getByText("HartMesh")).toBeTruthy();
  });
});
